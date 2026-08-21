"""
Embedding client. Talks to LiteLLM (OpenAI-compatible). Reads endpoint, key,
model, and expected dimensions from config/services.yaml.
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import httpx

_here = Path(__file__).resolve()
for _candidate in (_here.parent, *_here.parents):
    if (_candidate / "memos_config" / "loader.py").exists():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

from memos_config import config  # noqa: E402

logger = logging.getLogger("cognitive-worker.embedding")

# Lone (unpaired) UTF-16 surrogates. Valid emoji/astral chars are single
# non-surrogate code points in a Python 3 str, so this NEVER matches them; it
# only matches genuinely broken input (a surrogate pair split mid-character
# upstream, or malformed unicode emitted by an LLM extraction step).
_LONE_SURROGATE_RE = re.compile(r"[\ud800-\udfff]")


def _strip_lone_surrogates(text: str) -> str:
    """
    Remove unpaired UTF-16 surrogates so the text is UTF-8 encodable.

    LiteLLM's redis cache builds the cache key with
    hashlib.sha256(cache_key.encode()) (strict UTF-8). A lone surrogate (e.g.
    \\ud83c) raises UnicodeEncodeError "surrogates not allowed", which 500s the
    embedding request through every fallback. Sanitizing here kills the bad data
    at the source instead of relying on the proxy-side cache-type workaround.
    """
    cleaned, n = _LONE_SURROGATE_RE.subn("", text)
    if n:
        logger.warning("Stripped %d lone surrogate(s) from embedding input", n)
    return cleaned


async def get_embedding(text: str) -> list[float]:
    """
    Generate embedding via LiteLLM. Validates returned dimensions against
    config.litellm.models.embedding.dimensions.
    """
    text = _strip_lone_surrogates(text)
    base_url = config.litellm.base_url.rstrip("/")
    api_key = config.litellm.api_key or ""
    model = config.litellm.models.embedding.name
    expected_dims = int(config.litellm.models.embedding.dimensions)
    # Must exceed the proxy's upstream timeout plus one fallback hop, or we hang
    # up before LiteLLM can fail over — see the comment on this key in
    # config/services.yaml. getattr covers an older config file with no such
    # key; the `or` covers EMBEDDING_TIMEOUT being *set but empty*, which the
    # loader interpolates to "" and float("") would raise on.
    timeout = float(getattr(config.litellm.models.embedding, "timeout", None) or 100)

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": model,
        "input": text,
        "dimensions": expected_dims,  # OpenAI-style; local servers may ignore
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{base_url}/embeddings",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

    vec = data["data"][0]["embedding"]

    if len(vec) != expected_dims:
        raise ValueError(
            f"Embedding dimension mismatch: expected {expected_dims}, got {len(vec)}. "
            f"Check config.litellm.models.embedding.dimensions and Qdrant collection schema."
        )

    logger.debug(f"Embedding generated: {len(vec)} dims")
    return vec
