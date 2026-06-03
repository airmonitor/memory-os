"""
Embedding client. Talks to LiteLLM (OpenAI-compatible). Reads endpoint, key,
model, and expected dimensions from config/services.yaml.
"""
from __future__ import annotations

import logging
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


async def get_embedding(text: str) -> list[float]:
    """
    Generate embedding via LiteLLM. Validates returned dimensions against
    config.litellm.models.embedding.dimensions.
    """
    base_url = config.litellm.base_url.rstrip("/")
    api_key = config.litellm.api_key or ""
    model = config.litellm.models.embedding.name
    expected_dims = int(config.litellm.models.embedding.dimensions)

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": model,
        "input": text,
        "dimensions": expected_dims,  # OpenAI-style; local servers may ignore
    }

    async with httpx.AsyncClient(timeout=60) as client:
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
