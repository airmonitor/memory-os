"""
Chat LLM client via LiteLLM (`/chat/completions`, OpenAI-compatible).

Replaces the previous direct-Ollama (`/api/generate`) implementation. Callers
in tasks/reflection.py still invoke `ollama_chat()` — the name is kept for
backward compatibility with import sites.
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

logger = logging.getLogger("cognitive-worker.llm")


async def ollama_chat(prompt: str, model: str | None = None, timeout: int = 120) -> str:
    """Send a prompt to LiteLLM and return the response content.

    `model` arg overrides config.litellm.models.chat.name (used by callers that
    want to address a specific tier, e.g. reflection vs extraction).
    """
    base_url = config.litellm.base_url.rstrip("/")
    api_key = config.litellm.api_key or ""
    chat = config.litellm.models.chat
    model_id = model or chat.name

    url = f"{base_url}/chat/completions"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": float(chat.temperature),
        "max_tokens": int(chat.max_tokens),
        "stream": False,
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()

    try:
        return data["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, TypeError) as e:
        logger.warning(f"Unexpected LiteLLM response shape: {e}; payload={data}")
        return ""
