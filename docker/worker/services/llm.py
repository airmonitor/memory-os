"""
LLM client via native Ollama.
"""
import os
import logging

import httpx

logger = logging.getLogger("cognitive-worker.llm")

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "deepseek-v4-flash:cloud")
OLLAMA_API_KEY = os.environ.get("OLLAMA_API_KEY", "")


def get_auth_header() -> dict:
    """Returns auth header if API key is configured."""
    if OLLAMA_API_KEY:
        return {"Authorization": f"Bearer {OLLAMA_API_KEY}"}
    return {}


async def ollama_chat(prompt: str, model: str | None = None, timeout: int = 120) -> str:
    """
    Sends a prompt to native Ollama and returns the response.
    Uses cloud models like deepseek-v4-flash:cloud.
    """
    model = model or OLLAMA_MODEL
    url = f"{OLLAMA_BASE_URL}/api/generate"

    headers = {
        "Content-Type": "application/json",
        **get_auth_header(),
    }

    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.7,
            "num_predict": 4096,  # DeepSeek generates long reasoning; needs space
        },
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()

    # DeepSeek v4 flash: reasoning can consume tokens, leaving response empty
    # Return reasoning if content is empty
    response = data.get("response", "")
    if not response and "reasoning" in data:
        response = data["reasoning"]
    
    return response
