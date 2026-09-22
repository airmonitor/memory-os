"""Parse a JSON object out of an LLM reply that may wrap it in prose or a Markdown fence.

The full reflection used a strict `json.loads(response)`. gpt-5.6-luna answers with a bare
object, but local Qwen models (lm-studio-qwen3.6, qwen3.8-27b-splash) answer with a leading
blank line and a ```json fence around the same object. Measured on real semitora memories
2026-09-22: both Qwen replies were valid JSON inside the fence, and the strict parse sent
them to the `{"raw": ...}` fallback, which keeps the text but loses the structure the
insight point is built from. The micro-reflection already extracts with a regex; this is
the one parser both can share.
"""
from __future__ import annotations

import json
import re

_FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.S | re.I)


def parse_json_reply(text: str):
    """Return the parsed JSON value, or raise ValueError when no JSON object can be found.

    Order: the whole reply, then the first fenced block, then the span from the first `{`
    to the last `}`. Never guesses beyond that.
    """
    candidates = [text.strip()]
    fenced = _FENCE.search(text)
    if fenced:
        candidates.append(fenced.group(1).strip())
    start, end = text.find("{"), text.rfind("}")
    if 0 <= start < end:
        candidates.append(text[start:end + 1])
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    raise ValueError("no JSON object in reply")
