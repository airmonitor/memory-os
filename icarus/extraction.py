"""Transcript building, scoring and LLM extraction — as pure functions.

Moved out of hooks.py so the sweeper and the plugin share ONE implementation.
`score_exchanges` takes its inputs explicitly instead of reading module state,
which is what made the old `score_session()` unusable from a second process —
and, in gateway mode, what let two concurrent Slack threads score as one.
"""
from __future__ import annotations

import json
import re

DECISION_RE = re.compile(r"\b(decided|chose|selected|approach|will use|going with)\b", re.I)
OUTCOME_RE = re.compile(r"\b(result|outcome|works|fixed|passed|failed|measured)\b", re.I)
WEIGHTS = {"depth": 2, "decision": 3, "recall_usage": 2, "linked_entries": 2,
           "user_engagement": 1}
_TOOL_NAME_RE = re.compile(r'"name"\s*:\s*"([^"]+)"')

USER_MAX, ASSISTANT_MAX, TOOL_MAX = 500, 800, 300


def _tool_names(tool_calls: str) -> list[str]:
    return _TOOL_NAME_RE.findall(tool_calls or "")


def _render(message) -> str | None:
    content = (message.content or "").strip()
    if message.role == "user":
        return f"[User]\n{content[:USER_MAX]}" if content else None
    if message.role == "assistant":
        if content:
            return f"[Agent]\n{content[:ASSISTANT_MAX]}"
        names = _tool_names(message.tool_calls)
        return f"[tool: {', '.join(names)}]" if names else None
    if message.role == "tool":
        if not content:
            return None
        label = message.tool_name or "tool"
        return f"[tool result: {label}]\n{content[:TOOL_MAX]}"
    return None


def build_transcript(messages, *, context=()) -> str:
    lines = []
    if context:
        lines.append("=== CONTEXT (earlier in this conversation, not scored) ===")
        lines.extend(x for x in (_render(m) for m in context) if x)
        lines.append("=== CURRENT SLICE ===")
    lines.extend(x for x in (_render(m) for m in messages) if x)
    return "\n\n".join(lines)


def messages_to_exchanges(messages) -> list[dict]:
    """Pair each user message with the assistant text that follows it."""
    exchanges: list[dict] = []
    current = None
    for m in messages:
        if m.role == "user" and (m.content or "").strip():
            current = {"user": m.content.strip(), "assistant": ""}
            exchanges.append(current)
        elif m.role == "assistant" and (m.content or "").strip() and current is not None:
            current["assistant"] = (current["assistant"] + "\n" + m.content.strip()).strip()
    return exchanges


def score_exchanges(exchanges, *, recall_usage: float = 0.0, linked_entries: int = 0) -> dict:
    scores = {}
    substantive = [e for e in exchanges if len((e.get("assistant") or "").strip()) > 100]
    scores["depth"] = min(len(substantive) / 5, 1.0)

    all_text = " ".join((e.get("assistant") or "") for e in exchanges)
    has_decision = bool(DECISION_RE.search(all_text))
    has_outcome = bool(OUTCOME_RE.search(all_text))
    scores["decision"] = 1.0 if (has_decision and has_outcome) else (0.5 if has_decision else 0.0)
    scores["recall_usage"] = float(recall_usage)
    scores["linked_entries"] = min(linked_entries / 2, 1.0)
    substantial_user = sum(1 for e in exchanges if len((e.get("user") or "").strip()) > 50)
    scores["user_engagement"] = min(substantial_user / 3, 1.0)

    scores["total"] = round(
        sum(scores[k] * WEIGHTS[k] for k in WEIGHTS) / sum(WEIGHTS.values()), 2)
    return scores
