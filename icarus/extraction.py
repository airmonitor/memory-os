"""Transcript building, scoring and LLM extraction — as pure functions.

Moved out of hooks.py so the sweeper and the plugin share ONE implementation.
`score_exchanges` takes its inputs explicitly instead of reading module state,
which is what made the old `score_session()` unusable from a second process —
and, in gateway mode, what let two concurrent Slack threads score as one.
"""
from __future__ import annotations

import json
import logging
import re
import urllib.request

logger = logging.getLogger(__name__)

DECISION_RE = re.compile(
    r"(?i)\b(decided|resolved|completed|fixed|deployed|shipped|reviewed|approved|rejected)\b"
)
OUTCOME_RE = re.compile(
    r"(?i)(result:|outcome:|conclusion:|because|root cause|instead of|\d+%|\d+x)"
)
WEIGHTS = {"depth": 2, "decision": 3, "recall_usage": 2, "linked_entries": 2,
           "user_engagement": 1}
_TOOL_NAME_RE = re.compile(r'"name"\s*:\s*"([^"]+)"')

USER_MAX, ASSISTANT_MAX, TOOL_MAX = 500, 800, 300

REQUIRED_FIELDS = ("type", "summary", "content")

# Verbatim copy of the prompt from hooks.py:_llm_extract_entries — the move to
# this module must not change extraction behaviour.
EXTRACTION_PROMPT = (
    "You are a session archivist for an AI agent. Analyze this agent session "
    "transcript and extract ONLY significant entries worth preserving in a "
    "cross-agent knowledge base. Skip trivial sessions, greetings, and routine chatter.\n\n"
    "For each significant entry, provide:\n"
    "- type: \"decision\" (technical decision with rationale), "
    "\"resolution\" (bug fix or problem solved), "
    "or \"note\" (discovery or learning)\n"
    "- summary: one line, max 80 chars, in the original language of the session\n"
    "- content: structured markdown with ## Context, ## Action/Decision, and ## Outcome. "
    "Include concrete details: commands, paths, error messages, decisions made.\n"
    "- training_value: \"high\" (outcome verified, artifact produced, decision with evidence), "
    "\"normal\" (useful context or progress), "
    "or \"low\" (marginal, but not zero)\n\n"
    "If the session contains NOTHING worth preserving across sessions, "
    "return an empty array: []\n\n"
    "Return ONLY valid JSON array, no other text:\n"
    '[{"type": "decision", "summary": "...", "content": "...", "training_value": "high"}, ...]'
)


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
    """Pair each user message with the assistant text that follows it.

    Agent-initiated turns (assistant message before any user) start an anonymous
    exchange with empty user content.
    """
    exchanges: list[dict] = []
    current = None
    for m in messages:
        if m.role == "user" and (m.content or "").strip():
            current = {"user": m.content.strip(), "assistant": ""}
            exchanges.append(current)
        elif m.role == "assistant" and (m.content or "").strip():
            if current is None:
                # Agent-initiated turn: start an anonymous exchange
                current = {"user": "", "assistant": m.content.strip()}
                exchanges.append(current)
            else:
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


def parse_json_robust(raw):
    """Extract a JSON array from LLM output, tolerating markdown fences."""
    if raw is None:
        return []
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.S)
    start, end = text.find("["), text.rfind("]")
    if start == -1 or end <= start:
        return []
    try:
        parsed = json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def extract_entries(transcript, *, base_url, api_key, model, max_tokens, timeout,
                     opener=urllib.request.urlopen):
    if not api_key:
        logger.warning("icarus: no LiteLLM key — skipping LLM extraction")
        return []
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "system", "content": EXTRACTION_PROMPT},
                     {"role": "user", "content": transcript[:8000]}],
        "max_tokens": max_tokens,
        "temperature": 0.2,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions", data=payload,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {api_key}"})
    try:
        body = json.loads(opener(req, timeout=timeout).read().decode("utf-8"))
        entries = parse_json_robust(body["choices"][0]["message"]["content"])
    except Exception as exc:                      # fail-open: memory never breaks a turn
        logger.warning("icarus: extraction call failed: %s", exc)
        return []
    return [e for e in entries
            if isinstance(e, dict) and all(e.get(f) for f in REQUIRED_FIELDS)]
