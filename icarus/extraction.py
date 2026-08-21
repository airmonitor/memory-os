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

# The old allowlist (decision, resolution, note) predates readers that also
# branch on code-session/task/review/research. An entry outside this union
# is not dropped — it is kept with its type rewritten to "note" (see
# _validate_entries) because a mis-typed memory is recoverable and a dropped
# one is not.
ALLOWED_TYPES = {"decision", "resolution", "note", "code-session", "task",
                 "review", "research"}
_MIN_SUMMARY_LEN = 10
_MIN_CONTENT_LEN = 60
_SUMMARY_MAX, _CONTENT_MAX = 80, 2000

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


def _unwrap(parsed):
    """Normalise a parsed JSON value into a list of entry dicts.

    Handles the shapes some models return instead of a bare array:
    {"entries": [...]}, {"results": [...]}, or a single entry object
    ({"type": ..., ...}) — which becomes a one-element list rather than
    being dropped, since a bare-object response is a real extraction, not
    noise.
    """
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("entries", "results"):
            if key in parsed and isinstance(parsed[key], list):
                return parsed[key]
        if "type" in parsed:
            return [parsed]
    return []


def parse_json_robust(raw):
    """Extract entries from LLM output, tolerating markdown fences and dict wrappers."""
    if raw is None:
        return []
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.S)
    start_list, start_obj = text.find("["), text.find("{")
    # Prefer whichever top-level bracket appears first — an object wrapper
    # like {"entries": [...]} has its own "[" nested inside, after the "{".
    if start_list != -1 and (start_obj == -1 or start_list < start_obj):
        start, end = start_list, text.rfind("]")
    elif start_obj != -1:
        start, end = start_obj, text.rfind("}")
    else:
        return []
    if end <= start:
        return []
    try:
        parsed = json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return []
    return _unwrap(parsed)


def _validate_entries(entries):
    """Drop entries too short to be useful; normalise type/training_value; truncate."""
    valid = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        summary = (entry.get("summary") or "").strip()
        content = (entry.get("content") or "").strip()
        if len(summary) < _MIN_SUMMARY_LEN or len(content) < _MIN_CONTENT_LEN:
            continue
        etype = entry.get("type")
        if etype not in ALLOWED_TYPES:
            logger.warning("icarus: unknown extraction type %r — rewritten to 'note'", etype)
            etype = "note"
        valid.append({
            "type": etype,
            "summary": summary[:_SUMMARY_MAX],
            "content": content[:_CONTENT_MAX],
            "training_value": entry.get("training_value") or "normal",
        })
    return valid


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
    return _validate_entries(entries)
