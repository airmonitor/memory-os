"""Prompt injection sanitization — the strippers, as a leaf module.

Moved out of `icarus/hooks.py` byte-for-byte so the extraction path (and any
other consumer) can reuse the same patterns and functions without importing
the hooks module. This module imports nothing from `icarus.hooks` or
`icarus.state` — it stays a leaf.
"""

import re

# ── Prompt injection sanitization ────────────────────────────

INJECTION_PATTERNS = [
    # "ignore all previous/prior instructions/directives"
    (re.compile(r"(?i)\bignore\s+all\s+(previous|prior)\s+(instructions|directives|commands|messages|prompts|context)"),
     "[REDACTED]"),
    # "you are/will now become/act/acting as (a/an) AI/assistant..."
    (re.compile(r"(?i)\byou\s+(are|will\s+now)\s+(now\s+)?(become|act|acting)\s+as\s+(a\s+|an\s+)?(AI\s+assistant|assistant|AI|agent|LLM|chatbot|model|system)"),
     "[REDACTED]"),
    # "new instructions/directives/commands follow/above/below"
    (re.compile(r"(?i)\bnew\s+(instructions|directives|commands)\s+(follow|above|below)"),
     "[REDACTED]"),
    # Template injection: {{...}}, ${...}
    (re.compile(r"\{\{.*?\}\}|\$\{.*?\}"), "[REDACTED]"),
    # Triple-backtick code fences
    (re.compile(r"```"), "[code]"),
    # Markdown/javascript data: URLs in links and images
    (re.compile(r"(?i)(javascript|data)\s*:"), "sanitized:"),
    # XML/HTML injection: <script>, event handlers, iframes
    (re.compile(r"<\s*script[\s>]|on\w+\s*=|<\s*iframe[\s>]"), "[sanitized]"),
    # Known system prefixes
    (re.compile(r"(?i)\[IMPORTANT:.*?\]|\[SYSTEM:.*?\]|\[OVERRIDE:.*?\]"), "[REDACTED]"),
    # Control characters (keep newlines and tabs)
    (re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]"), ""),
    # Zero-width and invisible Unicode
    (re.compile(r"[\u200b-\u200f\u2028-\u202f\u2060-\u2064\ufeff]"), ""),
]


def validate_safe_content(text: str) -> str:
    """Catch unknown attack patterns via heuristic:
    high density of directive/imperative language in a short span.
    Falls back to [SANITIZED] placeholder if heuristic triggers.
    """
    if not text or len(text) < 20:
        return text
    try:
        # Count directive-style phrases per character
        directivess = len(re.findall(
            r"(?i)\b(ignore|forget|disregard|override|replace|pretend|act\s+as|you\s+(are|must|will|shall))\b",
            text
        ))
        if directivess >= 3 and directivess / max(len(text), 1) > 0.02:
            return "[SANITIZED]"
        return text
    except Exception:
        return text


def sanitize_context_text(text: str, max_len: int = 600) -> str:
    """Sanitize retrieved text before it enters the agent's context.
    Strips known injection patterns, validates safety, truncates.
    Fail-open: returns truncated original on error.
    """
    if not text:
        return ""
    try:
        result = str(text)
        for pattern, replacement in INJECTION_PATTERNS:
            result = pattern.sub(replacement, result)
        # Safety heuristic catch
        result = validate_safe_content(result)
        # Normalize excessive whitespace
        result = re.sub(r"\n{4,}", "\n\n\n", result)
        result = re.sub(r" {8,}", " ", result)
        return result.strip()[:max_len]
    except Exception:
        return str(text)[:max_len]


# The subset whose removal cannot change the meaning of a sentence. This is what
# the extraction path applies, and it is NOT an injection boundary — see
# docs/adr/0002, decision 1. A paraphrased instruction passes it untouched, and
# tests/test_sanitize.py asserts exactly that so nobody mistakes it for a control.
MECHANICAL_PATTERNS = [
    (re.compile(r"\{\{.*?\}\}|\$\{.*?\}"), ""),
    (re.compile(r"```"), ""),
    (re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]"), ""),
    # Escaped ranges, NOT literal invisible characters: a literal class survives
    # exactly until one editor normalises the file, and then it matches nothing.
    (re.compile("[\\u200b-\\u200f\\u2028-\\u202f\\u2060-\\u2064\\ufeff]"), ""),
]


def strip_mechanical(text: str) -> str:
    if not text:
        return ""
    out = str(text)
    for pattern, replacement in MECHANICAL_PATTERNS:
        out = pattern.sub(replacement, out)
    return out
