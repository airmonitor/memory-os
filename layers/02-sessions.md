# Layer 2 — Session Database

> **File:** `$HERMES_HOME/state.db` (SQLite + FTS5)
> **Tool:** `session_search`
> **Writer:** Hermes Gateway (automatic)

## What it stores

Every message sent and received by the agent is logged automatically by the gateway process:

```
sessions table:
  session_id, title, source (telegram/cli/cron), started_at, ended_at

messages table:
  id, session_id, role (user/assistant/tool), content, timestamp

messages_fts (FTS5 virtual table):
  Full-text index over message content
```

## How the agent uses it

The `session_search` tool provides three access patterns:

1. **Discovery** — `session_search(query="auth refactor")` → FTS5 search across all sessions, returns top matches with context windows
2. **Scroll** — `session_search(session_id="...", around_message_id=12345)` → read a specific session
3. **Browse** — `session_search()` → recent sessions chronologically

## Context injection (Icarus enhancement)

The Icarus fork adds `_search_sessions()` in `hooks.py` — FTS5 search during `pre_llm_call` that injects relevant past conversations into the system prompt. This means the agent doesn't need to explicitly call `session_search` — relevant history finds it automatically.

Key implementation details:
- FTS5 query: OR of tokens ≥ 4 characters from user message
- Excludes current session
- Deduplicates by session in Python
- Labeled `[sessions]` in prompt for source transparency
- Opens DB read-only: `sqlite3.connect("file:{db}?mode=ro", uri=True)`

## Configuration

No configuration needed — the gateway manages this automatically.

## Pitfalls

- **FTS5 is lexical, not semantic:** Queries in one language may not match content in another with different wording
- **`snippet()` + `GROUP BY` incompatibility:** Cannot combine in same SQL query — fetch ranked rows, dedup in Python
- **`started_at` is Unix timestamp (float):** Not an ISO string — use `datetime.fromtimestamp()` for display
