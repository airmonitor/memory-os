# Session Sweeper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract finished conversation slices from Hermes' own `state.db` on a schedule, write them to the fabric, and enqueue them for vector ingestion — with no change to Hermes Agent.

**Architecture:** A scheduled script reads Hermes' SQLite database read-only inside one `BEGIN DEFERRED` snapshot, finds sessions that have gone quiet past a threshold, claims each `(session_id, last_message_id)` slice in PostgreSQL, extracts entries with one LLM call, publishes fabric files atomically, and dispatches ARQ ingestion jobs under deterministic job ids. The plugin hook stops extracting; it keeps only its creative-memory write.

**Tech Stack:** Python 3.13 (runtime is the Hermes gateway venv), sqlite3 (stdlib, read-only URI), psycopg 3, arq 0.28, pytest.

**Spec:** `docs/adr/0001-session-extraction-via-state-db-sweeper.md`

## Global Constraints

- **No file in `/opt/hermes` is modified, ever.** Hermes' `state.db` is opened `file:…?mode=ro`; nothing writes to it, nothing migrates it.
- **Fail-open on the turn path, fail-loud on the sweep path.** A hook must never raise into a turn. The sweeper logs errors and records them in `sweeper_status`.
- **Every new tunable is a `${VAR:default}` key in `config/services.yaml`**, matching the existing convention. No new bare `os.environ` reads.
- **Nothing vendored may be imported above `memos_config`** in any `scripts/*.py` module — `memos_config/__init__.py` is what puts `vendor/` on `sys.path` in the deployed pod.
- Python floor: 3.11 (the deployed interpreter is 3.13; `datetime.UTC` and `X | None` are available).
- All timestamps from `state.db` are **UNIX epoch seconds as REAL**, not ISO strings.
- Defaults from the ADR: `idle_minutes=90`, `min_messages=4`, `context_overlap=4`, `max_lag_hours=24`, `max_per_run=3`, `quality_threshold=0.2`.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `icarus/hermes_state.py` (new) | Read-only accessor for Hermes `state.db`: snapshot, schema version, candidate sessions, message slices. Knows SQL; knows nothing about extraction. |
| `icarus/extraction.py` (new) | Pure transcript building, scoring and the LLM extraction call. No global state, no I/O except the one HTTP call. |
| `scripts/session_store.py` (new) | PostgreSQL bookkeeping: schema, watermarks, claim/extracted/published state machine, per-run status row. |
| `scripts/session_sweeper.py` (new) | Orchestration and CLI. The only module that touches all three stores. |
| `icarus/hooks.py` (modify) | `on_session_end` stops extracting; `_search_qdrant` registers lineage; extraction helpers re-exported from `icarus/extraction.py`. |
| `icarus/state.py` (modify) | `write_entry()` gains a deterministic `suffix` and writes atomically. |
| `scripts/context_enhancer.py` (modify) | `register_lineage()` becomes keyword-only. |
| `config/services.yaml` (modify) | `session_extraction` block, `litellm.models.extraction.timeout`. |
| `tests/` (new) | pytest suite; SQLite fixtures shaped like the measured Hermes schema. |

---

### Task 1: Test harness and a Hermes-shaped SQLite fixture

**Files:**
- Create: `tests/conftest.py`
- Create: `tests/test_fixture_shape.py`
- Create: `pytest.ini`

**Interfaces:**
- Consumes: nothing.
- Produces: `hermes_db(tmp_path, sessions=…, messages=…) -> pathlib.Path`, a fixture factory writing a `state.db` with the real column set; `MSG(...)` and `SESSION(...)` helpers returning dicts with the measured defaults.

- [ ] **Step 1: Write `pytest.ini`**

```ini
[pytest]
testpaths = tests
addopts = -q
markers =
    integration: needs a live PostgreSQL (set MEMOS_TEST_DSN)
```

- [ ] **Step 2: Write the fixture factory**

`tests/conftest.py`:

```python
"""Fixtures shaped like the Hermes state.db measured on hermes_agent 0.20.4.

Column sets and the role mix come from a real database (19 sessions,
115 messages): assistant 49, tool 34, user 27, session_meta 5; 28 rows with
empty content, 23 of which carry tool_calls; active=0 and compacted=1 unseen.
"""
import sqlite3
import time
import pytest

SESSION_COLUMNS = (
    "id", "source", "chat_type", "thread_id", "message_count",
    "started_at", "ended_at", "end_reason", "last_activity_at", "expiry_finalized",
)
MESSAGE_COLUMNS = (
    "id", "session_id", "role", "content", "tool_calls", "tool_name",
    "timestamp", "active", "compacted",
)


def SESSION(id, *, source="slack", chat_type="dm", thread_id="t1", message_count=0,
            started_at=None, ended_at=None, end_reason=None, last_activity_at=None,
            expiry_finalized=0):
    now = time.time()
    return dict(id=id, source=source, chat_type=chat_type, thread_id=thread_id,
                message_count=message_count, started_at=started_at or now,
                ended_at=ended_at, end_reason=end_reason,
                last_activity_at=last_activity_at if last_activity_at is not None else now,
                expiry_finalized=expiry_finalized)


def MSG(id, session_id, role, content="", *, tool_calls="", tool_name=None,
        timestamp=None, active=1, compacted=0):
    return dict(id=id, session_id=session_id, role=role, content=content,
                tool_calls=tool_calls, tool_name=tool_name,
                timestamp=timestamp if timestamp is not None else time.time(),
                active=active, compacted=compacted)


@pytest.fixture
def hermes_db(tmp_path):
    def build(sessions=(), messages=(), schema_version=26):
        path = tmp_path / "state.db"
        con = sqlite3.connect(path)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE schema_version (version INTEGER)")
        con.execute("INSERT INTO schema_version VALUES (?)", (schema_version,))
        con.execute(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, chat_type TEXT,"
            " thread_id TEXT, message_count INTEGER, started_at REAL, ended_at REAL,"
            " end_reason TEXT, last_activity_at REAL, expiry_finalized INTEGER)")
        con.execute(
            "CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,"
            " content TEXT, tool_calls TEXT, tool_name TEXT, timestamp REAL,"
            " active INTEGER, compacted INTEGER)")
        for s in sessions:
            con.execute(
                f"INSERT INTO sessions ({','.join(SESSION_COLUMNS)}) "
                f"VALUES ({','.join('?' * len(SESSION_COLUMNS))})",
                tuple(s[c] for c in SESSION_COLUMNS))
        for m in messages:
            con.execute(
                f"INSERT INTO messages ({','.join(MESSAGE_COLUMNS)}) "
                f"VALUES ({','.join('?' * len(MESSAGE_COLUMNS))})",
                tuple(m[c] for c in MESSAGE_COLUMNS))
        con.commit()
        con.close()
        return path
    return build
```

- [ ] **Step 3: Write the test that proves the fixture matches the measured shape**

`tests/test_fixture_shape.py`:

```python
import sqlite3
from tests.conftest import SESSION, MSG


def test_fixture_has_the_columns_the_reader_depends_on(hermes_db):
    path = hermes_db(sessions=[SESSION("s1")], messages=[MSG(1, "s1", "user", "hi")])
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    cols = {r[1] for r in con.execute("PRAGMA table_info(messages)")}
    assert {"id", "session_id", "role", "content", "tool_calls", "active", "compacted"} <= cols
    assert con.execute("SELECT version FROM schema_version").fetchone()[0] == 26
```

- [ ] **Step 4: Run the test**

Run: `.venv/bin/pytest tests/test_fixture_shape.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pytest.ini tests/
git commit -m "test: pytest harness and a Hermes-shaped state.db fixture"
```

---

### Task 2: `icarus/hermes_state.py` — the read-only accessor

**Files:**
- Create: `icarus/hermes_state.py`
- Test: `tests/test_hermes_state.py`

**Interfaces:**
- Consumes: Task 1's `hermes_db`, `SESSION`, `MSG`.
- Produces:
  - `connect_ro(path: str | Path) -> sqlite3.Connection`
  - `snapshot(conn) -> ContextManager[None]` (BEGIN DEFERRED / COMMIT)
  - `schema_version(conn) -> int | None`
  - `Candidate` dataclass: `session_id, source, ended_at, last_activity_at, message_count`
  - `find_candidates(conn, *, now, idle_seconds, max_lag_seconds, watermarks, min_messages, limit) -> list[Candidate]`
  - `Message` dataclass: `id, role, content, tool_calls, tool_name, timestamp, compacted`
  - `read_slice(conn, session_id, *, after_id) -> list[Message]`
  - `context_tail(conn, session_id, *, before_id, limit) -> list[Message]`

- [ ] **Step 1: Write the failing tests**

`tests/test_hermes_state.py`:

```python
import time
import pytest
from icarus import hermes_state as hs
from tests.conftest import SESSION, MSG

HOUR = 3600


def test_idle_session_is_a_candidate_but_active_one_is_not(hermes_db):
    now = time.time()
    path = hermes_db(
        sessions=[SESSION("quiet", last_activity_at=now - 2 * HOUR, message_count=6),
                  SESSION("busy", last_activity_at=now - 60, message_count=6)],
        messages=[MSG(i, "quiet", "user", "x" * 60) for i in range(1, 7)]
                 + [MSG(i, "busy", "user", "x" * 60) for i in range(10, 16)])
    con = hs.connect_ro(path)
    with hs.snapshot(con):
        found = hs.find_candidates(con, now=now, idle_seconds=90 * 60,
                                   max_lag_seconds=24 * HOUR, watermarks={},
                                   min_messages=4, limit=10)
    assert [c.session_id for c in found] == ["quiet"]


def test_ended_session_is_a_candidate_immediately(hermes_db):
    now = time.time()
    path = hermes_db(
        sessions=[SESSION("cli", source="cli", last_activity_at=now - 30,
                          ended_at=now - 30, end_reason="cli_close", message_count=5)],
        messages=[MSG(i, "cli", "user", "x" * 60) for i in range(1, 6)])
    con = hs.connect_ro(path)
    with hs.snapshot(con):
        found = hs.find_candidates(con, now=now, idle_seconds=90 * 60,
                                   max_lag_seconds=24 * HOUR, watermarks={},
                                   min_messages=4, limit=10)
    assert [c.session_id for c in found] == ["cli"]


def test_watermark_excludes_already_extracted_messages(hermes_db):
    now = time.time()
    path = hermes_db(
        sessions=[SESSION("s", last_activity_at=now - 2 * HOUR, message_count=6)],
        messages=[MSG(i, "s", "user", "x" * 60) for i in range(1, 7)])
    con = hs.connect_ro(path)
    with hs.snapshot(con):
        assert hs.find_candidates(con, now=now, idle_seconds=90 * 60,
                                  max_lag_seconds=24 * HOUR, watermarks={"s": 6},
                                  min_messages=1, limit=10) == []
        rows = hs.read_slice(con, "s", after_id=3)
    assert [m.id for m in rows] == [4, 5, 6]


def test_short_tail_waits_for_min_messages_until_the_lag_ceiling(hermes_db):
    now = time.time()
    path = hermes_db(
        sessions=[SESSION("young", last_activity_at=now - 2 * HOUR, message_count=2),
                  SESSION("old", last_activity_at=now - 30 * HOUR, message_count=2)],
        messages=[MSG(1, "young", "user", "x" * 60), MSG(2, "young", "assistant", "y" * 200),
                  MSG(3, "old", "user", "x" * 60), MSG(4, "old", "assistant", "y" * 200)])
    con = hs.connect_ro(path)
    with hs.snapshot(con):
        found = hs.find_candidates(con, now=now, idle_seconds=90 * 60,
                                   max_lag_seconds=24 * HOUR, watermarks={},
                                   min_messages=4, limit=10)
    assert [c.session_id for c in found] == ["old"]


def test_read_slice_keeps_tool_rows_and_drops_session_meta(hermes_db):
    path = hermes_db(
        sessions=[SESSION("s", message_count=4)],
        messages=[MSG(1, "s", "user", "q"),
                  MSG(2, "s", "assistant", "", tool_calls='[{"name": "read_file"}]'),
                  MSG(3, "s", "tool", "file contents", tool_name="read_file"),
                  MSG(4, "s", "session_meta", "ignored"),
                  MSG(5, "s", "assistant", "answer", active=0)])
    con = hs.connect_ro(path)
    with hs.snapshot(con):
        rows = hs.read_slice(con, "s", after_id=0)
    assert [(m.id, m.role) for m in rows] == [(1, "user"), (2, "assistant"), (3, "tool")]


def test_schema_version_is_reported(hermes_db):
    con = hs.connect_ro(hermes_db(schema_version=26))
    assert hs.schema_version(con) == 26


def test_connection_is_read_only(hermes_db):
    con = hs.connect_ro(hermes_db(sessions=[SESSION("s")]))
    with pytest.raises(Exception):
        con.execute("DELETE FROM sessions")
```

- [ ] **Step 2: Run them and watch them fail**

Run: `.venv/bin/pytest tests/test_hermes_state.py -v`
Expected: `ModuleNotFoundError: No module named 'icarus.hermes_state'`.

- [ ] **Step 3: Implement the module**

`icarus/hermes_state.py` — the important parts:

```python
"""Read-only accessor for the Hermes agent's own state.db.

WHY THIS EXISTS. On hermes_agent 0.20.4 the plugin hook `on_session_end` fires
once per user message, so the plugin never sees a finished conversation. Hermes
does: it stores every message in this database. Reading it is the only way to
extract sessions without changing Hermes. See docs/adr/0001-*.md.

RULES, and they are not stylistic:
  * The connection is opened `mode=ro`. This process never writes here.
  * Candidate selection and the message read MUST happen inside one snapshot
    (`snapshot()`), or a message appended between the two queries lands in a
    slice whose watermark then hides it forever.
  * The slice's upper bound is whatever `read_slice` returned inside that
    snapshot — never `max(id)` read later.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

TRANSCRIPT_ROLES = ("user", "assistant", "tool")


@dataclass(frozen=True)
class Candidate:
    session_id: str
    source: str
    ended_at: float | None
    last_activity_at: float
    message_count: int


@dataclass(frozen=True)
class Message:
    id: int
    role: str
    content: str
    tool_calls: str
    tool_name: str | None
    timestamp: float | None
    compacted: int


def connect_ro(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True, timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 5000")
    return con


@contextmanager
def snapshot(con: sqlite3.Connection):
    """One read transaction. In WAL mode this pins a consistent view."""
    con.execute("BEGIN DEFERRED")
    try:
        yield
    finally:
        con.execute("COMMIT")


def schema_version(con) -> int | None:
    try:
        row = con.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
        return int(row[0]) if row else None
    except sqlite3.Error:
        return None


def find_candidates(con, *, now, idle_seconds, max_lag_seconds, watermarks,
                    min_messages, limit) -> list[Candidate]:
    rows = con.execute(
        """
        SELECT id, source, ended_at, last_activity_at, message_count
        FROM sessions
        WHERE COALESCE(message_count, 0) > 0
          AND (ended_at IS NOT NULL
               OR COALESCE(last_activity_at, started_at) <= ?)
        ORDER BY COALESCE(last_activity_at, started_at) ASC
        """,
        (now - idle_seconds,),
    ).fetchall()

    out: list[Candidate] = []
    for r in rows:
        after = watermarks.get(r["id"], 0)
        pending = con.execute(
            f"""SELECT COUNT(*) FROM messages
                WHERE session_id = ? AND id > ? AND COALESCE(active, 1) <> 0
                  AND role IN ({','.join('?' * len(TRANSCRIPT_ROLES))})
                  AND (COALESCE(content, '') <> '' OR COALESCE(tool_calls, '') <> '')""",
            (r["id"], after, *TRANSCRIPT_ROLES),
        ).fetchone()[0]
        if pending == 0:
            continue
        # A short tail is not extracted until it stops being able to grow: the
        # lag ceiling is what stops it sitting unprocessed forever.
        aged_out = (now - (r["last_activity_at"] or 0)) >= max_lag_seconds
        if pending < min_messages and not aged_out:
            continue
        out.append(Candidate(r["id"], r["source"] or "", r["ended_at"],
                             r["last_activity_at"] or 0.0, int(r["message_count"] or 0)))
        if len(out) >= limit:
            break
    return out


def _rows_to_messages(rows) -> list[Message]:
    return [Message(int(r["id"]), r["role"], r["content"] or "", r["tool_calls"] or "",
                    r["tool_name"], r["timestamp"], int(r["compacted"] or 0)) for r in rows]


def read_slice(con, session_id: str, *, after_id: int) -> list[Message]:
    rows = con.execute(
        f"""SELECT id, role, content, tool_calls, tool_name, timestamp, compacted
            FROM messages
            WHERE session_id = ? AND id > ? AND COALESCE(active, 1) <> 0
              AND role IN ({','.join('?' * len(TRANSCRIPT_ROLES))})
            ORDER BY id ASC""",
        (session_id, after_id, *TRANSCRIPT_ROLES),
    ).fetchall()
    return _rows_to_messages(rows)


def context_tail(con, session_id: str, *, before_id: int, limit: int) -> list[Message]:
    if limit <= 0:
        return []
    rows = con.execute(
        f"""SELECT id, role, content, tool_calls, tool_name, timestamp, compacted
            FROM messages
            WHERE session_id = ? AND id <= ? AND COALESCE(active, 1) <> 0
              AND role IN ({','.join('?' * len(TRANSCRIPT_ROLES))})
            ORDER BY id DESC LIMIT ?""",
        (session_id, before_id, *TRANSCRIPT_ROLES, limit),
    ).fetchall()
    return list(reversed(_rows_to_messages(rows)))
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_hermes_state.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add icarus/hermes_state.py tests/test_hermes_state.py
git commit -m "feat(icarus): read-only snapshot accessor for Hermes state.db"
```

---

### Task 3: `icarus/extraction.py` — transcript and score, as pure functions

**Files:**
- Create: `icarus/extraction.py`
- Test: `tests/test_extraction.py`

**Interfaces:**
- Consumes: `icarus.hermes_state.Message`.
- Produces:
  - `build_transcript(messages, *, context=()) -> str`
  - `score_exchanges(exchanges, *, recall_usage=0.0, linked_entries=0) -> dict`
  - `messages_to_exchanges(messages) -> list[dict]`

- [ ] **Step 1: Write the failing tests**

`tests/test_extraction.py`:

```python
from icarus import extraction
from icarus.hermes_state import Message


def M(id, role, content="", tool_calls="", tool_name=None):
    return Message(id, role, content, tool_calls, tool_name, None, 0)


def test_tool_call_rows_become_markers_not_holes():
    t = extraction.build_transcript([
        M(1, "user", "read the config"),
        M(2, "assistant", "", tool_calls='[{"function": {"name": "read_file"}}]'),
        M(3, "tool", "line one\nline two", tool_name="read_file"),
        M(4, "assistant", "it says two lines"),
    ])
    assert "read the config" in t
    assert "[tool: read_file]" in t
    assert "line one" in t
    assert "it says two lines" in t


def test_context_messages_are_marked_and_precede_the_slice():
    t = extraction.build_transcript([M(9, "user", "new question")],
                                    context=[M(8, "assistant", "earlier answer")])
    assert t.index("earlier answer") < t.index("new question")
    assert "CONTEXT" in t


def test_exchanges_pair_user_with_the_following_assistant_text():
    ex = extraction.messages_to_exchanges([
        M(1, "user", "q1"), M(2, "assistant", "a1"),
        M(3, "tool", "ignored by pairing"), M(4, "assistant", "a1 continued"),
        M(5, "user", "q2"), M(6, "assistant", "a2"),
    ])
    assert [e["user"] for e in ex] == ["q1", "q2"]
    assert ex[0]["assistant"] == "a1\na1 continued"


def test_score_rises_with_substance_and_stays_low_for_chatter():
    chatter = [{"user": "hi", "assistant": "hello"}]
    assert extraction.score_exchanges(chatter)["total"] < 0.2

    real = [{"user": "u" * 60, "assistant": "we decided to use X. Result: it works. " + "d" * 200}
            for _ in range(5)]
    assert extraction.score_exchanges(real, recall_usage=0.5, linked_entries=2)["total"] >= 0.2


def test_scoring_never_divides_by_zero_on_an_empty_slice():
    assert extraction.score_exchanges([])["total"] == 0.0
```

- [ ] **Step 2: Run them and watch them fail**

Run: `.venv/bin/pytest tests/test_extraction.py -v`
Expected: `ModuleNotFoundError: No module named 'icarus.extraction'`.

- [ ] **Step 3: Implement**

`icarus/extraction.py`:

```python
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
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_extraction.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add icarus/extraction.py tests/test_extraction.py
git commit -m "feat(icarus): pure transcript building and scoring"
```

---

### Task 4: The LLM extraction call, with a configurable timeout

**Files:**
- Modify: `icarus/extraction.py`
- Modify: `icarus/hooks.py` (delete `_parse_json_robust`, `_build_transcript`, `_llm_extract_entries`; import from `extraction`)
- Modify: `config/services.yaml`
- Test: `tests/test_extraction_llm.py`

**Interfaces:**
- Produces: `parse_json_robust(raw) -> list`, and
  `extract_entries(transcript, *, base_url, api_key, model, max_tokens, timeout, opener=urllib.request.urlopen) -> list[dict]`
  where each dict has `type`, `summary`, `content`, `training_value`.

- [ ] **Step 1: Write the failing tests**

`tests/test_extraction_llm.py`:

```python
import io
import json
import pytest
from icarus import extraction


def fake_opener(payload, *, capture=None):
    def _open(req, timeout=None):
        if capture is not None:
            capture["timeout"] = timeout
            capture["url"] = req.full_url
            capture["body"] = json.loads(req.data.decode())
        return io.BytesIO(json.dumps(
            {"choices": [{"message": {"content": payload}}]}).encode())
    return _open


def test_entries_are_parsed_from_a_fenced_json_array():
    raw = '```json\n[{"type": "decision", "summary": "s", "content": "c", ' \
          '"training_value": "high"}]\n```'
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k",
                                     model="m", max_tokens=100, timeout=5,
                                     opener=fake_opener(raw))
    assert out == [{"type": "decision", "summary": "s", "content": "c",
                    "training_value": "high"}]


def test_the_configured_timeout_reaches_the_http_call():
    capture = {}
    extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                               max_tokens=100, timeout=42,
                               opener=fake_opener("[]", capture=capture))
    assert capture["timeout"] == 42
    assert capture["url"] == "http://x/v1/chat/completions"


def test_malformed_output_yields_no_entries_and_does_not_raise():
    assert extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                      max_tokens=10, timeout=5,
                                      opener=fake_opener("not json at all")) == []


def test_entries_missing_required_fields_are_dropped():
    raw = json.dumps([{"type": "decision"}, {"type": "note", "summary": "s", "content": "c"}])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert [e["summary"] for e in out] == ["s"]


def test_no_api_key_means_no_call():
    def explode(*a, **k):
        raise AssertionError("must not call the gateway without a key")
    assert extraction.extract_entries("t", base_url="http://x/v1", api_key="",
                                      model="m", max_tokens=10, timeout=5,
                                      opener=explode) == []
```

- [ ] **Step 2: Run and watch fail**

Run: `.venv/bin/pytest tests/test_extraction_llm.py -v`
Expected: `AttributeError: module 'icarus.extraction' has no attribute 'extract_entries'`.

- [ ] **Step 3: Implement, moving the prompt verbatim from `hooks.py`**

Add to `icarus/extraction.py` (prompt text copied unchanged from `hooks.py:_llm_extract_entries`, so behaviour does not drift):

```python
import logging
import urllib.request

logger = logging.getLogger(__name__)
REQUIRED_FIELDS = ("type", "summary", "content")


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
```

Add the config key in `config/services.yaml` under `litellm.models.extraction`:

```yaml
      # Client budget for one extraction call. Same rule as the embedding
      # timeout above: it must exceed the proxy's own upstream timeout plus one
      # fallback hop, or we hang up before LiteLLM can fail over.
      timeout: ${EXTRACTION_TIMEOUT:100}
```

In `icarus/hooks.py`, delete the three moved helpers and import them:

```python
from .extraction import build_transcript, extract_entries, parse_json_robust  # noqa: F401
```

- [ ] **Step 4: Run the whole suite**

Run: `.venv/bin/pytest -v`
Expected: all PASS, no import errors from `hooks.py`.

- [ ] **Step 5: Commit**

```bash
git add icarus/extraction.py icarus/hooks.py config/services.yaml tests/test_extraction_llm.py
git commit -m "feat(icarus): injectable extraction call with a configurable timeout"
```

---

### Task 5: `scripts/session_store.py` — the claim/publish state machine

**Files:**
- Create: `scripts/session_store.py`
- Test: `tests/test_session_store.py`

**Interfaces:**
- Produces:
  - `ensure_schema(conn) -> None`
  - `watermarks(conn) -> dict[str, int]`
  - `claim(conn, *, session_id, first_message_id, last_message_id, message_count) -> bool`
  - `mark_extracted(conn, *, session_id, last_message_id, entries, score) -> None`
  - `mark_published(conn, *, session_id, last_message_id, jobs) -> None`
  - `pending_dispatch(conn) -> list[dict]`
  - `record_run(conn, *, candidates, extracted, entries, jobs, schema_version, error) -> None`

- [ ] **Step 1: Write the failing tests (fake connection — no PostgreSQL needed)**

`tests/test_session_store.py`:

```python
import pytest
from scripts import session_store


class FakeCursor:
    def __init__(self, log, results):
        self.log, self.results, self._row = log, results, None

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))
        self._row = self.results.pop(0) if self.results else None
        return self

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._row or []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, results=None):
        self.log, self.results, self.commits = [], list(results or []), 0

    def cursor(self):
        return FakeCursor(self.log, self.results)

    def commit(self):
        self.commits += 1


def test_claim_returns_true_when_the_row_is_new():
    conn = FakeConn(results=[(1,)])
    assert session_store.claim(conn, session_id="s", first_message_id=1,
                               last_message_id=9, message_count=5) is True
    sql, params = conn.log[0]
    assert "ON CONFLICT" in sql and "DO NOTHING" in sql
    assert params == ("s", 1, 9, 5)
    assert conn.commits == 1


def test_claim_returns_false_when_another_sweeper_owns_the_slice():
    conn = FakeConn(results=[None])
    assert session_store.claim(conn, session_id="s", first_message_id=1,
                               last_message_id=9, message_count=5) is False


def test_watermarks_uses_the_max_published_or_claimed_id_per_session():
    conn = FakeConn(results=[[("s1", 12), ("s2", 4)]])
    assert session_store.watermarks(conn) == {"s1": 12, "s2": 4}
    sql, _ = conn.log[0]
    assert "MAX(last_message_id)" in sql and "GROUP BY session_id" in sql


def test_mark_published_records_the_job_ids():
    conn = FakeConn(results=[None])
    session_store.mark_published(conn, session_id="s", last_message_id=9,
                                 jobs=["ingest:s:9:0"])
    sql, params = conn.log[0]
    assert "status = 'published'" in sql
    assert params[0] == '["ingest:s:9:0"]' or params[0] == ["ingest:s:9:0"]


def test_a_stale_claim_can_be_reclaimed_but_a_fresh_one_cannot():
    conn = FakeConn(results=[(1,)])
    session_store.claim(conn, session_id="s", first_message_id=1, last_message_id=9,
                        message_count=5, stale_hours=2)
    sql, params = conn.log[0]
    assert "DO UPDATE" in sql
    assert "status = 'failed'" in sql and "make_interval" in sql
    assert params[-1] == 2


def test_pending_dispatch_returns_the_payload_for_re_dispatch():
    conn = FakeConn(results=[[("s", 9, [{"job_id": "ingest:s:9:0", "text": "c"}])]])
    assert session_store.pending_dispatch(conn) == [
        {"session_id": "s", "last_message_id": 9,
         "payload": [{"job_id": "ingest:s:9:0", "text": "c"}]}]


def test_ensure_schema_is_idempotent_sql():
    conn = FakeConn()
    session_store.ensure_schema(conn)
    joined = " ".join(sql for sql, _ in conn.log)
    assert joined.count("CREATE TABLE IF NOT EXISTS") == 2
    assert "UNIQUE (session_id, last_message_id)" in joined
```

- [ ] **Step 2: Run and watch fail**

Run: `.venv/bin/pytest tests/test_session_store.py -v`
Expected: `ModuleNotFoundError: No module named 'scripts.session_store'`.

- [ ] **Step 3: Implement**

`scripts/session_store.py`:

```python
"""PostgreSQL bookkeeping for session extraction.

The three side effects of a sweep — a fabric file, a watermark, an ARQ job —
cannot share one transaction, so the ordering here is the correctness argument:

    claim (unique row)  ->  extract (LLM)  ->  publish (files)  ->  dispatch (ARQ)

A crash anywhere leaves the slice re-runnable and non-duplicating: the claim
stops a second extraction, the deterministic filename makes republication an
overwrite, and arq's `_job_id` makes re-dispatch a no-op.

`import memos_config` MUST come before anything from vendor/ — see
memos_config/__init__.py.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from memos_config import config  # noqa: E402,F401

STALE_CLAIM_HOURS = 2

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS session_extraction (
        id               BIGSERIAL PRIMARY KEY,
        session_id       TEXT      NOT NULL,
        first_message_id BIGINT    NOT NULL,
        last_message_id  BIGINT    NOT NULL,
        message_count    INTEGER   NOT NULL,
        status           TEXT      NOT NULL DEFAULT 'claimed',
        score            REAL,
        entries          INTEGER   NOT NULL DEFAULT 0,
        payload          JSONB,
        jobs             JSONB,
        error            TEXT,
        claimed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (session_id, last_message_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sweeper_status (
        id             BIGSERIAL PRIMARY KEY,
        ran_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        candidates     INTEGER NOT NULL,
        extracted      INTEGER NOT NULL,
        entries        INTEGER NOT NULL,
        jobs           INTEGER NOT NULL,
        schema_version INTEGER,
        error          TEXT
    )
    """,
)


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        for stmt in SCHEMA:
            cur.execute(stmt)
    conn.commit()


def watermarks(conn) -> dict[str, int]:
    """How far each session has been consumed. 'failed' rows do not count, so a
    failed slice is offered again on the next sweep."""
    with conn.cursor() as cur:
        cur.execute("""SELECT session_id, MAX(last_message_id) FROM session_extraction
                       WHERE status <> 'failed' GROUP BY session_id""")
        return {row[0]: int(row[1]) for row in cur.fetchall()}


def claim(conn, *, session_id, first_message_id, last_message_id, message_count,
          stale_hours=STALE_CLAIM_HOURS) -> bool:
    """Win the right to extract this slice. False means somebody else owns it.

    THE `DO UPDATE` BRANCH IS NOT DECORATION. A crash between the claim and the
    extraction leaves a row stuck at 'claimed', and because `watermarks()` counts
    it, the slice would never be offered again — one lost conversation per crash,
    silently, forever. A claim older than `stale_hours`, or one already marked
    'failed', is therefore re-claimable; a fresh claim is not.
    """
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO session_extraction
                   (session_id, first_message_id, last_message_id, message_count)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (session_id, last_message_id) DO UPDATE
                   SET status = 'claimed', claimed_at = now(), updated_at = now()
                   WHERE session_extraction.status = 'failed'
                      OR (session_extraction.status = 'claimed'
                          AND session_extraction.updated_at
                              < now() - make_interval(hours => %s))
               RETURNING id""",
            (session_id, first_message_id, last_message_id, message_count, stale_hours))
        won = cur.fetchone() is not None
    conn.commit()
    return won


def mark_extracted(conn, *, session_id, last_message_id, entries, score, payload=()) -> None:
    """Record the extraction AND what still has to be dispatched.

    The payload is what makes re-dispatch possible after a Valkey outage: the
    next sweep reads it back instead of paying for the LLM call a second time.
    """
    _update(conn, session_id, last_message_id,
            "status = 'extracted', entries = %s, score = %s, payload = %s",
            (entries, score, json.dumps(list(payload))))


def mark_published(conn, *, session_id, last_message_id, jobs) -> None:
    _update(conn, session_id, last_message_id,
            "status = 'published', jobs = %s", (json.dumps(list(jobs)),))


def mark_failed(conn, *, session_id, last_message_id, error) -> None:
    _update(conn, session_id, last_message_id, "status = 'failed', error = %s",
            (str(error)[:2000],))


def _update(conn, session_id, last_message_id, assignment, params) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE session_extraction SET {assignment}, updated_at = now()
                WHERE session_id = %s AND last_message_id = %s""",
            (*params, session_id, last_message_id))
    conn.commit()


def pending_dispatch(conn, limit=50) -> list[dict]:
    """Slices extracted but never dispatched — a crash or a broker outage."""
    with conn.cursor() as cur:
        cur.execute("""SELECT session_id, last_message_id, payload FROM session_extraction
                       WHERE status = 'extracted' ORDER BY id ASC LIMIT %s""", (limit,))
        return [{"session_id": r[0], "last_message_id": int(r[1]), "payload": r[2] or []}
                for r in cur.fetchall()]


def record_run(conn, *, candidates, extracted, entries, jobs, schema_version, error) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sweeper_status
                   (candidates, extracted, entries, jobs, schema_version, error)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (candidates, extracted, entries, jobs, schema_version,
             str(error)[:2000] if error else None))
    conn.commit()
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_session_store.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/session_store.py tests/test_session_store.py
git commit -m "feat(scripts): claim/publish state machine for session extraction"
```

---

### Task 6: Deterministic, atomic `write_entry`

**Files:**
- Modify: `icarus/state.py:306-372`
- Test: `tests/test_write_entry.py`

**Interfaces:**
- Produces: `state.write_entry(..., suffix: str | None = None) -> str` — same behaviour as before when `suffix` is None; a given `suffix` makes the filename deterministic. The write is `tmp + os.replace`.

- [ ] **Step 1: Write the failing tests**

`tests/test_write_entry.py`:

```python
import os
import pytest


@pytest.fixture
def fabric(tmp_path, monkeypatch):
    """icarus.state reads FABRIC_DIR at import time, so the module has to be
    reloaded under the patched environment — and reloaded BACK afterwards, or
    every later test in the process inherits this tmp_path and
    `state.exchanges` points at a different module object than the one under
    test."""
    import importlib
    from icarus import state
    monkeypatch.setenv("FABRIC_DIR", str(tmp_path / "fabric"))
    importlib.reload(state)
    yield state
    monkeypatch.undo()
    importlib.reload(state)


def test_same_suffix_overwrites_instead_of_multiplying(fabric):
    a = fabric.write_entry("decision", "body", "a summary", suffix="deadbeef")
    b = fabric.write_entry("decision", "body v2", "a summary", suffix="deadbeef")
    assert a == b
    assert len(list((fabric.FABRIC_DIR).glob("*.md"))) == 1
    assert "body v2" in open(a).read()


def test_no_suffix_still_produces_unique_names(fabric):
    a = fabric.write_entry("note", "b", "s")
    b = fabric.write_entry("note", "b", "s")
    assert a != b


def test_no_partial_file_is_left_behind(fabric):
    fabric.write_entry("note", "b", "s", suffix="cafe")
    assert not list(fabric.FABRIC_DIR.glob("*.tmp"))
```

- [ ] **Step 2: Run and watch fail**

Run: `.venv/bin/pytest tests/test_write_entry.py -v`
Expected: `TypeError: write_entry() got an unexpected keyword argument 'suffix'`.

- [ ] **Step 3: Implement — two edits in `icarus/state.py`**

Signature gains the parameter, and the random suffix becomes the default:

```python
def write_entry(entry_type, content, summary, tier="hot", tags="", platform="cli",
                status="", outcome="", review_of="", revises="", customer_id="",
                assigned_to="", training_value="", verified="", evidence="",
                source_tool="", artifact_paths="", suffix=None):
    ...
    # A caller that can retry (the sweeper) passes a suffix derived from the
    # slice, so republishing the same slice OVERWRITES its entry instead of
    # producing a second copy. Random stays the default for interactive callers.
    suffix = suffix or secrets.token_hex(2)
```

The write becomes atomic:

```python
    path = FABRIC_DIR / filename
    tmp = path.with_suffix(".md.tmp")
    tmp.write_text("\n".join(lines), "utf-8")
    os.replace(tmp, path)          # atomic within one filesystem
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_write_entry.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add icarus/state.py tests/test_write_entry.py
git commit -m "feat(icarus): deterministic, atomic fabric writes"
```

---

### Task 7: `scripts/session_sweeper.py` — orchestration

**Files:**
- Create: `scripts/session_sweeper.py`
- Test: `tests/test_session_sweeper.py`

**Interfaces:**
- Produces:
  - `Deps` dataclass: `sqlite_conn, pg_conn, extract, write_entry, enqueue, now`
  - `sweep(deps, cfg) -> dict` returning `{"candidates": int, "extracted": int, "entries": int, "jobs": int, "redispatched": int}`
  - `redispatch(deps) -> int` — drains `pending_dispatch()` before any new work
  - `job_id(session_id, last_message_id, index) -> str` → `f"ingest:{session_id}:{last_message_id}:{index}"`
  - `entry_suffix(session_id, last_message_id, index) -> str` (first 8 hex of a sha256)
  - CLI: `--dry-run`, `--session <id>`, `--verbose`

- [ ] **Step 1: Write the failing tests**

`tests/test_session_sweeper.py`:

```python
import time
import pytest
from icarus import hermes_state as hs
from scripts import session_sweeper as sw
from tests.conftest import SESSION, MSG

HOUR = 3600
CFG = dict(idle_seconds=90 * 60, min_messages=2, context_overlap=2,
           max_lag_seconds=24 * HOUR, max_per_run=3, quality_threshold=0.2)


class FakePg:
    """Stands in for scripts.session_store. Watermarks are DERIVED from claims,
    the way the real table derives them, so a second sweep in a test sees what a
    second sweep in production would see."""

    def __init__(self, claimed=()):
        self.claimed = {k: "published" for k in claimed}
        self.calls, self.marks, self.payloads = [], [], {}

    def ensure_schema(self): self.calls.append("ensure_schema")

    def watermarks(self):
        out = {}
        for (sid, last), status in self.claimed.items():
            if status == "failed":
                continue
            out[sid] = max(out.get(sid, 0), last)
        return out

    def claim(self, **kw):
        key = (kw["session_id"], kw["last_message_id"])
        if key in self.claimed and self.claimed[key] != "failed":
            return False
        self.claimed[key] = "claimed"
        return True

    def mark_extracted(self, **kw):
        self.claimed[(kw["session_id"], kw["last_message_id"])] = "extracted"
        self.payloads[(kw["session_id"], kw["last_message_id"])] = list(kw.get("payload", []))
        self.marks.append(("extracted", kw))

    def mark_published(self, **kw):
        self.claimed[(kw["session_id"], kw["last_message_id"])] = "published"
        self.marks.append(("published", kw))

    def mark_failed(self, **kw):
        self.claimed[(kw["session_id"], kw["last_message_id"])] = "failed"
        self.marks.append(("failed", kw))

    def pending_dispatch(self):
        return [{"session_id": sid, "last_message_id": last,
                 "payload": self.payloads.get((sid, last), [])}
                for (sid, last), status in self.claimed.items() if status == "extracted"]

    def record_run(self, **kw): self.marks.append(("run", kw))


def rich_session(now):
    msgs = []
    for i in range(1, 7):
        msgs.append(MSG(2 * i - 1, "s", "user", "u" * 60))
        msgs.append(MSG(2 * i, "s", "assistant",
                        "we decided to use X. Result: measured, it works. " + "d" * 200))
    return [SESSION("s", last_activity_at=now - 2 * HOUR, message_count=len(msgs))], msgs


def make_deps(path, pg, *, entries=None, enqueue=None):
    written, jobs = [], []

    def write_entry(**kw):
        written.append(kw)
        return f"/fabric/{kw['suffix']}.md"

    def _enqueue(job, *args, job_id):
        jobs.append(job_id)
        return job_id
    return sw.Deps(sqlite_conn=hs.connect_ro(path), pg=pg,
                   extract=lambda transcript: entries if entries is not None else
                   [{"type": "decision", "summary": "s", "content": "c",
                     "training_value": "high"}],
                   write_entry=write_entry, enqueue=enqueue or _enqueue,
                   now=time.time), written, jobs


def test_a_quiet_substantive_session_is_extracted_published_and_dispatched(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), FakePg())
    result = sw.sweep(deps, CFG)
    assert result["extracted"] == 1 and result["entries"] == 1 and result["jobs"] == 1
    assert written[0]["suffix"] == sw.entry_suffix("s", 12, 0)
    assert jobs == [sw.job_id("s", 12, 0)]


def test_a_lost_claim_skips_the_llm_call_entirely(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg(claimed={("s", 12)})
    calls = []
    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    deps.extract = lambda t: calls.append(t) or []
    result = sw.sweep(deps, CFG)
    assert calls == []
    assert result["extracted"] == 0


def test_low_scoring_chatter_is_consumed_so_it_is_never_offered_twice(hermes_db):
    now = time.time()
    path = hermes_db(sessions=[SESSION("s", last_activity_at=now - 2 * HOUR, message_count=2)],
                     messages=[MSG(1, "s", "user", "hi"), MSG(2, "s", "assistant", "hello")])
    pg = FakePg()
    deps, written, jobs = make_deps(path, pg)
    result = sw.sweep(deps, CFG)
    assert result["entries"] == 0 and written == [] and jobs == []
    # The watermark must advance, or every sweep forever re-reads the same chatter
    # and pays a claim for it.
    assert pg.watermarks() == {"s": 2}
    deps2, _, _ = make_deps(path, pg)
    assert sw.sweep(deps2, CFG)["candidates"] == 0


def test_a_slice_that_failed_to_dispatch_goes_out_on_the_next_sweep(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()

    def boom(job, *args, job_id):
        raise RuntimeError("valkey down")

    deps, _, _ = make_deps(path, pg, enqueue=boom)
    sw.sweep(deps, CFG)
    assert pg.pending_dispatch(), "the slice must remain dispatchable"

    sent = []

    def ok(job, *args, job_id):
        sent.append(job_id)
        return job_id

    deps2, _, _ = make_deps(path, pg, enqueue=ok)
    result = sw.sweep(deps2, CFG)
    # Same job id as the first attempt: arq dedups, so a double delivery is a
    # no-op rather than a second copy in Qdrant.
    assert sent == [sw.job_id("s", 12, 0)]
    assert result["redispatched"] == 1
    # And no second LLM call was paid for.
    assert result["extracted"] == 0


def test_dispatch_failure_leaves_the_slice_re_runnable(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)

    def boom(job, *args, job_id):
        raise RuntimeError("valkey down")
    pg = FakePg()
    deps, written, _ = make_deps(hermes_db(sessions=sessions, messages=messages), pg,
                                 enqueue=boom)
    result = sw.sweep(deps, CFG)
    assert result["jobs"] == 0
    statuses = [m[0] for m in pg.marks]
    assert "extracted" in statuses and "published" not in statuses


def test_max_per_run_bounds_the_number_of_llm_calls(hermes_db):
    now = time.time()
    sessions, messages = [], []
    for n in range(5):
        sid = f"s{n}"
        sessions.append(SESSION(sid, last_activity_at=now - 2 * HOUR, message_count=4))
        base = 100 * n
        for i in range(2):
            messages.append(MSG(base + 2 * i + 1, sid, "user", "u" * 60))
            messages.append(MSG(base + 2 * i + 2, sid, "assistant",
                                "decided. Result: works. " + "d" * 200))
    deps, _, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), FakePg())
    result = sw.sweep(deps, dict(CFG, max_per_run=2))
    assert result["extracted"] == 2
```

- [ ] **Step 2: Run and watch fail**

Run: `.venv/bin/pytest tests/test_session_sweeper.py -v`
Expected: `ModuleNotFoundError: No module named 'scripts.session_sweeper'`.

- [ ] **Step 3: Implement**

`scripts/session_sweeper.py` — the core; the CLI wrapper builds `Deps` from config:

```python
"""Extract finished conversation slices from Hermes' state.db.

Runs on a schedule (hermes cron --no-agent --script). See
docs/adr/0001-session-extraction-via-state-db-sweeper.md; the ordering
claim -> extract -> publish -> dispatch is the correctness argument.

`import memos_config` MUST precede anything vendored.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from memos_config import config              # noqa: E402
from icarus import extraction, hermes_state  # noqa: E402
from scripts import session_store            # noqa: E402

logger = logging.getLogger("session_sweeper")


@dataclass
class Deps:
    sqlite_conn: object
    pg: object
    extract: Callable[[str], list]
    write_entry: Callable[..., str]
    enqueue: Callable[..., str]
    now: Callable[[], float]


def job_id(session_id: str, last_message_id: int, index: int) -> str:
    return f"ingest:{session_id}:{last_message_id}:{index}"


def entry_suffix(session_id: str, last_message_id: int, index: int) -> str:
    raw = f"{session_id}:{last_message_id}:{index}".encode()
    return hashlib.sha256(raw).hexdigest()[:8]


def redispatch(deps: Deps) -> int:
    """Send jobs for slices that were extracted but never dispatched.

    This is the other half of the ordering guarantee. Publishing before
    dispatching means a broker outage cannot lose an entry — but only if
    something comes back for it. The payload was stored with the claim, so this
    costs no LLM call.
    """
    sent = 0
    for row in deps.pg.pending_dispatch():
        job_ids = []
        try:
            for item in row["payload"]:
                deps.enqueue("process_ingestion", item["text"], "session",
                             job_id=item["job_id"])
                job_ids.append(item["job_id"])
        except Exception as exc:
            logger.warning("re-dispatch of %s:%s failed: %s",
                           row["session_id"], row["last_message_id"], exc)
            continue
        deps.pg.mark_published(session_id=row["session_id"],
                               last_message_id=row["last_message_id"], jobs=job_ids)
        sent += 1
    return sent


def sweep(deps: Deps, cfg: dict) -> dict:
    now = deps.now()
    deps.pg.ensure_schema()
    stats = {"candidates": 0, "extracted": 0, "entries": 0, "jobs": 0,
             "redispatched": redispatch(deps)}
    marks = deps.pg.watermarks()

    with hermes_state.snapshot(deps.sqlite_conn):
        candidates = hermes_state.find_candidates(
            deps.sqlite_conn, now=now, idle_seconds=cfg["idle_seconds"],
            max_lag_seconds=cfg["max_lag_seconds"], watermarks=marks,
            min_messages=cfg["min_messages"], limit=cfg["max_per_run"])
        stats["candidates"] = len(candidates)
        slices = []
        for cand in candidates:
            after = marks.get(cand.session_id, 0)
            messages = hermes_state.read_slice(deps.sqlite_conn, cand.session_id,
                                               after_id=after)
            if not messages:
                continue
            context = hermes_state.context_tail(deps.sqlite_conn, cand.session_id,
                                                before_id=after,
                                                limit=cfg["context_overlap"]) if after else []
            slices.append((cand, context, messages))

    for cand, context, messages in slices:
        first_id, last_id = messages[0].id, messages[-1].id
        if not deps.pg.claim(session_id=cand.session_id, first_message_id=first_id,
                             last_message_id=last_id, message_count=len(messages)):
            logger.info("slice %s:%s already claimed", cand.session_id, last_id)
            continue
        try:
            exchanges = extraction.messages_to_exchanges(messages)
            score = extraction.score_exchanges(exchanges)
            if score["total"] < cfg["quality_threshold"]:
                deps.pg.mark_extracted(session_id=cand.session_id, last_message_id=last_id,
                                       entries=0, score=score["total"])
                deps.pg.mark_published(session_id=cand.session_id, last_message_id=last_id,
                                       jobs=[])
                continue
            entries = deps.extract(extraction.build_transcript(messages, context=context))
            stats["extracted"] += 1
            payload = [{"job_id": job_id(cand.session_id, last_id, i), "text": e["content"]}
                       for i, e in enumerate(entries)]
            deps.pg.mark_extracted(session_id=cand.session_id, last_message_id=last_id,
                                   entries=len(entries), score=score["total"],
                                   payload=payload)
            job_ids = []
            for i, entry in enumerate(entries):
                deps.write_entry(entry_type=entry["type"], content=entry["content"],
                                 summary=entry["summary"], platform=cand.source or "cli",
                                 training_value=entry.get("training_value", "normal"),
                                 status="completed",
                                 suffix=entry_suffix(cand.session_id, last_id, i))
                stats["entries"] += 1
                jid = job_id(cand.session_id, last_id, i)
                deps.enqueue("process_ingestion", entry["content"], "session", job_id=jid)
                job_ids.append(jid)
                stats["jobs"] += 1
            deps.pg.mark_published(session_id=cand.session_id, last_message_id=last_id,
                                   jobs=job_ids)
        except Exception as exc:                    # one bad slice must not stop the sweep
            logger.warning("slice %s:%s failed: %s", cand.session_id, last_id, exc)
            # Without this the row stays 'claimed', watermarks() counts it, and the
            # slice is never offered again — one conversation lost per failure.
            deps.pg.mark_failed(session_id=cand.session_id, last_message_id=last_id,
                                error=exc)
    return stats
```

The CLI section builds real dependencies: `hermes_state.connect_ro(config.paths.state_db)`,
a psycopg connection through `scripts.db.get_conn()` wrapped in a small adapter exposing the
`session_store` functions, `extraction.extract_entries` bound to the configured model,
`icarus.state.write_entry`, and an `enqueue` that opens an arq pool and calls
`enqueue_job(name, *args, _job_id=job_id)`. `--dry-run` swaps `extract`, `write_entry` and
`enqueue` for logging stubs and skips `claim`.

- [ ] **Step 4: Run the suite**

Run: `.venv/bin/pytest -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/session_sweeper.py tests/test_session_sweeper.py
git commit -m "feat(scripts): session sweeper — claim, extract, publish, dispatch"
```

---

### Task 8: Retire hook extraction, wire lineage

**Files:**
- Modify: `icarus/hooks.py` (`on_session_end`, `_search_qdrant`)
- Modify: `scripts/context_enhancer.py:69-75` (`register_lineage` keyword-only)
- Test: `tests/test_hooks_retirement.py`

**Interfaces:**
- Produces: `register_lineage(*, session_id, query, retrieved_chunk_ids, generation_context_hash, generation_model="unknown")` — keyword-only.

- [ ] **Step 1: Write the failing tests**

`tests/test_hooks_retirement.py`:

```python
import inspect
import pytest


def test_register_lineage_is_keyword_only():
    from scripts.context_enhancer import register_lineage
    kinds = {p.kind for p in inspect.signature(register_lineage).parameters.values()}
    assert kinds == {inspect.Parameter.KEYWORD_ONLY}


def test_on_session_end_does_not_extract(monkeypatch):
    from icarus import hooks, state
    state.exchanges.clear()          # module-level list: leaks between tests
    called = []
    monkeypatch.setattr(hooks, "extract_entries", lambda *a, **k: called.append(1) or [])
    monkeypatch.setattr(state, "write_entry", lambda *a, **k: called.append(1))
    state.exchanges.extend([{"user": "u" * 60, "assistant": "decided. Result: works. " + "d" * 300}] * 6)
    hooks.on_session_end(session_id="s", platform="slack")
    assert called == []


def test_search_qdrant_registers_lineage(monkeypatch):
    from icarus import hooks
    seen = {}
    monkeypatch.setitem(__import__("sys").modules, "scripts.context_enhancer",
                        _fake_enhancer(seen))
    hooks._search_qdrant("what did we decide about X", top_k=2)
    # state.session_id is set by on_session_start; in a bare test process it is
    # "" and the hook substitutes "unknown". Either is acceptable, empty is not.
    assert seen["session_id"]
    assert seen["retrieved_chunk_ids"] == ["c1"]


def _fake_enhancer(seen):
    import types
    mod = types.ModuleType("scripts.context_enhancer")
    mod.embed_query = lambda q: [0.0]
    mod.embed_query_sparse = lambda q: ([0], [0.0])
    mod.search_with_fallback = lambda **kw: ([{"id": "c1", "score": 0.9}], "dense", 1.0, 0.0)

    def register_lineage(**kwargs):
        seen.update(kwargs)
        return "lineage-1"
    mod.register_lineage = register_lineage
    return mod
```

- [ ] **Step 2: Run and watch fail**

Run: `.venv/bin/pytest tests/test_hooks_retirement.py -v`
Expected: FAIL — `register_lineage` still accepts positionals; `on_session_end` still extracts.

- [ ] **Step 3: Implement**

`scripts/context_enhancer.py`:

```python
def register_lineage(
    *,
    session_id: str,
    query: str,
    retrieved_chunk_ids: List[str],
    generation_context_hash: str,
    generation_model: str = "unknown",
) -> Optional[str]:
    # KEYWORD-ONLY ON PURPOSE. The old positional signature put
    # generation_context_hash BEFORE generation_model, so a positional caller
    # stored the hash in the model column and the model in the hash column —
    # no error, and the row looked plausible. Rows with a 16-hex
    # generation_model in any deployment are that bug.
```

`icarus/hooks.py`, `on_session_end`:

```python
def on_session_end(session_id="", platform="", completed=False, **kwargs):
    """Persist the creative memory file. EXTRACTION NO LONGER HAPPENS HERE.

    On hermes_agent 0.20.4 this hook fires once per user message
    (agent/turn_finalizer.py:812), and `state.exchanges` is module-level, so in
    gateway mode it also blends concurrent Slack threads into one list. Session
    extraction moved to scripts/session_sweeper.py, which reads the
    authoritative transcript out of Hermes' own state.db.
    See docs/adr/0001-session-extraction-via-state-db-sweeper.md.
    """
    creative = state.load_creative()
    state.write_memory_file(creative)
```

`icarus/hooks.py` gains `import hashlib` at module level (it has none today), and inside
`_search_qdrant`, after `search_with_fallback` returns:

```python
        from scripts.context_enhancer import register_lineage
        try:
            register_lineage(
                session_id=state.session_id or "unknown",
                query=query,
                retrieved_chunk_ids=[str(r.get("id")) for r in results],
                generation_context_hash=hashlib.sha256(
                    "".join(str(r.get("id")) for r in results).encode()).hexdigest()[:16],
                generation_model=_EXTRACTION_MODEL or "unknown",
            )
        except Exception as exc:      # lineage is telemetry; recall must not depend on it
            logger.debug("icarus: lineage write skipped: %s", exc)
```

- [ ] **Step 4: Run the whole suite**

Run: `.venv/bin/pytest -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add icarus/hooks.py scripts/context_enhancer.py tests/test_hooks_retirement.py
git commit -m "feat(icarus): retire hook-side extraction, record retrieval lineage"
```

---

### Task 9: Configuration, docs, and the operator surface

**Files:**
- Modify: `config/services.yaml`
- Modify: `scripts/README.md`
- Create: `docs/session-sweeper.md`
- Test: `tests/test_config_keys.py`

- [ ] **Step 1: Write the failing test**

`tests/test_config_keys.py`:

```python
import os
import yaml


def test_every_sweeper_knob_is_a_config_key_with_a_default():
    raw = open("config/services.yaml").read()
    doc = yaml.safe_load(raw)
    block = doc["session_extraction"]
    for key in ("idle_minutes", "min_messages", "context_overlap", "max_lag_hours",
                "max_per_run", "quality_threshold"):
        assert key in block, key
        assert str(block[key]).startswith("${"), f"{key} must be ${{VAR:default}}"
    assert "${SESSION_IDLE_MINUTES:90}" in raw
    assert "${EXTRACTION_TIMEOUT:100}" in raw
```

- [ ] **Step 2: Run and watch fail**

Run: `.venv/bin/pytest tests/test_config_keys.py -v`
Expected: `KeyError: 'session_extraction'`.

- [ ] **Step 3: Add the block to `config/services.yaml`**

```yaml
# Session extraction (scripts/session_sweeper.py). Thresholds are policy, and
# the numbers come from measurement: gaps inside one real Slack thread ran
# 0-2 min within a burst and 42 min BETWEEN bursts of the same conversation,
# while different conversations sat 292 min and 1326 min apart. 90 minutes is
# above the largest in-conversation gap observed and far below the smallest
# between-conversation one. Raising it merges more into one entry; lowering it
# below 42 is known to split a real conversation.
session_extraction:
  idle_minutes: ${SESSION_IDLE_MINUTES:90}
  min_messages: ${SESSION_MIN_MESSAGES:4}
  context_overlap: ${SESSION_CONTEXT_OVERLAP:4}
  max_lag_hours: ${SESSION_MAX_LAG_HOURS:24}
  max_per_run: ${SESSION_MAX_PER_RUN:3}
  quality_threshold: ${SESSION_QUALITY_THRESHOLD:0.2}
```

- [ ] **Step 4: Write `docs/session-sweeper.md`**

Cover: what it does, the cron line
(`*/15 * * * * $VENV/bin/python $REPO/scripts/session_sweeper.py`), the two tables and how to
read them (`SELECT * FROM sweeper_status ORDER BY ran_at DESC LIMIT 5`), how to re-extract a
session (delete its `session_extraction` rows), how to suppress one (set `status='published'`),
and the three acceptance counters this is meant to move.

- [ ] **Step 5: Run the suite and commit**

```bash
.venv/bin/pytest -v
git add config/services.yaml docs/session-sweeper.md scripts/README.md tests/test_config_keys.py
git commit -m "feat(config): session_extraction thresholds and operator docs"
```

---

## Self-Review

**Spec coverage:** ADR §1 slice semantics → Tasks 2, 7, 9. §2 snapshot protocol → Task 2.
§3 claim/publish/dispatch → Tasks 5, 6, 7. §4 transcript fidelity → Tasks 2, 3. §5
compatibility and status rows → Tasks 2 (`schema_version`), 5 (`sweeper_status`), 9 (docs).
§6 lineage → Task 8. Hook retirement → Task 8.

**Not covered here, deliberately:** registering the cron job on the semitora host lives in
`semitora-agent-prerequisites/roles/memory_os` (a different repository), and re-vendoring this
tree there. Both are follow-up tickets, not steps in this plan.

**Type consistency check:** `Message`/`Candidate` field names used in Tasks 3, 7 match the
dataclasses defined in Task 2. `write_entry(..., suffix=)` in Task 7 matches the signature
added in Task 6. `job_id()` and `entry_suffix()` are defined once, in Task 7, and used only
there. `session_store` function names in Task 7's `FakePg` match Task 5's module surface.
