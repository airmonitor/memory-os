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
