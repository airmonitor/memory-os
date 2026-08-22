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
