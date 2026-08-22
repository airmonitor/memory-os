"""Fixtures shaped like the Hermes state.db measured on hermes_agent 0.20.4.

Column sets and the role mix come from a real database (19 sessions,
115 messages): assistant 49, tool 34, user 27, session_meta 5; 28 rows with
empty content, 23 of which carry tool_calls; active=0 and compacted=1 unseen.
"""
import os
import sqlite3
import time
from pathlib import Path

import pytest

# --- Issue #18: no test may touch a real machine path -------------------
#
# c2c1cb6 fixed ONE test (test_hooks_retirement.py::test_on_session_end_does_
# not_extract) that called the real state.write_memory_file() against
# HERMES_HOME as inherited from the process environment. That test passed in
# a git worktree (no .env) and failed in the primary checkout, because the
# repo's git-ignored .env points HERMES_HOME at another machine's home
# directory. Nothing stopped the NEXT test from doing the same thing --
# isolation was each test's own responsibility, and that is what let this
# through.
#
# The 8 names below are every path-shaped setting under `paths:` in
# config/services.yaml (hermes_home, state_db, memory_store_db, fabric_dir,
# vault, wiki_root, telemetry_log, reflection_log), matched to their env var
# by grepping the git-ignored .env for those exact keys and cross-checking
# icarus/state.py's own os.environ.get() calls -- HERMES_HOME and FABRIC_DIR
# are the two it actually reads; the rest are consumed by other modules via
# memos_config but are patched here too since "at minimum" in the ticket
# means covering the whole paths: block, not just what one module happens to
# use today.
_PATH_ENV_VARS = (
    "HERMES_HOME", "FABRIC_DIR", "VAULT_PATH", "STATE_DB_PATH",
    "MEMORY_STORE_DB", "TELEMETRY_LOG_PATH", "REFLECTION_LOG_PATH", "WIKI_ROOT",
)

# Two more found the same way (grepping every os.environ.get()/getenv() in
# the tree, not just state.py) that don't fit the 8-name list above:
#   - OBSIDIAN_VAULT_PATH (icarus/obsidian.py) is a DIFFERENT name from
#     VAULT_PATH, gated on ICARUS_OBSIDIAN (icarus/state.py:379) which is
#     unset by default -- so also cleared, in case a leaked shell env has it.
#   - FABRIC_RETRIEVE_PATH (icarus/state.py:564) defaults to "" and is read
#     at CALL time, not import time; setting it to tmp_path removes any
#     chance of it resolving to Path("") -> cwd.
_EXTRA_PATH_ENV_VARS = ("OBSIDIAN_VAULT_PATH", "FABRIC_RETRIEVE_PATH")

# The autouse fixture below patches os.environ for every test, but that is
# the fixture half only: icarus/state.py (and its import-time siblings
# icarus/export-training.py, icarus/fabric-retrieve.py,
# icarus/scripts/eval-replacement.py) read HERMES_HOME/FABRIC_DIR at MODULE
# IMPORT time. A module already imported earlier in this same test process --
# by an earlier test, or during collection -- keeps whatever path it computed
# at that first import, and env-var patching after the fact does nothing for
# it. tests/test_write_entry.py's `fabric` fixture and
# tests/test_hooks_retirement.py's on_session_end test both work around this
# themselves, by reloading the module under the patched env; that per-test
# discipline stays (belt). This is the guard that catches the next test that
# forgets it (braces): it snapshots the real, machine-specific locations
# state.py's fallbacks resolve to when nothing overrides them --
# `(HERMES_HOME or Path.home())` for four files, `Path.home() / "fabric"` for
# FABRIC_DIR -- plus whatever HERMES_HOME actually names in this process,
# captured HERE, at conftest import time, which happens before pytest
# collects/imports any test module and therefore before anything could have
# leaked the git-ignored .env into os.environ. If any of that changed by the
# time a test finishes, the test fails -- whether or not .env is present,
# because collection-time capture works out to None just as validly as a
# real path.
_HOME = Path.home()
_REAL_HERMES_HOME_AT_COLLECTION = os.environ.get("HERMES_HOME")
_WATCHED_HOME_PATHS = [
    _HOME / ".hermes",
    _HOME / "fabric",
    _HOME / ".icarus-training-job.txt",
    _HOME / ".icarus-models.json",
    _HOME / ".icarus-telemetry.jsonl",
    _HOME / ".icarus-state.json",
]
if _REAL_HERMES_HOME_AT_COLLECTION:
    _WATCHED_HOME_PATHS.append(Path(_REAL_HERMES_HOME_AT_COLLECTION))


def _snapshot_real_paths():
    """(path, exists, mtime/size or dir listing) for every watched path.

    Directories are walked shallowly-recursive so a bare mkdir with no file
    written under it still shows up as a change -- rglob("*") alone would
    miss an empty directory.
    """
    snap = {}
    for p in _WATCHED_HOME_PATHS:
        if not p.exists():
            snap[p] = None
        elif p.is_dir():
            entries = []
            for f in p.rglob("*"):
                if f.is_file():
                    st = f.stat()
                    entries.append((str(f.relative_to(p)), "file", st.st_mtime_ns, st.st_size))
                elif f.is_dir():
                    entries.append((str(f.relative_to(p)), "dir"))
            snap[p] = tuple(sorted(entries))
        else:
            st = p.stat()
            snap[p] = (st.st_mtime_ns, st.st_size)
    return snap


@pytest.fixture(autouse=True)
def _isolate_filesystem_paths(tmp_path, monkeypatch):
    before = _snapshot_real_paths()
    for name in _PATH_ENV_VARS + _EXTRA_PATH_ENV_VARS:
        monkeypatch.setenv(name, str(tmp_path / name.lower()))
    monkeypatch.delenv("ICARUS_OBSIDIAN", raising=False)
    yield
    after = _snapshot_real_paths()
    changed = [p for p in _WATCHED_HOME_PATHS if before[p] != after[p]]
    assert not changed, (
        "test wrote to a real machine path outside tmp_path: "
        f"{[str(p) for p in changed]} -- a module almost certainly read "
        "HERMES_HOME/FABRIC_DIR at import time before this fixture's "
        "monkeypatch.setenv could reach it. Reload the module under the "
        "patched env the way tests/test_write_entry.py's `fabric` fixture "
        "and tests/test_hooks_retirement.py's on_session_end test do."
    )


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
