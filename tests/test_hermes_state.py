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
