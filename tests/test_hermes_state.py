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


def test_read_slice_range_is_inclusive_at_both_ends(hermes_db):
    # Ids run 1..7 and the range asked for is 3..5, so both bounds have
    # something to exclude. A db holding only 3..5 would let a completely
    # unbounded query pass this.
    path = hermes_db(sessions=[SESSION("s", message_count=7)],
                     messages=[MSG(i, "s", "user", "x" * 60) for i in range(1, 8)])
    state_db = hs.connect_ro(path)
    msgs = hs.read_slice_range(state_db, "s", first_id=3, last_id=5)
    assert [m.id for m in msgs] == [3, 4, 5]


def test_read_slice_range_is_empty_when_every_message_went_inactive(hermes_db):
    # `active = 0` is set when the db is BUILT, not by an UPDATE: connect_ro
    # opens the file `mode=ro` and would refuse the write. Same end state —
    # every message in 3..5 gone, the ones outside it still there.
    path = hermes_db(
        sessions=[SESSION("s", message_count=7)],
        messages=[MSG(i, "s", "user", "x" * 60, active=0 if 3 <= i <= 5 else 1)
                  for i in range(1, 8)])
    state_db = hs.connect_ro(path)
    assert hs.read_slice_range(state_db, "s", first_id=3, last_id=5) == []


def test_session_source_is_empty_when_the_session_row_is_gone(hermes_db):
    """The sweeper's `or "cli"` fallback is what covers it — a retry slice has
    no Candidate to read `source` from, only this."""
    path = hermes_db(sessions=[SESSION("s", source="slack", message_count=1)],
                     messages=[MSG(1, "s", "user", "x" * 60)])
    state_db = hs.connect_ro(path)
    assert hs.session_source(state_db, "s") == "slack"
    assert hs.session_source(state_db, "vanished") == ""
