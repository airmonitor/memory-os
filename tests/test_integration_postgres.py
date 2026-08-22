"""Integration coverage for the SQL a fake cannot check.

Every assertion `tests/test_session_store.py` makes about the claim SQL, the
advisory lock and the JSONB payload trusts a `FakeConn`/`FakeCursor` pair that
agrees with whatever `scripts/session_store.py` actually sends it — it can
prove the code builds the SQL it intends to build, never that PostgreSQL
accepts that SQL or does what the docstrings claim. This file runs the same
module against a real server.

Skipped unless MEMOS_TEST_DSN is set, so the default suite stays offline:
    MEMOS_TEST_DSN=postgresql://user:pass@localhost:5432/memos_test .venv/bin/pytest -m integration

Each test claims a fresh, randomly-suffixed session_id and the `conn` fixture
drops both tables on teardown, so the file can run twice IN SEQUENCE against
the same database without colliding with a previous run. That teardown is
also why two runs must never overlap and this must never point at a database
anything else is using: a concurrent run's DROP TABLE deletes the other run's
rows out from under it, and pointing this at a shared database drops that
database's own session_extraction/sweeper_status bookkeeping.
"""
import os
import uuid
import pytest

psycopg = pytest.importorskip("psycopg")
DSN = os.environ.get("MEMOS_TEST_DSN")
pytestmark = [pytest.mark.integration,
              pytest.mark.skipif(not DSN, reason="set MEMOS_TEST_DSN")]


@pytest.fixture
def conn():
    from scripts import session_store
    c = psycopg.connect(DSN)
    session_store.ensure_schema(c)
    yield c
    # A genuine psycopg.Error mid-test (not just an AssertionError) leaves the
    # connection INERROR, and a transaction in that state rejects everything,
    # including this teardown's own DROP TABLE, with "current transaction is
    # aborted". Left unhandled, that leaks the tables and their rows into the
    # database and breaks the next run's exact-count assertions - the very
    # property the module docstring above promises for sequential re-runs.
    # rollback() is a no-op on an already-clean transaction, so it is safe to
    # call unconditionally here.
    c.rollback()
    with c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS session_extraction, sweeper_status")
    c.commit()
    c.close()


def test_a_second_claim_on_a_fresh_row_loses(conn):
    from scripts import session_store
    sid = f"s-{uuid.uuid4()}"
    assert session_store.claim(conn, session_id=sid, first_message_id=1,
                               last_message_id=9, message_count=4) is True
    assert session_store.claim(conn, session_id=sid, first_message_id=1,
                               last_message_id=9, message_count=4) is False


def test_a_stale_claim_is_reclaimable(conn):
    from scripts import session_store
    sid = f"s-{uuid.uuid4()}"
    session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                        message_count=4)
    with conn.cursor() as cur:
        cur.execute("UPDATE session_extraction SET updated_at = now() - interval '3 hours' "
                    "WHERE session_id = %s", (sid,))
    conn.commit()
    assert session_store.expire_stale_claims(conn, stale_hours=2) == 1
    assert session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                               message_count=4) is True
    # A crash between claim and extraction is transient by nature - OOM, eviction,
    # a rolled pod. Reclaiming must neither increment attempts (three crashes would
    # quarantine a slice that never once failed deterministically) nor reset them
    # (the count would never accumulate across crash cycles). Asserting `== 0`
    # here cannot tell "never touched" from "reset to zero by a bug" - attempts
    # started at 0, so both land on the same value. See the second half below,
    # which starts attempts at 1, for the assertion that can actually fail.
    with conn.cursor() as cur:
        cur.execute("SELECT attempts FROM session_extraction WHERE session_id = %s", (sid,))
        assert cur.fetchone()[0] == 0


def test_a_stale_claim_with_a_prior_failure_keeps_its_attempts_count(conn):
    """The other half of the previous test's docstring. A row that has
    already failed deterministically once (attempts == 1) is reclaimed, then
    crashes before extraction finishes and goes stale - that reclaim, and the
    stale-recovery cycle that follows (expire_stale_claims, then claim()
    again), must leave attempts at 1, not reset it to 0. Starting from a
    non-zero count is what lets this test actually fail if either of those
    ever touched `attempts` - the previous test's `== 0` cannot, because
    attempts started at 0 there too."""
    from scripts import session_store
    sid = f"s-{uuid.uuid4()}"
    session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                        message_count=4)
    assert session_store.mark_failed(conn, session_id=sid, last_message_id=9,
                                     error="bad json", count_attempt=True) == 1
    # Reclaim the now-'failed' row via claim()'s 'failed' reclaim arm, so the
    # row goes back to 'claimed' with attempts == 1 before it goes stale below.
    assert session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                               message_count=4) is True
    with conn.cursor() as cur:
        cur.execute("UPDATE session_extraction SET updated_at = now() - interval '3 hours' "
                    "WHERE session_id = %s", (sid,))
    conn.commit()
    assert session_store.expire_stale_claims(conn, stale_hours=2) == 1
    with conn.cursor() as cur:
        cur.execute("SELECT attempts FROM session_extraction WHERE session_id = %s", (sid,))
        assert cur.fetchone()[0] == 1  # expire_stale_claims must not touch it
    assert session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                               message_count=4) is True
    with conn.cursor() as cur:
        cur.execute("SELECT attempts FROM session_extraction WHERE session_id = %s", (sid,))
        # expire_stale_claims already set status back to 'failed', so this
        # claim() reclaims via the 'failed' arm again (not the stale-'claimed'
        # arm - that one only fires for a row still 'claimed' when claim() is
        # called, which expire_stale_claims running first makes rare; see its
        # own docstring). Either arm must leave attempts alone.
        assert cur.fetchone()[0] == 1


def test_the_payload_round_trips_as_jsonb(conn):
    from scripts import session_store
    sid = f"s-{uuid.uuid4()}"
    payload = [{"job_id": "ingest:x:9:0", "text": "t", "entry_type": "decision",
                "summary": "s", "training_value": "high"}]
    session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                        message_count=4)
    session_store.mark_extracted(conn, session_id=sid, last_message_id=9, entries=1,
                                 score=0.5, payload=payload)
    rows = [r for r in session_store.pending_dispatch(conn) if r["session_id"] == sid]
    assert rows[0]["payload"] == payload


def test_ensure_schema_is_idempotent_against_a_live_server(conn):
    from scripts import session_store
    session_store.ensure_schema(conn)          # second call must not raise
    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'sweeper_status'")
        cols = {r[0] for r in cur.fetchall()}
    assert {"redispatched", "quarantined", "aborted", "locked_out",
            "stale_slices"} <= cols


def test_ensure_schema_upgrades_an_existing_table_missing_the_new_columns(conn):
    """The `conn` fixture already ran `ensure_schema` once, which is the
    on-a-fresh-database path. The upgrade path — an existing deployment whose
    tables predate `attempts`/`quarantined`/`redispatched` — is a different
    statement (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`) and needs its own
    proof: build the OLD-shaped tables by hand, then confirm `ensure_schema`
    adds the missing columns rather than erroring on tables that already
    exist."""
    from scripts import session_store
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS session_extraction, sweeper_status")
        cur.execute("""
            CREATE TABLE session_extraction (
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
        """)
        cur.execute("""
            CREATE TABLE sweeper_status (
                id             BIGSERIAL PRIMARY KEY,
                ran_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
                candidates     INTEGER NOT NULL,
                extracted      INTEGER NOT NULL,
                entries        INTEGER NOT NULL,
                jobs           INTEGER NOT NULL,
                schema_version INTEGER,
                error          TEXT
            )
        """)
    conn.commit()

    session_store.ensure_schema(conn)  # must ALTER the existing tables, not raise

    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'session_extraction'")
        se_cols = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'sweeper_status'")
        ss_cols = {r[0] for r in cur.fetchall()}
    assert "attempts" in se_cols
    # The circuit-breaker columns are on this list for the same reason as the
    # rest: they arrive by ALTER, never by the CREATE above, so a deployment
    # whose sweeper_status predates them upgrades or the run-level breakers
    # keep leaving no durable trace at all.
    assert {"redispatched", "quarantined", "aborted", "locked_out",
            "stale_slices"} <= ss_cols


def test_record_run_stores_the_circuit_breaker_outcome(conn):
    """The scenario the cross-session breaker exists for, as a row.

    A misrouting gateway fails two sessions deterministically in one run;
    `sweep()` rolls both attempts back and therefore ZEROES `quarantined`, and
    it returns normally, so `error` is NULL. Before these columns the row read
    `candidates=2, extracted=0, quarantined=0, error=NULL` — the shape of a
    quiet healthy run — while the sweep burned two LLM calls every 15 minutes.
    `aborted` is the only durable evidence, and this asserts it survives a
    round trip through a real server (the BOOLEAN column, the widened INSERT,
    and the argument list `main()` now passes).
    """
    from scripts import session_store
    session_store.record_run(conn, candidates=2, extracted=0, entries=0, jobs=0,
                             schema_version=26, error=None, redispatched=0,
                             quarantined=0, aborted=True, locked_out=3, stale_slices=1)
    with conn.cursor() as cur:
        cur.execute("SELECT aborted, locked_out, stale_slices, quarantined, error "
                    "FROM sweeper_status ORDER BY id DESC LIMIT 1")
        assert cur.fetchone() == (True, 3, 1, 0, None)


def test_record_run_defaults_the_new_columns_for_a_healthy_run(conn):
    """Callers that predate the new keywords must still write a row — the
    defaults are load-bearing, not tidy, because this call site's whole job is
    to never raise."""
    from scripts import session_store
    session_store.record_run(conn, candidates=1, extracted=1, entries=2, jobs=2,
                             schema_version=26, error=None)
    with conn.cursor() as cur:
        cur.execute("SELECT aborted, locked_out, stale_slices "
                    "FROM sweeper_status ORDER BY id DESC LIMIT 1")
        assert cur.fetchone() == (False, 0, 0)


def test_mark_failed_returns_the_incremented_attempts_count(conn):
    from scripts import session_store
    sid = f"s-{uuid.uuid4()}"
    session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                        message_count=4)
    assert session_store.mark_failed(conn, session_id=sid, last_message_id=9,
                                     error="bad json", count_attempt=True) == 1
    # A transient failure must not move the counter at all.
    assert session_store.mark_failed(conn, session_id=sid, last_message_id=9,
                                     error="gateway timeout", count_attempt=False) == 1
    # The row is 'failed' after mark_failed, so claim()'s reclaim arm re-wins it
    # without touching attempts - only mark_failed(count_attempt=True) does.
    session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                        message_count=4)
    assert session_store.mark_failed(conn, session_id=sid, last_message_id=9,
                                     error="bad json again", count_attempt=True) == 2


def test_rollback_attempt_decrements_without_going_below_zero(conn):
    from scripts import session_store
    sid = f"s-{uuid.uuid4()}"
    session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                        message_count=4)
    session_store.mark_failed(conn, session_id=sid, last_message_id=9,
                              error="bad json", count_attempt=True)

    def attempts():
        with conn.cursor() as cur:
            cur.execute("SELECT attempts FROM session_extraction "
                        "WHERE session_id = %s AND last_message_id = %s", (sid, 9))
            return cur.fetchone()[0]

    assert attempts() == 1
    session_store.rollback_attempt(conn, session_id=sid, last_message_id=9)
    assert attempts() == 0
    # GREATEST(attempts - 1, 0) - a second rollback on an already-zeroed row
    # must not go negative.
    session_store.rollback_attempt(conn, session_id=sid, last_message_id=9)
    assert attempts() == 0


def test_the_session_lock_is_released_by_the_transaction(conn):
    from scripts import session_store
    other = psycopg.connect(DSN)
    try:
        with conn.transaction():
            assert session_store.try_session_lock(conn, "lock-me") is True
            assert session_store.try_session_lock(other, "lock-me") is False
        # transaction over -> the lock is gone without anyone releasing it
        with other.transaction():
            assert session_store.try_session_lock(other, "lock-me") is True
    finally:
        other.close()
