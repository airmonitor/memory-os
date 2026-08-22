"""Integration coverage for the SQL a fake cannot check.

Every assertion `tests/test_session_store.py` makes about the claim SQL, the
advisory lock and the JSONB payload trusts a `FakeConn`/`FakeCursor` pair that
agrees with whatever `scripts/session_store.py` actually sends it — it can
prove the code builds the SQL it intends to build, never that PostgreSQL
accepts that SQL or does what the docstrings claim. This file runs the same
module against a real server.

Skipped unless MEMOS_TEST_DSN is set, so the default suite stays offline:
    MEMOS_TEST_DSN=postgresql://user:pass@localhost:5432/memos_test .venv/bin/pytest -m integration

Each test claims a fresh, randomly-suffixed session_id, and the `conn` fixture
gives every TEST its own throwaway PostgreSQL schema (`CREATE SCHEMA
test_<random>`, `search_path` pointed at it before `ensure_schema` ever runs),
dropped with `CASCADE` on teardown. Nothing this file creates can collide
with, or be dropped alongside, anything a real deployment or a colleague's run
put in `public` — `session_store`'s statements never qualify a schema, so
whatever `search_path` names at connect time is where its tables land, and a
schema unique to this connection is what keeps that from ever being `public`.
That is what makes the file safe to point at a live database in the first
place, not just at a disposable container.

Sequential re-runs against the same database are MEASURED safe (two runs in a
row, same container, both green — see VERIFY in the tickets 16/17 handover).
Concurrent runs are reasoned safe for the tables (disjoint schemas cannot
collide) but NOT measured, and one thing schema isolation does not cover:
`pg_try_advisory_xact_lock` keys are cluster-wide, not schema-scoped, so two
overlapping runs choosing the same lock-test session id could still contend
with each other. Every lock-test session id in this file is randomised for
exactly that reason — treat a fixed string there as a latent collision, not a
readability nicety.
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
    schema = f"test_{uuid.uuid4().hex[:12]}"
    with c.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")
    # CREATE SCHEMA and SET are both transaction-scoped in PostgreSQL: either
    # would be silently undone by a rollback of the transaction that issued
    # them. Doing them here and calling ensure_schema() next, with no commit
    # in between, means all three land in the ONE transaction ensure_schema's
    # own conn.commit() seals — after that, nothing a test does (including the
    # INERROR rollback below) can undo the schema or the search_path, only
    # rows inside the schema.
    session_store.ensure_schema(c)
    yield c
    # A genuine psycopg.Error mid-test (not just an AssertionError) leaves the
    # connection INERROR, and a transaction in that state rejects everything,
    # including this teardown's own DROP SCHEMA, with "current transaction is
    # aborted". rollback() is a no-op on an already-clean transaction, so it
    # is safe to call unconditionally here.
    c.rollback()
    with c.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    c.commit()
    c.close()


def test_a_second_claim_on_a_fresh_row_loses(conn):
    from scripts import session_store
    sid = f"s-{uuid.uuid4()}"
    assert session_store.claim(conn, session_id=sid, first_message_id=1,
                               last_message_id=9, message_count=4) is True
    assert session_store.claim(conn, session_id=sid, first_message_id=1,
                               last_message_id=9, message_count=4) is False


def _make_due(conn, session_id, last_message_id):
    """Bring a failed row's backoff clock forward so that it is due now.

    `mark_failed` and `_EXPIRE_STALE_SQL` both schedule the next retry at least
    15 minutes out, and since ADR-0003 decision 3 BOTH readers honour it —
    `failed_slices()` and the 'failed' arm of `claim()`. A test that wants to
    observe the reclaim rather than the backoff has to say so explicitly.
    """
    with conn.cursor() as cur:
        cur.execute("UPDATE session_extraction SET next_retry_at = now() - interval '1 minute' "
                    "WHERE session_id = %s AND last_message_id = %s",
                    (session_id, last_message_id))
    conn.commit()


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
    # The expiry set the backoff clock as well (15 minutes out — this row's
    # first failure), and claim()'s 'failed' arm honours it, so an IMMEDIATE
    # reclaim must lose. In production this row comes back a cadence later.
    assert session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                               message_count=4) is False
    _make_due(conn, sid, 9)
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
                                     error="bad json", count_attempt=True) == (1, 1)
    # Reclaim the now-'failed' row via claim()'s 'failed' reclaim arm, so the
    # row goes back to 'claimed' with attempts == 1 before it goes stale below.
    # Due first: that arm honours the backoff clock mark_failed just set.
    _make_due(conn, sid, 9)
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
    _make_due(conn, sid, 9)
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
        # table_schema = current_schema() matters now that every test runs
        # inside its own schema: without it, a live database with its own
        # public.sweeper_status would union columns across both and could
        # false-pass this even if THIS test's table were missing them.
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'sweeper_status' AND table_schema = current_schema()")
        cols = {r[0] for r in cur.fetchall()}
    assert {"redispatched", "quarantined", "aborted", "locked_out",
            "stale_slices", "retried"} <= cols


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
                    "WHERE table_name = 'session_extraction' AND table_schema = current_schema()")
        se_cols = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'sweeper_status' AND table_schema = current_schema()")
        ss_cols = {r[0] for r in cur.fetchall()}
    # `retries` and `next_retry_at` belong on this assertion for the same
    # reason `attempts` does, and their absence would be worse: an existing
    # deployment that upgraded without them fails on the first mark_failed
    # (UndefinedColumn) rather than degrading, so every path into 'failed'
    # breaks at once on a stack that was working (ADR-0003 decision 3).
    assert {"attempts", "retries", "next_retry_at"} <= se_cols
    # The circuit-breaker columns are on this list for the same reason as the
    # rest: they arrive by ALTER, never by the CREATE above, so a deployment
    # whose sweeper_status predates them upgrades or the run-level breakers
    # keep leaving no durable trace at all.
    assert {"redispatched", "quarantined", "aborted", "locked_out",
            "stale_slices", "retried"} <= ss_cols


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
    # (attempts, retries) since ADR-0003 decision 3. `retries` moves on every
    # one of these three calls; `attempts` only on the two deterministic ones.
    assert session_store.mark_failed(conn, session_id=sid, last_message_id=9,
                                     error="bad json", count_attempt=True) == (1, 1)
    # A transient failure must not move the counter at all.
    assert session_store.mark_failed(conn, session_id=sid, last_message_id=9,
                                     error="gateway timeout", count_attempt=False) == (1, 2)
    # The row is 'failed' after mark_failed, so claim()'s reclaim arm re-wins it
    # without touching attempts - only mark_failed(count_attempt=True) does.
    session_store.claim(conn, session_id=sid, first_message_id=1, last_message_id=9,
                        message_count=4)
    assert session_store.mark_failed(conn, session_id=sid, last_message_id=9,
                                     error="bad json again", count_attempt=True) == (2, 3)


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
    # Randomised: pg_try_advisory_xact_lock keys are cluster-wide, not scoped
    # to this test's schema, so a fixed literal here could collide with a
    # concurrently-running copy of this same file (see the module docstring).
    lock_key = f"lock-me-{uuid.uuid4()}"
    other = psycopg.connect(DSN)
    try:
        with conn.transaction():
            assert session_store.try_session_lock(conn, lock_key) is True
            assert session_store.try_session_lock(other, lock_key) is False
        # transaction over -> the lock is gone without anyone releasing it
        with other.transaction():
            assert session_store.try_session_lock(other, lock_key) is True
    finally:
        other.close()


def test_the_production_lock_sequence_holds_across_watermarks_and_releases_at_claim(conn):
    """Drives the PRODUCTION sequence `sweep()` actually uses — never
    `with conn.transaction():`, which is the construct being avoided here.

    Production opens its transaction IMPLICITLY: `try_session_lock()` takes
    the advisory xact lock and deliberately does not commit (see its
    docstring), `watermarks()` in between is a plain read that does not
    commit either, and `claim()`'s own `conn.commit()` is what ends the
    transaction — which is also what releases the lock. That sequence is what
    holds the lock across the watermark re-read and the claim, while
    releasing it before the LLM call `extract()` makes next. Nothing here
    proves that by construction; it proves it by driving the real functions
    in the real order and watching a second connection get refused, then
    granted, at the right moments.
    """
    from scripts import session_store
    lock_key = f"prod-sequence-{uuid.uuid4()}"
    sid = f"s-{uuid.uuid4()}"
    other = psycopg.connect(DSN)
    try:
        assert session_store.try_session_lock(conn, lock_key) is True
        # The watermark re-read the fix depends on (session_sweeper.sweep,
        # "THE RE-READ IS THE FIX, NOT THE LOCK"). A plain SELECT — must not
        # commit, and does not, in the real function.
        session_store.watermarks(conn)
        # Still held: a second connection must be refused across the re-read,
        # exactly as it would be mid-sweep on a real host.
        assert session_store.try_session_lock(other, lock_key) is False
        # claim() is the ONLY commit in this sequence in production.
        assert session_store.claim(conn, session_id=sid, first_message_id=1,
                                   last_message_id=9, message_count=4) is True
        # Released now that claim() has committed — before any LLM call runs.
        assert session_store.try_session_lock(other, lock_key) is True
    finally:
        other.rollback()
        other.close()


def _next_retry_at(conn, session_id, last_message_id):
    """The backoff clock as the server wrote it. Read back rather than
    computed, because the clock is the server's, not this process's."""
    with conn.cursor() as cur:
        cur.execute("SELECT next_retry_at FROM session_extraction "
                    "WHERE session_id = %s AND last_message_id = %s",
                    (session_id, last_message_id))
        return cur.fetchone()[0]


def test_retries_counts_every_failure_and_attempts_only_deterministic(conn):
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    a, r = session_store.mark_failed(conn, session_id="s", last_message_id=10,
                                     error="timeout", count_attempt=False)
    assert (a, r) == (0, 1)
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    a, r = session_store.mark_failed(conn, session_id="s", last_message_id=10,
                                     error="unparseable", count_attempt=True)
    assert (a, r) == (1, 2)


def test_each_failure_pushes_next_retry_at_further_out(conn):
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    first = _next_retry_at(conn, "s", 10)
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    second = _next_retry_at(conn, "s", 10)
    # 15 min then 30 min, so the gap roughly doubles. Compared against each
    # other rather than against a literal: the clock is the server's.
    assert (second - first).total_seconds() > 12 * 60


def test_the_backoff_survives_a_row_that_failed_forty_times(conn):
    """Not just 'is it capped'. LEAST applies to the RESULT of the
    multiplication, so an uncapped POWER(2, retries) raises 'interval out of
    range' INSIDE mark_failed — measured at retries=40 on PostgreSQL 17. A row
    that has failed forty times would stop being recordable as failed."""
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    for _ in range(40):
        session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    with conn.cursor() as cur:
        cur.execute("SELECT next_retry_at - now() < interval '25 hours' "
                    "FROM session_extraction WHERE session_id='s' AND last_message_id=10")
        assert cur.fetchone()[0] is True


def test_failed_slices_returns_only_failed_rows_that_are_due(conn):
    from scripts import session_store
    for last, status in ((10, "failed"), (20, "published"), (30, "quarantined")):
        session_store.claim(conn, session_id="s", first_message_id=last - 9,
                            last_message_id=last, message_count=10)
        if status == "failed":
            session_store.mark_failed(conn, session_id="s", last_message_id=last, error="x")
        elif status == "published":
            session_store.mark_published(conn, session_id="s", last_message_id=last, jobs=[])
        else:
            session_store.mark_quarantined(conn, session_id="s", last_message_id=last,
                                           error="x")
    # (s,10) is failed, but mark_failed just scheduled it 15 minutes out, so it
    # is not DUE. Due-ness and status are two different questions.
    assert session_store.failed_slices(conn) == []
    with conn.cursor() as cur:
        cur.execute("UPDATE session_extraction SET next_retry_at = now() - interval '1 minute' "
                    "WHERE session_id='s' AND last_message_id=10")
    conn.commit()
    rows = session_store.failed_slices(conn)
    assert [(r["session_id"], r["first_message_id"], r["last_message_id"]) for r in rows] \
        == [("s", 1, 10)]


def test_a_row_that_predates_the_column_is_due_immediately(conn):
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    with conn.cursor() as cur:
        cur.execute("UPDATE session_extraction SET next_retry_at = NULL")
    conn.commit()
    assert len(session_store.failed_slices(conn)) == 1


def test_slice_status_is_none_for_a_row_that_does_not_exist(conn):
    from scripts import session_store
    assert session_store.slice_status(conn, "nobody", 1) is None


def test_a_row_rescheduled_between_selection_and_claim_is_not_reclaimed(conn):
    """The window ADR-0003 decision 3 puts the due-ness test inside the UPDATE
    for: `failed_slices()` reads BEFORE the session lock is taken, so another
    sweeper can claim the row, fail it and push its backoff hours out in
    between — leaving `status` back at 'failed', which a status-only re-read
    waves straight through. Checked in the claim, the row is either still due
    when it is taken or it is not taken."""
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    _make_due(conn, "s", 10)
    owed = session_store.failed_slices(conn)            # selected while due
    assert owed
    session_store.claim(conn, session_id="s", first_message_id=1,       # another sweeper
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    assert session_store.slice_status(conn, "s", 10) == "failed"        # status says go
    assert session_store.claim(conn, session_id="s", first_message_id=1,
                               last_message_id=10, message_count=10) is False


def _age_claim(conn, session_id, last_message_id, *, hours):
    """Backdate a claimed row so `expire_stale_claims` sees it as abandoned.

    Staleness is wall-clock, tested inside the bulk UPDATE, so a test that
    wants a stale row says so directly rather than sleeping for one.
    """
    with conn.cursor() as cur:
        cur.execute("UPDATE session_extraction "
                    "SET updated_at = now() - make_interval(hours => %s) "
                    "WHERE session_id = %s AND last_message_id = %s",
                    (hours, session_id, last_message_id))
    conn.commit()


def test_a_hole_closes_at_its_own_range(conn):
    """(s,1..10) failed under (s,11..20) published. The RECLAIM arm must fire on
    the failed row's own key — the fresh path can never reach it."""
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    session_store.claim(conn, session_id="s", first_message_id=11,
                        last_message_id=20, message_count=10)
    session_store.mark_published(conn, session_id="s", last_message_id=20, jobs=[])
    _make_due(conn, "s", 10)
    owed = session_store.failed_slices(conn)
    assert [(r["first_message_id"], r["last_message_id"]) for r in owed] == [(1, 10)]
    assert session_store.claim(conn, session_id="s", first_message_id=1,
                               last_message_id=10, message_count=10) is True
    session_store.mark_published(conn, session_id="s", last_message_id=10, jobs=[])
    assert session_store.slice_status(conn, "s", 10) == "published"


def test_the_frontier_includes_failed_so_the_fresh_path_starts_above_it(conn):
    """The double-claim guard. With the OLD rule the frontier here is 0 and the
    fresh path would build (1..25), overlapping the retry's (1..10) under a key
    UNIQUE (session_id, last_message_id) cannot see."""
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    assert session_store.watermarks(conn) == {"s": 10}


def test_attempts_accumulates_across_retries_on_the_same_row(conn):
    """_CLAIM_RECLAIM_SQL's DO UPDATE sets status and timestamps only, so the
    counter survives the reclaim. This is what #15 was missing: the key stops
    moving, so there is a counter to accumulate on.

    The `_make_due` before each re-claim is not decoration. Without it the
    reclaim arm's due-ness condition rejects every one of these claims (they
    return False, silently), `mark_failed` increments anyway because it is a
    keyed UPDATE that does not read `status`, and the test passes while proving
    nothing about the reclaim — the exact shape the plan's version had.
    """
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    for expected in (1, 2, 3):
        attempts, _ = session_store.mark_failed(conn, session_id="s", last_message_id=10,
                                                error="unparseable", count_attempt=True)
        assert attempts == expected
        _make_due(conn, "s", 10)
        assert session_store.claim(conn, session_id="s", first_message_id=1,
                                   last_message_id=10, message_count=10) is True


def test_three_deterministic_failures_quarantine_and_the_frontier_is_unmoved(conn):
    """ADR-0003's own named risk: with the frontier, a failed row consumes the
    watermark, so if the retry path ever regresses these messages are
    unreachable from BOTH passes. Write this one first."""
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    for _ in range(3):
        session_store.mark_failed(conn, session_id="s", last_message_id=10,
                                  error="unparseable", count_attempt=True)
        session_store.claim(conn, session_id="s", first_message_id=1,
                            last_message_id=10, message_count=10)
    session_store.mark_quarantined(conn, session_id="s", last_message_id=10, error="ceiling")
    assert session_store.watermarks(conn) == {"s": 10}
    assert session_store.failed_slices(conn) == []


def test_forty_transient_failures_never_quarantine_and_stay_recoverable(conn):
    """The counter has no ceiling by design. After forty failures the row is
    still 'failed', still carries its range, and comes back the moment its
    backoff elapses."""
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    for _ in range(40):
        session_store.mark_failed(conn, session_id="s", last_message_id=10, error="timeout")
    assert session_store.slice_status(conn, "s", 10) == "failed"
    _make_due(conn, "s", 10)
    assert [(r["first_message_id"], r["last_message_id"])
            for r in session_store.failed_slices(conn)] == [(1, 10)]


def test_a_stale_claim_is_rescheduled_not_retried_immediately(conn):
    """expire_stale_claims is bulk SQL, so its backoff is computed in the
    UPDATE. If it is not, a slice that crashes the process every time comes
    back every single cadence forever."""
    from scripts import session_store
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    _age_claim(conn, "s", 10, hours=3)
    assert session_store.expire_stale_claims(conn) == 1
    assert session_store.slice_status(conn, "s", 10) == "failed"
    assert session_store.failed_slices(conn) == []      # scheduled, not due
