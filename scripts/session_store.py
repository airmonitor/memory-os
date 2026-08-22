"""PostgreSQL bookkeeping for session extraction.

The three side effects of a sweep — a fabric file, a watermark, an ARQ job —
cannot share one transaction, so the ordering here is the correctness argument:

    claim (unique row)  ->  extract (LLM)  ->  publish (files)  ->  dispatch (ARQ)

A crash anywhere leaves the slice re-runnable and non-duplicating: the claim
stops a second extraction, and the deterministic filename makes republication
an overwrite.

The third leg is still the weakest, but it no longer duplicates. arq refuses a
duplicate `_job_id` only while that job's result key still exists, and
`keep_result` is 3600 s in `config/services.yaml` — so a re-dispatch is a no-op
for one hour after the first delivery, not forever, and a replay later than
that really does enqueue the job a second time. What that second delivery does
in Qdrant is the part that changed (ADR-0002 decision 2): `ingest_memory` now
takes an optional `point_id`, and the sweeper passes
`uuid5(NAMESPACE_URL, job_id)` (`scripts.session_sweeper.point_id`) for every
entry it dispatches or re-dispatches, so the
replay UPSERTS the same point instead of adding a second copy of the same
memory. `uuid4()` remains the worker's default for every other producer.

The deterministic point id is also what makes a deliberate replay safe — a
re-extraction of the same slice overwrites its own points rather than
accumulating them — which is why the operator page documents reconciliation
only for the two cases it cannot cover: a re-extraction that yields FEWER
entries (the dropped `job_id`s never recur, so their points are orphaned) and
points written before this shipped, which carry a `uuid4` id nothing can
compute back from.

What is guaranteed: no second fabric file, no second ARQ delivery inside the
result-retention window, no duplicate Qdrant point even outside it, and no
second LLM call once `mark_extracted` has banked the payload — a crash in the
window between the model answering and that commit leaves the row at 'claimed',
and the reclaim correctly pays for the call again, because at that point
nothing durable exists to reuse.

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

# BELOW memos_config, never above: that import's side effect is what puts
# vendor/ on sys.path in the deployed pod. tests/test_import_order.py enforces
# it for every script in this tree.
import logging  # noqa: E402
import psycopg  # noqa: E402

logger = logging.getLogger(__name__)

STALE_CLAIM_HOURS = 2

# 15 min, doubling, capped at a day: the first retry is one cron cadence later,
# the fifth is four hours later, the eighth and everything after is daily. Long
# enough that a hopeless row stops costing a sweep; short enough that a row
# waiting out an outage comes back as soon as the outage is over.
RETRY_BACKOFF_BASE = "15 minutes"
RETRY_BACKOFF_CAP = "24 hours"

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
    # sweeper_status predates `redispatched`, and this repo has no migration
    # runner for host-side scripts — so the upgrade path for a table that
    # already exists is this ALTER, not the CREATE above.
    """
    ALTER TABLE sweeper_status
        ADD COLUMN IF NOT EXISTS redispatched INTEGER NOT NULL DEFAULT 0
    """,
    # session_extraction predates `attempts` (ADR-0002 decision 4) for the same
    # reason — the migration path for an existing table is an ALTER, not the
    # CREATE above. Counts only genuine, classified-deterministic extraction
    # failures; see mark_failed.
    """
    ALTER TABLE session_extraction
        ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0
    """,
    # `attempts` (above) counts only classified-deterministic failures, which
    # is right — an outage must not spend the budget that exists for content
    # the model cannot parse. But that left THREE producers of 'failed' with no
    # exit at all: expire_stale_claims, a transient ExtractionFailed, and the
    # generic `except Exception`. All three call mark_failed(count_attempt=
    # False). Before ADR-0003 they were harmless because the next sweep
    # re-derived a fresh slice with a new key; now that a failed row is retried
    # AT ITS OWN RANGE, the same row comes back forever. This counter is the
    # bound for those three, and it counts every transition into 'failed'
    # regardless of classification. NOT a maximum age: a stack that was down
    # for two days would quarantine every open slice on the first boot after
    # it, having never retried any of them (ADR-0003 decision 3).
    """
    ALTER TABLE session_extraction
        ADD COLUMN IF NOT EXISTS retries INTEGER NOT NULL DEFAULT 0
    """,
    # WHAT BOUNDS THOSE THREE, and it is not a ceiling. Revision 2 of ADR-0003
    # proposed retiring a row after 20 retries; review killed it, correctly: a
    # 'quarantined' row still counts toward the frontier while the retry pass
    # reads only 'failed' rows, so quarantining is precisely how messages
    # become unreachable from BOTH passes. A five-hour gateway outage would
    # have permanently discarded every slice it touched. What actually needs
    # bounding is the RATE, not the count — so the row is rescheduled instead.
    #
    # NULL means due now, which is what every row that predates this column
    # already is: no backfill (ADR-0003 decision 3).
    """
    ALTER TABLE session_extraction
        ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ
    """,
    """
    ALTER TABLE sweeper_status
        ADD COLUMN IF NOT EXISTS quarantined INTEGER NOT NULL DEFAULT 0
    """,
    # The three circuit-breaker outcomes, same ALTER shape and same reason.
    # They are here because WITHOUT them the run-level breakers left no durable
    # trace at all: sweep() computed `aborted`, `locked_out` and
    # `stale_slices`, main() dropped them on the floor, and the exact scenario
    # the cross-session breaker exists for -- a misrouting gateway, two or more
    # sessions, every run claims, fails deterministically, rolls back, aborts
    # -- wrote `candidates=2, extracted=0, quarantined=0, error=NULL`, run
    # after run, while burning two LLM calls each time. `quarantined` is
    # deliberately ZEROED on that path (see session_sweeper.sweep), so the one
    # signal ADR-0002 names reads 0 exactly when the systemic failure is
    # happening. `aborted` is what an operator queries instead.
    """
    ALTER TABLE sweeper_status
        ADD COLUMN IF NOT EXISTS aborted BOOLEAN NOT NULL DEFAULT false
    """,
    # ONE counter, not two, and that is a decision rather than an omission:
    # `locked_out` covers both "another sweeper holds this session's advisory
    # lock" and "we held the lock but the watermark re-read round trip
    # failed". The sweeper's own comment at that re-read records why (fix
    # round 1): from the run's perspective the candidate was not safely
    # processable past the lock step either way, and a third counter for "held
    # the lock but couldn't confirm freshness" would not tell an operator
    # anything they would act on differently. Both are transient-by-nature and
    # both mean "offered again next sweep".
    """
    ALTER TABLE sweeper_status
        ADD COLUMN IF NOT EXISTS locked_out INTEGER NOT NULL DEFAULT 0
    """,
    """
    ALTER TABLE sweeper_status
        ADD COLUMN IF NOT EXISTS stale_slices INTEGER NOT NULL DEFAULT 0
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


_TRY_SESSION_LOCK_SQL = "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))"


def try_session_lock(conn, session_id: str) -> bool:
    """Win the exclusive right to sweep THIS session for the rest of the
    caller's transaction (ADR-0002 decision 3).

    `pg_try_advisory_xact_lock`, not the session-scoped `pg_try_advisory_lock`:
    the xact-scoped variant is released on commit or rollback of the CALLER's
    transaction — including the rollback a crashed process gets for free. A
    session-scoped lock held by a hung process would block every later sweep
    of that session until its connection dies: an unbounded stall traded for a
    rare double-extraction. Which means THIS FUNCTION MUST NEVER COMMIT — a
    commit here would release the lock immediately, before `claim()` ever runs
    in the same transaction as this call.

    Keyed on the session id via `hashtextextended`, bound as a parameter (never
    interpolated) — not a single fixed key, which would serialise unrelated
    sessions and targeted repair runs that touch entirely different rows.

    THE LOCK ALONE PROTECTS NOTHING. Two sweeps that both read `watermarks()`
    before either takes this lock compute DIFFERENT slice boundaries for the
    same underlying messages, so their claims land on different
    `(session_id, last_message_id)` keys — the UNIQUE constraint never fires,
    and both win. The caller MUST re-read this session's watermark after
    winning the lock and drop the slice if it has moved past
    `first_message_id`; see the per-slice loop in `session_sweeper.sweep()`.
    """
    with conn.cursor() as cur:
        cur.execute(_TRY_SESSION_LOCK_SQL, (session_id,))
        row = cur.fetchone()
    return bool(row[0])


_CLAIM_INSERT_COLUMNS = "(session_id, first_message_id, last_message_id, message_count)"

_CLAIM_FAST_SQL = f"""
    INSERT INTO session_extraction {_CLAIM_INSERT_COLUMNS}
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (session_id, last_message_id) DO NOTHING
    RETURNING id
"""

_CLAIM_RECLAIM_SQL = f"""
    INSERT INTO session_extraction {_CLAIM_INSERT_COLUMNS}
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (session_id, last_message_id) DO UPDATE
        SET status = 'claimed', claimed_at = now(), updated_at = now()
        WHERE session_extraction.status = 'failed'
           OR (session_extraction.status = 'claimed'
               AND session_extraction.updated_at
                   < now() - make_interval(hours => %s))
    RETURNING id
"""


# BULK SQL over every stale row at once, which is exactly why the backoff is
# computed in the UPDATE and not in Python — there is no per-row Python pass
# here to compute it in. The two clauses are identical to _MARK_FAILED_SQL's,
# inner LEAST included; that comment explains why the exponent cap is not
# redundant, and this statement is the reason its blast radius is a whole
# sweep rather than a single row.
_EXPIRE_STALE_SQL = """
    UPDATE session_extraction
       SET status = 'failed', error = 'stale claim expired', updated_at = now(),
           -- A stale claim is a failure like any other from this counter's point
           -- of view: the slice was taken and no answer came back. Not counting it
           -- here would let a slice that crashes the process every single time
           -- retry every cadence forever, which is the shape ADR-0003 decision 3
           -- bounds. It is rescheduled, never retired: the process crashing is not
           -- evidence about the conversation.
           retries = session_extraction.retries + 1,
           next_retry_at = now() + LEAST(
               INTERVAL '15 minutes' * POWER(2, LEAST(session_extraction.retries, 7)),
               INTERVAL '24 hours')
     WHERE status = 'claimed'
       AND updated_at < now() - make_interval(hours => %s)
"""


def expire_stale_claims(conn, *, stale_hours=STALE_CLAIM_HOURS) -> int:
    """Turn abandoned 'claimed' rows into 'failed' ones. Returns how many.

    THIS MUST RUN BEFORE `watermarks()` IS READ, and the reason is that
    `claim()`'s own stale-claimed branch could never fire for the case it was
    written for. A hard crash leaves a row at 'claimed'; `watermarks()` counts
    it; `find_candidates` then sees zero pending messages for that session and
    skips it — so `claim()` is never called with that key, the ON CONFLICT
    never happens, and the reclaim arm is unreachable. Measured: a stuck
    'claimed' row at (s, 12) with 12 unextracted messages yields
    `candidates: 0` forever.

    Expiring the row here instead drops it out of the watermark, which puts the
    slice back in front of `find_candidates`, and the existing 'failed' arm of
    `claim()` then re-claims it. No new state, no new path.
    """
    with conn.cursor() as cur:
        cur.execute(_EXPIRE_STALE_SQL, (stale_hours,))
        expired = cur.rowcount or 0
    conn.commit()
    return expired


def claim(conn, *, session_id, first_message_id, last_message_id, message_count,
          stale_hours=None) -> bool:
    """Win the right to extract this slice. False means somebody else owns it.

    THE RECLAIM STEP IS NOT DECORATION. A crash between the claim and the
    extraction leaves a row stuck at 'claimed', and because `watermarks()`
    counts it, the slice would never be offered again — one lost conversation
    per crash, silently, forever. So a fresh `INSERT ... DO NOTHING` is tried
    first (the common case: a brand-new slice, or one somebody else legitimately
    owns), and only on conflict does a second statement attempt the reclaim: a
    claim already marked 'failed', or one older than `stale_hours`, is
    re-claimable; a fresh 'claimed' row is not.

    Which arm actually fires: the 'failed' one. `expire_stale_claims()` runs at
    the top of every sweep and has already rewritten abandoned 'claimed' rows to
    'failed' by the time a candidate reaches here — it has to, because a row
    left at 'claimed' never becomes a candidate at all (see that function). The
    stale-'claimed' arm is kept as belt-and-braces for a row that goes stale
    between the expiry and this statement, i.e. a second sweeper mid-flight.

    This runs on every call — with no `stale_hours` given it falls back to
    `STALE_CLAIM_HOURS` — so the production call site does not have to remember
    to opt in. Passing `stale_hours` explicitly skips straight to the reclaim
    statement.

    Claiming (this function) never touches `attempts` — grabbing ownership of
    a slice is not yet attempting it. ADR-0002 decision 4 counts only genuine,
    classified-deterministic extraction failures, and that counting lives
    entirely in `mark_failed`'s `count_attempt` parameter. An earlier revision
    threaded a `count_attempt` parameter through this function's reclaim SQL
    too; it was removed (fix round 1) because the sweeper's only call site
    could never pass anything but the value that means "don't count" — a
    parameter with one legal value is worse than no parameter.
    """
    ins = (session_id, first_message_id, last_message_id, message_count)
    with conn.cursor() as cur:
        if stale_hours is not None:
            cur.execute(_CLAIM_RECLAIM_SQL, (*ins, stale_hours))
            won = cur.fetchone() is not None
        else:
            cur.execute(_CLAIM_FAST_SQL, ins)
            won = cur.fetchone() is not None
            if not won:
                cur.execute(_CLAIM_RECLAIM_SQL, (*ins, STALE_CLAIM_HOURS))
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


# Two things about the backoff expression are load-bearing and neither is
# obvious:
#
# 1. `session_extraction.retries` on the right-hand side is the value BEFORE
#    this statement's increment, so the first failure schedules 15 minutes out
#    (2^0) and the second 30 (2^1). Off by one and every row waits double from
#    the start.
# 2. THE INNER LEAST(…, 7) IS NOT REDUNDANT WITH THE OUTER ONE, and the blast
#    radius is larger than one row. `_EXPIRE_STALE_SQL` carries the same
#    expression over EVERY stale row in one statement, so a single row that has
#    aged past the overflow point aborts the whole top-of-sweep expiry — and
#    `expire_stale_claims` running is what makes abandoned rows visible to
#    `find_candidates` at all. One poison row would stop every sweep. The outer
#    LEAST applies to the RESULT of the multiplication, so an uncapped exponent
#    overflows the interval type before the cap can help. Measured on
#    PostgreSQL 17, 2026-08-22: POWER(2, 40) there raises `ERROR: interval out
#    of range` — inside `mark_failed`, which means a slice that has failed
#    forty times can no longer be recorded as failed at all. Verified schedule
#    with the inner cap: 15m, 30m, 1h, 2h, 4h, 8h, 16h, 24h, 24h, 24h for
#    retries = 0..9.
_MARK_FAILED_SQL = """
    UPDATE session_extraction
       SET status = 'failed', error = %s,
           attempts = attempts + CASE WHEN %s THEN 1 ELSE 0 END,
           retries = session_extraction.retries + 1,
           next_retry_at = now() + LEAST(
               INTERVAL '15 minutes' * POWER(2, LEAST(session_extraction.retries, 7)),
               INTERVAL '24 hours'),
           updated_at = now()
     WHERE session_id = %s AND last_message_id = %s
    RETURNING attempts, retries
"""


def mark_failed(conn, *, session_id, last_message_id, error,
                count_attempt=False) -> tuple[int, int]:
    """Mark a slice retryable and return its counters after this call.

    `count_attempt` is the classification decision itself (ADR-0002 decision
    4): a transient failure (connection error, timeout, HTTP status error, a
    missing key, or a decoded body that is not a chat completion) calls this
    with `count_attempt=False` — an outage must not spend one of the three
    tries a genuinely bad slice gets. A deterministic failure (the gateway
    answered like a gateway; the model's own content did not parse or
    validate) calls this with `count_attempt=True`, and the caller compares
    the returned value against `session_extraction.max_attempts` to decide
    whether to quarantine instead.

    Returns `(attempts, retries)`. `attempts` is the classification decision
    (ADR-0002 decision 4); `retries` is every failure this row has ever had and
    is what bounds the three producers `attempts` deliberately does not count
    (ADR-0003 decision 3). `attempts` has a ceiling and quarantines; `retries`
    has none and never does — it only drives `next_retry_at`.
    """
    with conn.cursor() as cur:
        cur.execute(_MARK_FAILED_SQL,
                    (str(error)[:2000], count_attempt, session_id, last_message_id))
        row = cur.fetchone()
    conn.commit()
    return (int(row[0]), int(row[1])) if row else (0, 0)


def mark_quarantined(conn, *, session_id, last_message_id, error) -> None:
    """Retire a slice that hit the deterministic-failure ceiling.

    Only status and error change — the payload column is left as-is (there
    normally is none, since extraction never once succeeded for this slice).
    `watermarks()` excludes only 'failed', so a quarantined slice still counts
    toward the watermark and the session's later messages are not blocked
    behind one that will never parse (ADR-0002 decision 4). The one-statement
    operator replay is `UPDATE session_extraction SET status='failed',
    attempts=0 WHERE ...`.
    """
    _update(conn, session_id, last_message_id, "status = 'quarantined', error = %s",
            (str(error)[:2000],))


_ROLLBACK_ATTEMPT_SQL = """
    UPDATE session_extraction
       SET status = 'failed', attempts = GREATEST(attempts - 1, 0), updated_at = now()
     WHERE session_id = %s AND last_message_id = %s
"""


def rollback_attempt(conn, *, session_id, last_message_id) -> None:
    """Undo one counted deterministic failure.

    Used only by the sweeper's cross-session circuit breaker: two
    deterministic failures in different sessions inside one run are systemic
    (a misrouting gateway), not two bad slices, so the run aborts and every
    attempt it spent this run is refunded — the row is left exactly as if the
    ceiling had never been touched (ADR-0002 decision 4).
    """
    with conn.cursor() as cur:
        cur.execute(_ROLLBACK_ATTEMPT_SQL, (session_id, last_message_id))
    conn.commit()


def _update(conn, session_id, last_message_id, assignment, params) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE session_extraction SET {assignment}, updated_at = now()
                WHERE session_id = %s AND last_message_id = %s""",
            (*params, session_id, last_message_id))
    conn.commit()


def pending_dispatch(conn, limit=50) -> list[dict]:
    """Slices extracted but never dispatched — a crash or a broker outage.

    Returns [] when the table does not exist yet, instead of raising. That is
    not defensive noise: `sweep()` calls this through `redispatch()` BEFORE it
    does anything else, and `--dry-run` stubs `ensure_schema` precisely so a
    dry run creates nothing. On a database where the sweeper has never run for
    real, those two facts meet and `--dry-run` dies with UndefinedTable on the
    first thing it touches — measured on the semitora host, 2026-08-22, on the
    very first dry run against a fresh database. A dry run that cannot survive
    a fresh database is useless exactly when an operator most wants one.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT session_id, last_message_id, payload FROM session_extraction
                           WHERE status = 'extracted' ORDER BY id ASC LIMIT %s""", (limit,))
            return [{"session_id": r[0], "last_message_id": int(r[1]), "payload": r[2] or []}
                    for r in cur.fetchall()]
    except psycopg.errors.UndefinedTable:
        conn.rollback()          # the failed statement poisons the transaction
        logger.info("session_extraction does not exist yet — nothing to re-dispatch")
        return []


def record_run(conn, *, candidates, extracted, entries, jobs, schema_version, error,
               redispatched=0, quarantined=0, aborted=False, locked_out=0,
               stale_slices=0) -> None:
    """One row per run, success or failure — "stalled" has to be a query.

    `redispatched` is here because a backlog that never drains is exactly the
    shape of failure this table exists to surface: slices stuck at 'extracted'
    get re-offered every sweep, so a non-zero count that never falls to zero
    means the broker is not accepting them. It used to be computed by `sweep()`
    and thrown away into a log line.

    `quarantined` is the same idea for the failure ceiling (ADR-0002 decision
    4): nothing alerts on it yet, so this is the signal an operator has to go
    looking for — a non-zero count is a slice that will never be retried
    again without the documented manual replay.

    `aborted`, `locked_out` and `stale_slices` are the run-level circuit
    breakers and the lock, and they are here for a stronger reason than
    completeness: every one of them used to be computed by `sweep()` and
    dropped by `main()`, so the events they describe NEVER REACHED THIS TABLE
    AT ALL. A misrouting gateway hitting two sessions trips the cross-session
    breaker, which rolls the attempts back and — correctly — zeroes
    `quarantined`; the row it left behind read `candidates=2, extracted=0,
    quarantined=0, error=NULL`, i.e. indistinguishable from a quiet, healthy
    run, while the sweep burned two LLM calls every 15 minutes. `error` stays
    NULL there too, because the run did not crash: it defended itself. So
    `aborted = true` is the only durable evidence that it happened.

    `locked_out` is deliberately ONE counter covering two causes (lock refused,
    and watermark re-read failed while holding the lock) — see the ALTER's
    comment in SCHEMA and the sweeper's own note at that re-read.

    All five keyword arguments default, and that is load-bearing rather than
    tidy: `record_run` has callers that predate each of them (including the
    fake-backed unit tests), and a required parameter here would turn a
    bookkeeping addition into a break at the one call site whose whole job is
    to never raise.
    """
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sweeper_status
                   (candidates, extracted, entries, jobs, redispatched,
                    quarantined, aborted, locked_out, stale_slices,
                    schema_version, error)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (candidates, extracted, entries, jobs, redispatched, quarantined,
             bool(aborted), locked_out, stale_slices,
             schema_version, str(error)[:2000] if error else None))
    conn.commit()
