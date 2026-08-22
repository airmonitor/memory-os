"""PostgreSQL bookkeeping for session extraction.

The three side effects of a sweep — a fabric file, a watermark, an ARQ job —
cannot share one transaction, so the ordering here is the correctness argument:

    claim (unique row)  ->  extract (LLM)  ->  publish (files)  ->  dispatch (ARQ)

A crash anywhere leaves the slice re-runnable and non-duplicating: the claim
stops a second extraction, and the deterministic filename makes republication
an overwrite.

The third leg is weaker than it used to be written. arq refuses a duplicate
`_job_id` only while that job's result key still exists, and `keep_result` is
3600 s in `config/services.yaml` — so a re-dispatch is a no-op for one hour
after the first delivery, not forever. A replay later than that enqueues again,
and because `ingest_memory` assigns a fresh `uuid.uuid4()` per point, the
second run writes a DUPLICATE Qdrant point rather than overwriting the first.
What is guaranteed: no second LLM call, no second fabric file, and no double
ingestion inside the result-retention window. Making the point id deterministic
is the real fix; it belongs in the worker image
(`docker/worker/tasks/ingestion.py`) and is tracked separately.

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
    # sweeper_status predates `redispatched`, and this repo has no migration
    # runner for host-side scripts — so the upgrade path for a table that
    # already exists is this ALTER, not the CREATE above.
    """
    ALTER TABLE sweeper_status
        ADD COLUMN IF NOT EXISTS redispatched INTEGER NOT NULL DEFAULT 0
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


_EXPIRE_STALE_SQL = """
    UPDATE session_extraction
       SET status = 'failed', error = 'stale claim expired', updated_at = now()
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


def record_run(conn, *, candidates, extracted, entries, jobs, schema_version, error,
               redispatched=0) -> None:
    """One row per run, success or failure — "stalled" has to be a query.

    `redispatched` is here because a backlog that never drains is exactly the
    shape of failure this table exists to surface: slices stuck at 'extracted'
    get re-offered every sweep, so a non-zero count that never falls to zero
    means the broker is not accepting them. It used to be computed by `sweep()`
    and thrown away into a log line.
    """
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sweeper_status
                   (candidates, extracted, entries, jobs, redispatched,
                    schema_version, error)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (candidates, extracted, entries, jobs, redispatched, schema_version,
             str(error)[:2000] if error else None))
    conn.commit()
