# Retry the Row Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `failed` extraction slice is retried at its own recorded message range instead of being re-derived from the watermark, so a hole under a published slice closes and the attempt ceiling can actually fire.

**Architecture:** Two candidate sources feed one processing loop, interleaved. The retry pass reads `session_extraction` rows at `status='failed'` that are due, and reads exactly their `first_message_id..last_message_id` out of `state.db`; the fresh pass keeps working as it does today but over a watermark that now includes failed rows (the frontier). Only two things are terminal — an emptied range and the existing deterministic ceiling. Every other failure is rescheduled on an exponential backoff, never retired, because a `quarantined` row counts toward the frontier and would take its messages out of reach of both passes.

**Tech Stack:** Python 3.11+, psycopg 3, sqlite3 (read-only), arq, pytest. PostgreSQL 17 in Docker for the integration suite.

**Spec:** `docs/adr/0003-the-watermark-is-a-contiguous-prefix.md` (revision 3)

## Global Constraints

- `import memos_config` MUST precede any vendored import in every script under `scripts/` — `tests/test_import_order.py` enforces it.
- No new table. Schema changes are appended to the `SCHEMA` tuple in `scripts/session_store.py` as `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, never by editing the `CREATE TABLE`.
- `attempts` counts classified-deterministic failures ONLY (ADR-0002 decision 4) and has a ceiling that quarantines. `retries` counts every transition into `failed`, has NO ceiling, and drives the backoff clock. They are two columns and must never be merged.
- A transient, unclassified or stale-claim failure must NEVER reach `mark_quarantined`. A quarantined row counts toward the frontier, so retiring one puts its messages out of reach of both passes — that is data loss, and ADR-0003 decision 3 exists to forbid it.
- The sweeper is fail-open per slice: one bad round trip logs a warning and `continue`s; it never ends the run.
- Comments in this repo record measured failures. Every non-obvious decision below carries its reason in the code, not only in this plan.
- Offline suite (`pytest tests/ -x -q`) must be green at every commit. The integration suite needs Docker and is run explicitly.

---

### Task 1: The `retries` counter and the backoff clock

**Files:**
- Modify: `scripts/session_store.py` (SCHEMA tuple, `mark_failed`, `_EXPIRE_STALE_SQL`)
- Test: `tests/test_session_store.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `mark_failed(...) -> tuple[int, int]` returning `(attempts, retries)`; columns `retries` and `next_retry_at` on `session_extraction`.

**Two traps:**
1. `mark_failed` currently returns `attempts` as a bare int and the sweeper compares it against `max_attempts` at `scripts/session_sweeper.py:350`. Changing the return type breaks that call site — update it in this task, in the same commit.
2. `tests/test_integration_postgres.py:269`, `test_mark_failed_returns_the_incremented_attempts_count`, asserts the bare int. It must be updated in this task too. The integration fixture in that file is named **`conn`**, not `pg_conn`.

- [ ] **Step 1: Write the failing test**

```python
def test_retries_counts_every_failure_and_attempts_only_deterministic(conn):
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
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    for _ in range(40):
        session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    with conn.cursor() as cur:
        cur.execute("SELECT next_retry_at - now() < interval '25 hours' "
                    "FROM session_extraction WHERE session_id='s' AND last_message_id=10")
        assert cur.fetchone()[0] is True
```

- [ ] **Step 2: Run it to see it fail**

Run: `pytest tests/test_integration_postgres.py -k "retries_counts or next_retry_at or failed_forty_times" -v`
Expected: FAIL — `mark_failed` returns an int, and neither column exists.

- [ ] **Step 3: Add the columns**

Append to the `SCHEMA` tuple, after the `attempts` ALTER:

```python
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
```

and, immediately after it, the clock that makes the counter useful:

```python
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
```

Add the schedule constants next to `STALE_CLAIM_HOURS`:

```python
# 15 min, doubling, capped at a day: the first retry is one cron cadence later,
# the fifth is four hours later, the eighth and everything after is daily. Long
# enough that a hopeless row stops costing a sweep; short enough that a row
# waiting out an outage comes back as soon as the outage is over.
RETRY_BACKOFF_BASE = "15 minutes"
RETRY_BACKOFF_CAP = "24 hours"
```

- [ ] **Step 4: Increment it in both producers**

In `mark_failed`, return both counters:

```python
def mark_failed(conn, *, session_id, last_message_id, error, count_attempt=False) -> tuple[int, int]:
    """... existing docstring ...

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
```

and its SQL gains, in the `SET` list:

```sql
       retries = session_extraction.retries + 1,
       next_retry_at = now() + LEAST(
           INTERVAL '15 minutes' * POWER(2, LEAST(session_extraction.retries, 7)),
           INTERVAL '24 hours')
```

plus `RETURNING attempts, retries`.

Two things about that expression are load-bearing and neither is obvious:

1. `session_extraction.retries` on the right-hand side is the value BEFORE this statement's
   increment, so the first failure schedules 15 minutes out (`2^0`) and the second 30 (`2^1`).
   Off by one and every row waits double from the start.
2. **The inner `LEAST(…, 7)` is not redundant with the outer one.** The outer `LEAST` applies to
   the RESULT of the multiplication, so an uncapped exponent overflows the interval type before
   the cap can help. Measured on PostgreSQL 17, 2026-08-22: `POWER(2, 40)` there raises
   `ERROR: interval out of range` — inside `mark_failed`, which means a slice that has failed
   forty times can no longer be recorded as failed at all. Verified schedule with the inner cap:
   `15m, 30m, 1h, 2h, 4h, 8h, 16h, 24h, 24h, 24h` for `retries = 0..9`.

`_EXPIRE_STALE_SQL` gets the identical two clauses. It is BULK SQL over every stale row at
once, which is exactly why the backoff is computed in the UPDATE and not in Python — there is no
per-row Python pass here to compute it in:

```sql
       -- A stale claim is a failure like any other from this counter's point
       -- of view: the slice was taken and no answer came back. Not counting it
       -- here would let a slice that crashes the process every single time
       -- retry every cadence forever, which is the shape ADR-0003 decision 3
       -- bounds. It is rescheduled, never retired: the process crashing is not
       -- evidence about the conversation.
       retries = session_extraction.retries + 1,
       next_retry_at = now() + LEAST(
           INTERVAL '15 minutes' * POWER(2, LEAST(session_extraction.retries, 7)),
           INTERVAL '24 hours'),
```

- [ ] **Step 5: Fix the one call site**

`scripts/session_sweeper.py:350` becomes:

```python
            attempts, retries = deps.pg.mark_failed(
                session_id=cand.session_id, last_message_id=last_id,
                error=exc, count_attempt=True)
```

and the two `count_attempt=False` sites (lines ~327 and ~386) discard the tuple for now — Task 5 uses it. Update `FakePg.mark_failed` in `tests/test_session_sweeper.py` to track `self.retries` and return the tuple.

- [ ] **Step 6: Run the suites**

Run: `pytest tests/ -x -q` then `pytest tests/test_integration_postgres.py -v` (needs Docker)
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add scripts/session_store.py scripts/session_sweeper.py tests/
git commit -m "feat(store): a retries counter and a backoff clock for the three failures attempts will not count"
```

---

### Task 2: `failed_slices()` — the retry pass's candidate source

**Files:**
- Modify: `scripts/session_store.py`
- Test: `tests/test_session_store.py`

**Interfaces:**
- Produces: `failed_slices(conn, limit=50) -> list[dict]` with keys `session_id`, `first_message_id`, `last_message_id`, `attempts`, `retries`; `slice_status(conn, session_id, last_message_id) -> str | None`.

- [ ] **Step 1: Write the failing test**

```python
def test_failed_slices_returns_only_failed_rows_that_are_due(conn):
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
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    with conn.cursor() as cur:
        cur.execute("UPDATE session_extraction SET next_retry_at = NULL")
    conn.commit()
    assert len(session_store.failed_slices(conn)) == 1


def test_slice_status_is_none_for_a_row_that_does_not_exist(conn):
    assert session_store.slice_status(conn, "nobody", 1) is None
```

- [ ] **Step 2: Run it to see it fail**

Run: `pytest tests/test_integration_postgres.py -k "failed_slices or predates_the_column or slice_status" -v`
Expected: FAIL — `AttributeError: module 'scripts.session_store' has no attribute 'failed_slices'`

- [ ] **Step 3: Implement**

```python
def failed_slices(conn, limit=50) -> list[dict]:
    """Slices that owe an answer, oldest first.

    'failed' ONLY. A 'quarantined' row is terminal by definition and must never
    come back here, or the ceiling that retired it would retire it again every
    sweep forever. 'claimed' is somebody else's work in flight — expire_stale_
    claims is what turns an abandoned one into a 'failed' row this can see, and
    it runs at the top of every sweep for exactly that reason.

    DUE ONLY. `next_retry_at` is the backoff clock (ADR-0003 decision 3): a row
    that keeps failing is pushed further out each time rather than retired, so
    it stops costing a sweep every cadence without its messages being thrown
    away. NULL is due — that is every row written before the column existed.

    Oldest first (by updated_at) so a hole that has been open longest is
    offered before a fresh failure, and so the ordering is stable across sweeps
    rather than whatever the planner returns.

    Returns [] when the table does not exist yet, for the same reason
    pending_dispatch does: `--dry-run` against a database that has never been
    swept must report what it would do, not crash on a missing relation.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT session_id, first_message_id, last_message_id, attempts, retries
                     FROM session_extraction
                    WHERE status = 'failed'
                      AND (next_retry_at IS NULL OR next_retry_at <= now())
                    ORDER BY updated_at ASC
                    LIMIT %s""", (limit,))
            rows = cur.fetchall()
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        return []
    return [{"session_id": r[0], "first_message_id": int(r[1]),
             "last_message_id": int(r[2]), "attempts": int(r[3]),
             "retries": int(r[4])} for r in rows]


def slice_status(conn, session_id: str, last_message_id: int) -> str | None:
    """One row's status, or None if there is no such row.

    Exists for the sweeper's post-lock freshness re-read on a RETRY slice, and
    the targeting is the point: `failed_slices` is a windowed, due-filtered
    list, so asking "is my row still in it" reports a false "somebody else
    resolved this" the moment there are more than `limit` owed rows. Status is
    the only question that re-read is actually asking.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM session_extraction "
                    "WHERE session_id = %s AND last_message_id = %s",
                    (session_id, last_message_id))
        row = cur.fetchone()
    return row[0] if row else None
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_integration_postgres.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/session_store.py tests/test_integration_postgres.py
git commit -m "feat(store): failed_slices and slice_status, the retry pass's candidate source"
```

---

### Task 3: `read_slice_range()` — read the row's own range

**Files:**
- Modify: `icarus/hermes_state.py`
- Test: `tests/test_hermes_state.py`

**Interfaces:**
- Produces: `read_slice_range(con, session_id, *, first_id, last_id) -> list[Message]`; `session_source(con, session_id) -> str`.

- [ ] **Step 1: Write the failing tests**

```python
def test_read_slice_range_is_inclusive_at_both_ends(state_db):
    msgs = hermes_state.read_slice_range(state_db, "s", first_id=3, last_id=5)
    assert [m.id for m in msgs] == [3, 4, 5]


def test_read_slice_range_is_empty_when_every_message_went_inactive(state_db):
    state_db.execute("UPDATE messages SET active = 0 WHERE id BETWEEN 3 AND 5")
    assert hermes_state.read_slice_range(state_db, "s", first_id=3, last_id=5) == []
```

- [ ] **Step 2: Run them to see them fail**

Run: `pytest tests/test_hermes_state.py -k read_slice_range -v`
Expected: FAIL — attribute does not exist.

- [ ] **Step 3: Implement**

```python
def read_slice_range(con, session_id: str, *, first_id: int, last_id: int) -> list[Message]:
    """The messages a claimed row covers, both ends inclusive.

    `read_slice` (above) asks "what is unconsumed" and is unbounded above by
    design. This asks a different question — "what did this row claim" — and
    that difference is the whole of ADR-0003: a failed slice is RETRIED, not
    re-derived, so the range comes from the row rather than from the watermark.

    An empty return is meaningful, not an error. It means every message in the
    range went inactive or was rewritten by compaction since the claim, and the
    caller must retire the row rather than retry it — there is nothing left to
    extract and no number of retries changes that.
    """
    rows = con.execute(
        f"""SELECT id, role, content, tool_calls, tool_name, timestamp, compacted
            FROM messages
            WHERE session_id = ? AND id >= ? AND id <= ? AND COALESCE(active, 1) <> 0
              AND role IN ({','.join('?' * len(TRANSCRIPT_ROLES))})
            ORDER BY id ASC""",
        (session_id, first_id, last_id, *TRANSCRIPT_ROLES),
    ).fetchall()
    return _rows_to_messages(rows)


def session_source(con, session_id: str) -> str:
    """The `source` column a Candidate would have carried.

    A retry slice does not come from find_candidates, so it has no Candidate —
    but the sweeper reads `cand.source` for one thing only, the `platform`
    field on every fabric entry it writes. Empty string when the session row is
    gone; the sweeper's existing `or "cli"` fallback covers it.
    """
    row = con.execute("SELECT source FROM sessions WHERE id = ?", (session_id,)).fetchone()
    return (row["source"] or "") if row else ""
```

- [ ] **Step 4: Run to verify green**

Run: `pytest tests/test_hermes_state.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add icarus/hermes_state.py tests/test_hermes_state.py
git commit -m "feat(icarus): read a claimed row's own range, both ends inclusive"
```

---

### Task 4: The retry pass and the frontier watermark

**This task is atomic and must not be split.** Landing the frontier watermark without the retry pass strands every failed row (nothing re-offers it); landing the retry pass without the frontier lets both passes claim the same messages under two keys `UNIQUE (session_id, last_message_id)` cannot see. Either half alone is a regression.

**Files:**
- Modify: `scripts/session_store.py` (`watermarks`), `scripts/session_sweeper.py` (`sweep`), `tests/test_session_sweeper.py` (four `FakePg` classes)

**Interfaces:**
- Consumes: `failed_slices` (Task 2), `read_slice_range` + `session_source` (Task 3).
- Produces: `stats["retried"]`; slices carry a `retry: bool`.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_hole_under_a_published_slice_is_offered_at_its_own_range():
    """(S,1..10) failed, (S,11..20) published, nothing new. The old code offered
    nothing at all — MAX(last_message_id) WHERE status<>'failed' was 20."""
    pg = FakePg(claimed=[("s", 20)])
    pg.claimed[("s", 10)] = "failed"
    pg.ranges[("s", 10)] = (1, 10)
    deps = build_deps_with(pg, messages=range(1, 21))
    session_sweeper.sweep(deps, cfg())
    assert ("published", {"session_id": "s", "last_message_id": 10}) in keys(pg.marks)


def test_the_retry_pass_and_the_fresh_pass_never_claim_the_same_messages():
    """(S,1..10) failed with a tail grown to 25. The frontier is 10, so the
    fresh pass starts at 11 — it must NOT build (1..25)."""
    pg = FakePg()
    pg.claimed[("s", 10)] = "failed"
    pg.ranges[("s", 10)] = (1, 10)
    deps = build_deps_with(pg, messages=range(1, 26))
    session_sweeper.sweep(deps, cfg(max_per_run=5))
    claimed_ranges = [c["kwargs"] for c in pg.calls if c["fn"] == "claim"]
    assert sorted((c["first_message_id"], c["last_message_id"]) for c in claimed_ranges) \
        == [(1, 10), (11, 25)]
```

```python
def test_two_poison_retry_rows_do_not_starve_an_unrelated_fresh_session():
    """Both breakers end the run with `break`. Interleaving is what guarantees
    the fresh candidate is reached before the second retry can trip one."""
    pg = FakePg()
    for sid, last in (("a", 10), ("b", 10)):
        pg.claimed[(sid, last)] = "failed"
        pg.ranges[(sid, last)] = (1, 10)
    deps = build_deps_with(pg, sessions={"a": range(1, 11), "b": range(1, 11),
                                         "c": range(1, 11)},
                           extract=raises_deterministic_for("a", "b"))
    session_sweeper.sweep(deps, cfg(max_per_run=3, deterministic_sessions_abort=2))
    assert ("published", {"session_id": "c", "last_message_id": 10}) in keys(pg.marks)
```

- [ ] **Step 2: Run them to see them fail**

Run: `pytest tests/test_session_sweeper.py -k "hole_under_a_published or never_claim_the_same or poison_retry_rows" -v`
Expected: FAIL — the first finds no `published` mark for `(s,10)`, the second sees a single `(1, 25)` claim, the third never reaches session `c`.

- [ ] **Step 3: Flip `watermarks()` to the frontier**

```python
def watermarks(conn) -> dict[str, int]:
    """How far each session has been consumed. ALL statuses count, 'failed'
    included, and that inclusion is load-bearing rather than a simplification.

    Excluding 'failed' was how a failed slice got offered again: it dropped out
    of MAX, so the next sweep re-derived a slice from below it. ADR-0003 ended
    that — a failed row is retried at its own range by the retry pass — and
    with the exclusion still in place BOTH passes would target the same
    messages. Concretely: (s,1..10) failed with the conversation grown to 25
    gives the fresh pass `after = 0`, so it builds (s,1..25), a DIFFERENT key
    from (s,10), which means UNIQUE (session_id, last_message_id) cannot catch
    the overlap and the same ten messages are extracted twice.

    The frontier makes the fresh pass start at 11 and leaves 1..10 to the retry
    pass. Nothing is stranded, because the retry pass is now what re-offers it.
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT session_id, MAX(last_message_id) FROM session_extraction
                       GROUP BY session_id""")
        return {row[0]: int(row[1]) for row in cur.fetchall()}
```

Mirror the same rule in all four `FakePg.watermarks` (lines ~48, ~396, ~1015, ~1055) — the fakes derive watermarks in Python and will otherwise keep asserting the old semantics while production does something else.

- [ ] **Step 4: Build the retry slices ahead of the fresh ones**

Inside `sweep()`, before the `find_candidates` block:

```python
    # Retries are capped at max_per_run - 1 so a run ALWAYS has at least one
    # fresh slot. Revision 2 of ADR-0003 let them take the whole budget and run
    # strictly first; review killed it: both circuit breakers end the run with
    # `break` over one list, so two failing retry rows in different sessions
    # abort a sweep before any fresh candidate is reached — every cadence,
    # until an operator notices.
    retry_slices = []
    retry_budget = max(1, cfg["max_per_run"] - 1)
    try:
        owed = deps.pg.failed_slices()
    except Exception as exc:                      # fail-open, same as every other round trip
        logger.warning("failed_slices read failed: %s", exc)
        owed = []
    for row in owed[:retry_budget]:
        if session_id_filter and row["session_id"] != session_id_filter:
            continue
        messages = hermes_state.read_slice_range(
            deps.sqlite_conn, row["session_id"],
            first_id=row["first_message_id"], last_id=row["last_message_id"])
        cand = hermes_state.Candidate(
            row["session_id"], hermes_state.session_source(deps.sqlite_conn, row["session_id"]),
            None, 0.0, len(messages))
        # NO idle_seconds / min_messages GATE, deliberately. Those ask "has this
        # conversation finished growing" — a question already answered when this
        # range was claimed the first time. Applying them again would park a
        # short hole in a still-active session forever.
        retry_slices.append((cand, [], messages, row))
```

Feed both into one loop, INTERLEAVED — retry, fresh, retry, fresh — so a fresh slice has
already been processed by the time a second retry can trip a breaker:

```python
    # itertools.zip_longest, not concatenation. Retry first at each position
    # keeps "a session that owes an answer gives it before it takes on more
    # work" true where it matters — within a session the frontier already makes
    # the two ranges disjoint — while guaranteeing fresh work is reached.
    slices = [x for pair in itertools.zip_longest(
                  [(c, ctx, msgs, row) for c, ctx, msgs, row in retry_slices],
                  [(c, ctx, msgs, None) for c, ctx, msgs in fresh_slices])
              for x in pair if x is not None]
```

and unpack `for cand, context, messages, retry_row in slices:`. Add `import itertools` to the
stdlib block at the top of the module.

- [ ] **Step 5: Replace the staleness check for retry slices**

**This is the trap that silently disables the whole feature.** The existing check is

```python
if fresh_marks.get(cand.session_id, 0) >= first_id:   # stale, skip
```

With the frontier, a hole's `first_id` is 1 and the frontier is 20, so `20 >= 1` is true and **every retry slice would be skipped as stale**. The check is asking the wrong question for a retry. Replace it with a status re-read:

```python
        if retry_row is not None:
            # A retry's freshness question is not "did the watermark move" —
            # it moved past this range long ago, that is what makes it a hole.
            # It is "does this row still owe an answer": another sweeper
            # holding the lock before us may have retried and published it.
            # Same round trip, same fail-open handling, different key.
            # slice_status, NOT a scan of failed_slices(): that list is windowed
            # at 50 rows AND filtered by the backoff clock, so on a busy repair
            # a genuinely-owed row falls outside it and gets logged as
            # "resolved by another sweeper" when nobody resolved anything.
            try:
                still_owed = deps.pg.slice_status(
                    session_id=cand.session_id, last_message_id=last_id) == "failed"
            except Exception as exc:
                logger.warning("failed-row re-read for %s failed: %s", cand.session_id, exc)
                stats["locked_out"] += 1
                continue
            if not still_owed:
                logger.info("slice %s:%s was resolved by another sweeper while we "
                            "waited for the lock", cand.session_id, last_id)
                stats["stale_slices"] += 1
                continue
        else:
            ... existing watermark re-read, unchanged ...
```

- [ ] **Step 6: Count it**

Add `"retried": 0` to the `stats` dict at `scripts/session_sweeper.py:175`, increment it where a retry slice wins its claim, and add the matching `ALTER TABLE sweeper_status ADD COLUMN IF NOT EXISTS retried INTEGER NOT NULL DEFAULT 0` plus its `record_status` binding. A hole can close having produced zero entries (a range that extracts cleanly but yields nothing), so an operator reading entry counts alone would see nothing happen.

- [ ] **Step 7: Run the suites**

Run: `pytest tests/ -x -q`
Expected: PASS, including the two new tests.

- [ ] **Step 8: Commit**

```bash
git add scripts/ tests/
git commit -m "feat(sweeper): retry the failed row at its own range; the watermark becomes the frontier"
```

---

### Task 5: The one terminal exit, and the one that must not be

**Files:**
- Modify: `scripts/session_sweeper.py`
- Test: `tests/test_session_sweeper.py`

**Interfaces:**
- Consumes: `retry_row` from Task 4, `(attempts, retries)` from Task 1.

- [ ] **Step 1: Write the failing tests**

```python
def test_an_emptied_range_is_quarantined_not_retried_forever():
    pg = FakePg()
    pg.claimed[("s", 10)] = "failed"
    pg.ranges[("s", 10)] = (1, 10)
    deps = build_deps_with(pg, messages=[])        # every message went inactive
    session_sweeper.sweep(deps, cfg())
    assert pg.claimed[("s", 10)] == "quarantined"


def test_twenty_transient_failures_never_quarantine():
    """The row backs off; it is never retired. A quarantined row counts toward
    the frontier and the retry pass reads only 'failed' rows, so retiring one
    over an OUTAGE is how a conversation is lost for good (ADR-0003 dec. 3)."""
    pg = FakePg()
    pg.claimed[("s", 10)] = "failed"
    pg.ranges[("s", 10)] = (1, 10)
    pg.retries[("s", 10)] = 40                     # far past any plausible ceiling
    deps = build_deps_with(pg, messages=range(1, 11), extract=raises_transient)
    session_sweeper.sweep(deps, cfg())
    assert pg.claimed[("s", 10)] == "failed"
    assert pg.attempts.get(("s", 10), 0) == 0      # never a deterministic failure
```

- [ ] **Step 2: Run them to see them fail**

Run: `pytest tests/test_session_sweeper.py -k "emptied_range or twenty_transient" -v`
Expected: FAIL — the first leaves the row at `failed`; the second fails only if a ceiling was wrongly introduced, so write it as the regression guard it is and confirm it passes for the right reason (sabotage it by adding a ceiling, watch it go red, remove the ceiling).

- [ ] **Step 3: Quarantine an emptied range**

Immediately after building a retry slice's messages, before it joins `slices`:

```python
        if not messages:
            # Every message in this range went inactive or was rewritten by
            # compaction since the claim. There is nothing left to extract, so
            # retrying is not a slower success — it is a loop. Terminal.
            logger.warning("slice %s:%s covers no readable messages any more — "
                           "quarantining (ADR-0003 decision 1)",
                           row["session_id"], row["last_message_id"])
            deps.pg.mark_quarantined(session_id=row["session_id"],
                                     last_message_id=row["last_message_id"],
                                     error="range no longer readable")
            stats["quarantined"] += 1
            continue
```

- [ ] **Step 4: Surface the backoff instead of retiring on it**

There is NO second ceiling. The three `count_attempt=False` call sites capture the tuple and log,
so a row backing off toward daily is visible rather than silent:

```python
            _, retries = deps.pg.mark_failed(session_id=cand.session_id,
                                             last_message_id=last_id,
                                             error=exc, count_attempt=False)
            if retries >= 5:
                # ~4h and doubling by this point. Deliberately a log line and
                # not a quarantine: retiring the row would put its messages out
                # of reach of both passes, and nothing here is evidence about
                # the conversation — only about the infrastructure.
                logger.warning("slice %s:%s has failed %d times and is backing off; "
                               "it will not retire on its own (ADR-0003 decision 3)",
                               cand.session_id, last_id, retries)
```

The cooldown the cross-session breaker needs comes for free: `rollback_attempt` refunds
`attempts` only, never `retries` and never `next_retry_at`, so a rolled-back cohort is still
deferred and cannot trip the breaker again on the next cadence. Say so in a comment there.

- [ ] **Step 5: No new config key**

`_load_cfg` is unchanged. The backoff base and cap are SQL literals in `session_store`, next to
their constants — there is no operator knob, because a shorter cap is how the starvation of
Finding B comes back.

- [ ] **Step 6: Run the suite**

Run: `pytest tests/ -x -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scripts/session_sweeper.py tests/test_session_sweeper.py
git commit -m "feat(sweeper): an emptied range retires; an outage never does"
```

---

### Task 6: The lock moves into the CLI

**Files:**
- Modify: `scripts/session_sweeper.py` (`main`, argparse)
- Test: `tests/test_session_sweeper.py`

- [ ] **Step 1: Write the failing test**

```python
def test_a_second_sweeper_exits_zero_without_sweeping(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMOS_SWEEPER_LOCK", str(tmp_path / "sweeper.lock"))
    with session_sweeper.run_lock() as first:
        assert first is True
        with session_sweeper.run_lock() as second:
            assert second is False
```

- [ ] **Step 2: Run it to see it fail**

Run: `pytest tests/test_session_sweeper.py -k second_sweeper -v`
Expected: FAIL — `run_lock` does not exist.

- [ ] **Step 3: Implement**

```python
@contextlib.contextmanager
def run_lock(path=None):
    """Exclusive, host-local, for EVERY invocation — cron and manual alike.

    ADR-0002 claimed a `flock -n` in the cron line as the cheap first line of
    defence. It does not exist: measured 2026-08-22, the installed wrapper
    /opt/data/scripts/memoryos-session-sweeper.sh is four lines and `grep -c
    flock` on it returns 0. So two sweeps genuinely could overlap — a sweep
    slower than the 15-minute cadence is enough — which is how #14's hole forms
    with no operator error at all.

    It lives HERE and not in the wrapper because a wrapper cannot cover a
    manual `--session` run, and a manual run racing the cron is the documented
    shape of that bug. A file lock is sufficient because state.db is host-local:
    two sweepers on different hosts is not a topology this component has.

    Whether `hermes cron` serialises its own executions was never determined.
    With this unconditional, it does not have to be.
    """
    path = path or os.environ.get("MEMOS_SWEEPER_LOCK", str(_REPO / ".sweeper.lock"))
    fh = open(path, "w")
    try:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        fh.close()
```

`main()` wraps its body:

```python
    if args.no_lock:
        logger.warning("--no-lock: running without the exclusive sweeper lock")
    with run_lock() if not args.no_lock else contextlib.nullcontext(True) as acquired:
        if not acquired:
            logger.info("another sweeper holds the lock — nothing to do")
            return 0
        ...
```

Exit `0`, not non-zero: a skipped tick is the lock working, and `--no-agent` cron delivers stdout verbatim, so a non-zero exit would page an operator every time a sweep runs long.

- [ ] **Step 4: Run to verify green**

Run: `pytest tests/ -x -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/session_sweeper.py tests/test_session_sweeper.py
git commit -m "feat(sweeper): the exclusive lock the cron line never had"
```

---

### Task 7: Prove it against a real PostgreSQL

**Files:**
- Modify: `tests/test_integration_postgres.py`

**Why the fakes are not enough:** `FakePg` derives watermarks in Python and would agree with whatever the code does. Every claim in ADR-0003 that concerns SQL semantics — the `DO UPDATE` arm firing on a `failed` row, `attempts` surviving it, the frontier — has to be proven against the real thing.

- [ ] **Step 1: Write the six cases**

The fixture in `tests/test_integration_postgres.py` is named **`conn`**. Each test drives
`session_store` directly against the real server — the sweeper's own logic is covered by the
fakes in Task 4; what needs a real PostgreSQL here is the SQL semantics.

```python
def test_a_hole_closes_at_its_own_range(conn):
    """(s,1..10) failed under (s,11..20) published. The RECLAIM arm must fire on
    the failed row's own key — the fresh path can never reach it."""
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
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    session_store.mark_failed(conn, session_id="s", last_message_id=10, error="x")
    assert session_store.watermarks(conn) == {"s": 10}


def test_attempts_accumulates_across_retries_on_the_same_row(conn):
    """_CLAIM_RECLAIM_SQL's DO UPDATE sets status and timestamps only, so the
    counter survives the reclaim. This is what #15 was missing: the key stops
    moving, so there is a counter to accumulate on."""
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    for expected in (1, 2, 3):
        attempts, _ = session_store.mark_failed(conn, session_id="s", last_message_id=10,
                                                error="unparseable", count_attempt=True)
        assert attempts == expected
        session_store.claim(conn, session_id="s", first_message_id=1,
                            last_message_id=10, message_count=10)


def test_three_deterministic_failures_quarantine_and_the_frontier_is_unmoved(conn):
    """ADR-0003's own named risk: with the frontier, a failed row consumes the
    watermark, so if the retry path ever regresses these messages are
    unreachable from BOTH passes. Write this one first."""
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
    session_store.claim(conn, session_id="s", first_message_id=1,
                        last_message_id=10, message_count=10)
    _age_claim(conn, "s", 10, hours=3)
    assert session_store.expire_stale_claims(conn) == 1
    assert session_store.slice_status(conn, "s", 10) == "failed"
    assert session_store.failed_slices(conn) == []      # scheduled, not due
```

`_make_due` and `_age_claim` are two-line helpers that set `next_retry_at` / `updated_at`
directly; wall-clock ageing is what the real UPDATE keys off, and a test says so directly rather
than sleeping.

- [ ] **Step 2: Run against Docker PostgreSQL 17**

Run: `pytest tests/test_integration_postgres.py -v`
Expected: PASS, twice in sequence (the suite is order-dependent if schemas leak).

- [ ] **Step 3: Sabotage-prove three of them**

Revert `watermarks()` to `WHERE status <> 'failed'` and confirm `test_the_frontier_includes_failed_so_the_fresh_path_starts_above_it` fails. Drop the backoff clause from `_EXPIRE_STALE_SQL` and confirm `test_a_stale_claim_is_rescheduled_not_retried_immediately` fails. Add a 20-retry quarantine back into `mark_failed` and confirm `test_forty_transient_failures_never_quarantine_and_stay_recoverable` fails. Restore after each, re-run green. A test that cannot fail is not a test.

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration_postgres.py
git commit -m "test(integration): the hole, the frontier, and the outage that must not retire a row"
```

---

### Task 8: Wire the new knobs into the deployment repo

**Files:**
- Modify: `config/services.yaml` (fork), and in `semitora-agent-prerequisites`: `inventory/group_vars/all.yml`, `roles/memory_os/templates/producer.env.j2`, `tests/quoting-roundtrip.yml`
- Modify: `docs/carried-forward.md` §18, `manifests/README.md` if a claim changes

- [ ] **Step 1: Confirm there is no new knob**

Decision 3 deliberately exposes no config: the backoff base and cap are SQL literals, because a
shortened cap is how the starvation this design was rewritten to prevent comes back. So
`config/services.yaml`, `group_vars` and `producer.env.j2` need **no new key**, and
`tests/quoting-roundtrip.yml` needs no new fixture var. Verify by grepping the branch for
`SESSION_` additions and finding none; if the implementation added one, that is the bug.

- [ ] **Step 2: Run the deployment repo's gates unchanged**

Run: `make test && make lint` in `semitora-agent-prerequisites`
Expected: PASS with no fixture edit. (`make test` exists because a missing fixture var has been
committed red once already.)

- [ ] **Step 5: Re-vendor and commit**

Re-vendor the fork into `roles/memory_os/files/memory-os/`, carrying the single `memos_config/__init__.py` local patch forward, then commit in both repos.
