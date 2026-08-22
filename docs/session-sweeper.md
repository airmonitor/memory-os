# Session Sweeper

The session sweeper runs on a schedule and extracts finished conversation slices from the Hermes agent's SQLite state database. For each candidate slice, it reads the messages, calls the LLM to extract fabric entries (key insights, decisions, context), writes entries to disk, and enqueues them for vector ingestion into Qdrant. Claim-before-extract and deterministic filenames mean a crash at any point leaves the slice reclaimable and never multiplies fabric entries. It pays for a second LLM call only if it crashes *before* the extraction is banked (`mark_extracted`); once the payload is on the row, every retry drains it for free. (The one duplicate a crash can still produce is a Qdrant point: arq's `_job_id` dedup lasts only as long as `keep_result`, 3600 s — see ADR-0001 §3.4.)

See [ADR-0001](./adr/0001-session-extraction-via-state-db-sweeper.md) for the design, the ordering argument (claim → extract → publish → dispatch), and the three side effects that cannot share one transaction.

## Installation

Install as a cron job on the agent host (this is the same line as `setup/install.md` §10 — keep the two in step):

```cron
*/15 * * * * /usr/bin/flock -n /tmp/memoryos-sweeper.lock $VENV_DIR/bin/python $PROJECT_DIR/scripts/session_sweeper.py >> $HERMES_LOG_DIR/session_sweeper.cron.log 2>&1
```

Substitute `$VENV_DIR` (typically `/opt/agent/venv`), `$PROJECT_DIR` (typically `/opt/agent/memory-os`) and `$HERMES_LOG_DIR` with the actual paths — cron does not read your shell profile. Use the venv interpreter, not the system one: the sweeper imports `arq` and `psycopg`.

`flock -n` is not decoration. An extraction is an LLM call, and a sweep that outlives its 15-minute slot must become a no-op rather than let a second sweeper start claiming beside it.

Before wiring it up, see what a run would do without writing anything:

```bash
$VENV_DIR/bin/python $PROJECT_DIR/scripts/session_sweeper.py --dry-run --verbose
```

## Deployment Order

**The worker image must be updated before, or in the same change, as the sweeper — never after.** Since ADR-0002 decision 2, the sweeper calls `deps.enqueue("process_ingestion", ..., point_id=point_id(job_id))`, passing a keyword argument that only exists on the current `docker/worker/main.py::process_ingestion(ctx, memory_text, source, tags=None, point_id=None)`. An OLDER worker image — deployed before that parameter was added — has a `process_ingestion` with no `point_id` parameter at all, so ARQ calling that job raises `TypeError: process_ingestion() got an unexpected keyword argument 'point_id'` inside the worker on every attempt (ARQ's own retry policy does not help — the signature mismatch is identical on every retry). The sweeper never learns about it either way: `mark_published` is called right after enqueuing, in the same code path regardless of whether the job later succeeds in the worker. The fabric entry is on disk, `session_extraction.status` is `'published'`, and nothing in `sweeper_status` or `session_extraction.error` ever records that the worker rejected the job — the memory is lost silently, not retried by anything this repo's own bookkeeping would notice.

Rolling the worker out first (or atomically with the sweeper) is what avoids this: the current worker's `point_id` parameter defaults to `None` and falls through to `ingest_memory`'s existing `uuid4()` fallback, so it accepts jobs from an OLDER sweeper too. The upgrade is one-directional-safe only in the worker-first order.

## Monitoring

The sweeper records every run (success or failure) in the PostgreSQL `sweeper_status` table. To inspect the five most recent runs:

```sql
SELECT ran_at, candidates, extracted, entries, jobs, redispatched, schema_version, error
  FROM sweeper_status ORDER BY ran_at DESC LIMIT 5;
```

Columns:
- `ran_at`: Run timestamp
- `candidates`: Quiet sessions offered for extraction on this run, after the `--session` filter and the `max_per_run` cut
- `extracted`: Slices that cleared the quality threshold and had the LLM called for them. This counts the **call**, not its yield — a slice where the model honestly returned no entries still counts here
- `entries`: Fabric entries written to disk by this run's fresh extractions (re-dispatched slices are counted by `redispatched`, not here)
- `jobs`: ARQ jobs enqueued by this run's fresh extractions
- `redispatched`: Slices left `extracted` by an earlier run and finished on this one. A number that never falls to zero means the broker is not accepting jobs (or a fabric write keeps failing) — the backlog is stalled
- `schema_version`: Hermes `state.db` schema version observed on this run. It is compared against `icarus.hermes_state.KNOWN_SCHEMA_VERSION`, and a mismatch logs one `SCHEMA-DRIFT` warning per run carrying both numbers. Drift never stops a sweep
- `error`: NULL on success; the error if the **whole run** crashed. A single slice failing does not land here — it lands in `session_extraction.error`, below

Per-slice failures are the more common case, and they are deliberately loud:

```sql
SELECT session_id, last_message_id, status, error, updated_at
  FROM session_extraction WHERE status = 'failed' ORDER BY updated_at DESC LIMIT 10;
```

A failed slice is excluded from the watermark, so it is offered again on the next sweep. `error = 'stale claim expired'` means a previous run died between claiming the slice and extracting it; `expire_stale_claims()` rewrote the abandoned row at the top of a later sweep so the slice could come back.

An extraction that could not reach the model — a timing-out proxy, a missing API key, output that is not JSON — raises rather than returning "no entries", precisely so it shows up here instead of quietly advancing the watermark past an unread conversation.

## Attempts and Quarantine

`session_extraction.attempts` (ADR-0002 decision 4) counts only **classified-deterministic** failures — the gateway answered like a gateway, but the *model's* output did not parse or validate. A transient failure (connection error, timeout, an HTTP status error, a missing key, or a body that is not a chat completion) never moves it: an outage spanning several sweeps must not spend a slice's three tries just because the gateway was down. Reclaiming a stale `'claimed'` row (a crash between claim and extraction) does not touch it either, for the same reason.

```sql
SELECT session_id, last_message_id, attempts, status, error, updated_at
  FROM session_extraction WHERE attempts > 0 ORDER BY updated_at DESC LIMIT 10;
```

At `SESSION_MAX_ATTEMPTS` (default 3) a slice that keeps failing deterministically becomes `quarantined` instead of sitting at `'failed'` forever, burning an LLM call every 15 minutes for a conversation that will never parse. `quarantined` still counts toward the watermark — `watermarks()` excludes only `'failed'` — so it does not block that session's later messages behind a slice that is never coming back on its own. Nothing alerts on this yet, so it has to be watched for:

```sql
SELECT ran_at, quarantined FROM sweeper_status WHERE quarantined > 0 ORDER BY ran_at DESC LIMIT 5;
```

**The one-statement replay** that un-retires a quarantined slice and gives it a fresh set of attempts (for example, after a prompt or model fix that would now handle it):

```sql
UPDATE session_extraction SET status = 'failed', attempts = 0
  WHERE session_id = 'example-session-id' AND last_message_id = 12345;
```

No code change and no `ensure_schema` involved — `claim()`'s existing `'failed'`-reclaim arm picks the row up on the very next sweep.

## Re-extracting a Session

To re-extract a specific session (for example, if a model update improved extraction quality):

```sql
DELETE FROM session_extraction WHERE session_id = 'example-session-id';
```

On the next sweep, the session will be offered again if it is still a candidate (quiet for `idle_minutes`, or ended). Fabric filenames are deterministic per `(session_id, last_message_id, index)`, so the re-run overwrites the old entries rather than adding a second copy of each.

### Reconciling Qdrant Points After a Re-extraction

Since ADR-0002 decision 2, the sweeper's Qdrant point id is `uuid5(NAMESPACE_URL, job_id)` (`scripts.session_sweeper.point_id`), where `job_id` is `ingest:{session_id}:{last_message_id}:{index}` (`scripts.session_sweeper.job_id`) — the same string every time for the same slice and entry index. **If a re-extraction reproduces the same `(session_id, last_message_id)` slice with the same number of entries, nothing needs reconciling**: every entry gets the same `job_id`, therefore the same point id, and the worker's `qdrant.upsert` overwrites the old point's payload in place.

Reconciliation is only a real step in two cases, and the honest limit has to be stated: **the Qdrant payload the worker writes (`docker/worker/tasks/ingestion.py::ingest_memory`) carries no `session_id` field at all** — `payload["source"]` is the literal constant `"session"` for every point the sweeper produces, not the originating session. So there is no payload filter that selects "every point from session X"; the only handle is the point's own id, and only when you still know the `job_id`s that produced it.

- **Same session, fewer entries than before** (the re-extraction is genuinely smaller). The dropped entries' old `job_id`s do not recur, so their points are not overwritten and are left orphaned. Before deleting the `session_extraction` row(s) to trigger the re-extraction, read the old `jobs` column (it holds exactly the `job_id` list `mark_published` recorded) and compute the now-orphaned ids yourself:

  ```python
  from scripts.session_sweeper import point_id
  orphaned_ids = [point_id(j) for j in old_job_ids_no_longer_produced]
  await qdrant.delete(collection_name="knowledge_base",
                       points_selector=PointIdsList(points=orphaned_ids))
  ```

- **Legacy points ingested before ADR-0002 decision 2 shipped**, which carry a random `uuid4` id with no relationship to any `job_id`. These cannot be found or deleted by id or by payload filter — there is nothing in the point to compute back from. As of this ADR the production corpus has `points_count: 0`, so there is nothing to reconcile yet; if that changes before a session is re-extracted, the only options are locating the old point by manually reviewing `payload["text"]` against the fabric files it was written from, or accepting the duplicate.

## Suppressing a Session

To exclude a session from extraction (for example, if it is too short or of low quality):

```sql
UPDATE session_extraction SET status = 'published' 
  WHERE session_id = 'example-session-id';
```

This works because `watermarks()` counts every status **except** `failed`. Marking the row `published` is therefore what puts the session *into* the watermark: `find_candidates` then sees no messages past that watermark and stops offering the slice. (A row left at `failed` does the opposite — it drops out of the watermark and the slice comes back.) To suppress a session that has no row yet, insert one with the session's current `max(messages.id)` as `last_message_id`.

## Integration Tests

`tests/test_session_store.py` and `tests/test_session_lock.py` check the SQL `scripts/session_store.py` *builds*, against a `FakeConn`/`FakeCursor` pair that only proves the code sent the statement it intended to send — it agrees with whatever the code does, so it cannot check that PostgreSQL accepts `ON CONFLICT ... DO UPDATE ... WHERE`, evaluates `make_interval`/`GREATEST` the way the docstrings claim, round-trips the JSONB payload, upgrades an existing table with `ADD COLUMN IF NOT EXISTS`, or that `pg_try_advisory_xact_lock` is really released by the transaction rather than the connection. `tests/test_integration_postgres.py` runs those same code paths against a real server instead.

It is skipped by default — collected but immediately skipped unless `MEMOS_TEST_DSN` is set — so a plain `.venv/bin/pytest` run stays offline with no host and no credential. To run it:

```bash
docker run -d --rm --name memos-test-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
MEMOS_TEST_DSN=postgresql://postgres:test@localhost:55432/postgres .venv/bin/pytest tests/test_integration_postgres.py -v
docker rm -f memos-test-pg
```

Or select just this file by marker: `.venv/bin/pytest -m integration` (still needs `MEMOS_TEST_DSN`, or every test in it skips). Each test claims a randomly-suffixed `session_id`, and the `conn` fixture drops both `session_extraction` and `sweeper_status` on teardown — that makes the file safe to run again, in sequence, against the same database (measured: three runs in a row, same container, all passed). It does **not** make the file safe to run concurrently, or to point at any database something else is using: two overlapping runs' teardowns race on dropping each other's tables, and pointing this at a shared database — a colleague's, or a real deployment's — drops that database's own `session_extraction`/`sweeper_status` bookkeeping the moment the first test in this file finishes. Use a throwaway database or container, one run at a time.

## Acceptance Criteria

The sweeper exists to move two counters, and one that it does not touch:

1. **Fabric entries on disk**: `entries` in `sweeper_status` grows as slices are extracted, and files appear under `FABRIC_DIR`
2. **Qdrant points**: `points_count` in the Qdrant UI for the `knowledge_base` collection increases as the worker drains the jobs the sweeper enqueued
3. **Lineage rows**: the PostgreSQL `lineage` table does **not** grow when the sweeper ingests. `register_lineage` is called on the *retrieval* path (`icarus/hooks.py:_search_qdrant`), so `lineage` grows when the agent recalls a memory, not when this job stores one. Watch it after the agent searches, not after a sweep — an operator waiting for lineage rows behind an ingest will call a working pipeline broken

If (1) and (2) rise after conversations, ingestion is working; if (3) then rises after the agent recalls something, retrieval is working too.
