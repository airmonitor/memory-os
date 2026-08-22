# Session Sweeper

The session sweeper runs on a schedule and extracts finished conversation slices from the Hermes agent's SQLite state database. For each candidate slice, it reads the messages, calls the LLM to extract fabric entries (key insights, decisions, context), writes entries to disk, and enqueues them for vector ingestion into Qdrant. Claim-before-extract and deterministic filenames mean a crash at any point leaves the slice reclaimable, never pays for a second LLM call, and never multiplies fabric entries. (The one duplicate a crash can still produce is a Qdrant point: arq's `_job_id` dedup lasts only as long as `keep_result`, 3600 s — see ADR-0001 §3.4.)

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

## Re-extracting a Session

To re-extract a specific session (for example, if a model update improved extraction quality):

```sql
DELETE FROM session_extraction WHERE session_id = 'example-session-id';
```

On the next sweep, the session will be offered again if it is still a candidate (quiet for `idle_minutes`, or ended). Fabric filenames are deterministic per `(session_id, last_message_id, index)`, so the re-run overwrites the old entries rather than adding a second copy of each.

## Suppressing a Session

To exclude a session from extraction (for example, if it is too short or of low quality):

```sql
UPDATE session_extraction SET status = 'published' 
  WHERE session_id = 'example-session-id';
```

This works because `watermarks()` counts every status **except** `failed`. Marking the row `published` is therefore what puts the session *into* the watermark: `find_candidates` then sees no messages past that watermark and stops offering the slice. (A row left at `failed` does the opposite — it drops out of the watermark and the slice comes back.) To suppress a session that has no row yet, insert one with the session's current `max(messages.id)` as `last_message_id`.

## Acceptance Criteria

The sweeper exists to move two counters, and one that it does not touch:

1. **Fabric entries on disk**: `entries` in `sweeper_status` grows as slices are extracted, and files appear under `FABRIC_DIR`
2. **Qdrant points**: `points_count` in the Qdrant UI for the `knowledge_base` collection increases as the worker drains the jobs the sweeper enqueued
3. **Lineage rows**: the PostgreSQL `lineage` table does **not** grow when the sweeper ingests. `register_lineage` is called on the *retrieval* path (`icarus/hooks.py:_search_qdrant`), so `lineage` grows when the agent recalls a memory, not when this job stores one. Watch it after the agent searches, not after a sweep — an operator waiting for lineage rows behind an ingest will call a working pipeline broken

If (1) and (2) rise after conversations, ingestion is working; if (3) then rises after the agent recalls something, retrieval is working too.
