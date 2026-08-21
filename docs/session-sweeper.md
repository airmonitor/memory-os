# Session Sweeper

The session sweeper runs on a schedule and extracts finished conversation slices from the Hermes agent's SQLite state database. For each candidate slice, it reads the messages, calls the LLM to extract fabric entries (key insights, decisions, context), writes entries to disk, and enqueues them for vector ingestion into Qdrant. The process is crash-safe: claim-before-extract and deterministic filenames mean a crash at any point leaves the slice reclaimable and non-duplicating.

See [ADR-0001](./adr/0001-session-extraction-via-state-db-sweeper.md) for the design, the ordering argument (claim → extract → publish → dispatch), and the three side effects that cannot share one transaction.

## Installation

Install as a cron job on the agent host:

```bash
*/15 * * * * $VENV/bin/python $REPO/scripts/session_sweeper.py
```

Substitute `$VENV` (typically `/opt/agent/venv`) and `$REPO` (typically `/opt/agent/memory-os`) with the actual paths.

## Monitoring

The sweeper records every run (success or failure) in the PostgreSQL `sweeper_status` table. To inspect the five most recent runs:

```sql
SELECT ran_at, candidates, extracted, entries, jobs, schema_version, error
  FROM sweeper_status ORDER BY ran_at DESC LIMIT 5;
```

Columns:
- `ran_at`: Run timestamp
- `candidates`: Number of quiet sessions offered for extraction
- `extracted`: Number of sessions that passed quality threshold and produced entries
- `entries`: Total fabric entries written to disk
- `jobs`: Total ARQ jobs enqueued for Qdrant ingestion
- `schema_version`: Hermes schema version at time of run (consistency check)
- `error`: NULL on success; error message if the run crashed

## Re-extracting a Session

To re-extract a specific session (for example, if a model update improved extraction quality):

```sql
DELETE FROM session_extraction WHERE session_id = 'example-session-id';
```

On the next sweep, the session will be offered again if it is still a candidate.

## Suppressing a Session

To exclude a session from extraction (for example, if it is too short or of low quality):

```sql
UPDATE session_extraction SET status = 'published' 
  WHERE session_id = 'example-session-id';
```

Suppressed sessions no longer appear in the watermark, so they will not be processed even if new messages arrive.

## Acceptance Criteria

The sweeper exists to move three counters:

1. **Fabric entries on disk**: `entries` in `sweeper_status` grows as slices are extracted
2. **Qdrant points**: `points_count` in the Qdrant UI for the `knowledge_base` collection increases
3. **Lineage rows**: The PostgreSQL `lineage` table grows as entries are indexed

If all three are rising over time, the pipeline is working.
