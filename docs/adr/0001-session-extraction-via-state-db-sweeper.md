# ADR-0001: Extract sessions by sweeping Hermes' `state.db`, not from the plugin hook

**Status**: Proposed (revision 2 — after adversarial review, see "Review history")
**Date**: 2026-08-22
**Deciders**: operator (semitora), icarus/memory-os maintainers

## Context

Layer 4 of MemoryOS — turning a finished conversation into a durable fabric entry —
writes nothing on modern Hermes. Measured on `nousresearch/hermes-agent:v2026.8.18`
(`hermes_agent 0.20.4`) after four substantive turns with a real agent:
`fabric 0 / qdrant points 0 / lineage 0`, with no error anywhere.

Four measurements define the problem. All were taken in the running pod, not reasoned about.

1. **The plugin hook `on_session_end` fires once per user message.** Upstream says so in its
   own comment at `agent/turn_finalizer.py:812` — *"Fired at the very end of every
   run_conversation call"* — and `run_conversation()` runs once per message. The
   memory-provider method of the same name (`agent/memory_provider.py:251`) is the real
   per-session one: *"NOT called after every turn — only at actual session boundaries."*
   The icarus fork was written against Hermes 0.14–0.15, where the plugin hook meant what
   its name says.

2. **`state.exchanges` is module-level, in-process state, and it is a truncated copy.**
   `post_llm_call` appends `user[:200]` and `assistant[:500]`. Under `hermes chat -q` (one
   process per message) the hook always sees exactly one exchange, `score_session()` returns
   0.073 against its own 0.2 threshold, and it returns before writing. Under `gateway run`
   — which is how this agent actually runs — the process persists, so the list accumulates
   **across concurrent Slack threads and mixes them into one "session."** Both readings are
   defects; they are different defects.

3. **`write_entry()` enqueues nothing.** It writes a markdown file to `FABRIC_DIR` and
   returns. The only ARQ producers in the tree are `wiki_continuous_ingest.py` (not deployed
   where no corpus is mounted) and `reflection_trigger.py`. So even a perfectly repaired hook
   would leave `points_count` and `lineage` at zero: fabric would grow and the vector store
   would not.

4. **Hermes already persists everything the extraction needs, and icarus already reads it.**
   `state.db` carries `sessions(id, source, chat_type, thread_id, message_count, started_at,
   ended_at, end_reason, last_activity_at, …)` and `messages(session_id, role, content,
   timestamp, tool_calls, tool_name, active, compacted, …)`, plus a `schema_version` table
   (**26** on this image). `icarus/hooks.py:_search_sessions()` has been opening that file
   `mode=ro` for months, and the probes behind this ADR read it from a *separate process in
   the same pod* — which is exactly the position a scheduled job occupies.

Live-data facts that shape the design (probed 2026-08-21/22, 19 sessions / 115 messages):

- **A Slack session never ends.** 12 sessions carry `ended_at`, all `end_reason='cli_close'`;
  the six `source='slack'` rows have `ended_at IS NULL` and `expiry_finalized=0`. Slack rows
  are per **thread**. One thread ran 15:51→21:30 with 35 messages. There is no end-of-session
  event on the channel the client actually uses.
- **Quiet periods do not cleanly equal conversation ends.** Gaps between consecutive messages
  inside two real threads, in minutes: `0×11, 1, 1, 1, 2, 42, 292` and `0×15, 1, 1, 1326`.
  Inside a working burst ≤ 2 min. **42 minutes occurred *within* a thread that continued.**
- **Roles**: `assistant` 49, `tool` 34, `user` 27, `session_meta` 5. Of 28 empty-`content`
  rows, 23 are assistant rows carrying `tool_calls` (the announcement) — the tool *results*
  are separate `role='tool'` rows **with** content. So tool activity is recoverable.
- **`active=0`: 0 rows. `compacted=1`: 0 rows.** Compaction has never run on this database, so
  its behaviour is unobserved rather than known-safe.
- **`journal_mode=wal`**, and `state.db-wal` / `state.db-shm` are readable by the pod uid.

## Decision Drivers

- **No changes to Hermes Agent.** We do not control its release cadence. Everything must live
  in this repository.
- **Extraction must survive both run modes** — `gateway run` (one process, many threads) and
  `chat -q` (one process per message).
- **The acceptance gate is three counters** — a fabric entry, `points_count > 0`, `lineage`
  rows > 0 — not "the pod is green."
- **Fail-open on the turn path, fail-loud on the extraction path.** A memory layer must never
  block a turn; it also must never be quietly dead, which is this component's characteristic
  failure.
- **Bounded, idempotent, and observable.** Extraction is an LLM call: never twice for the same
  input, never silently skipped.

## Considered Options

### Option 1: Persist `state.exchanges` across processes

Key accumulated exchanges by `session_id` in a durable store; keep extracting from the hook.

- **Pros**: keeps the current control flow; smallest conceptual change.
- **Cons**: still fires per turn, so it needs its own "is the session over?" rule anyway;
  duplicates a transcript Hermes already stores, and duplicates it *worse* (200/500-char
  truncation, no tool rows); does not fix cross-thread mixing without extra keying.

### Option 2: Register icarus as a Hermes memory provider

Move extraction to `MemoryProvider.on_session_end(messages)`.

- **Pros**: semantically the right hook; Hermes decides when a session ends.
- **Cons**: depends on a provider registration path this fork does not use
  (`hermes memory status` → `Provider: (none — built-in only)`); still yields **nothing on
  Slack**, where sessions have no end; and it re-couples us to a Hermes interface after the
  first one moved under us.

### Option 3: Sweep `state.db` from our own scheduled job

- **Pros**: independent of plugin-hook semantics; identical behaviour in both run modes and on
  Slack; reads the authoritative transcript including tool rows the hook never saw; naturally
  incremental; a sweep that finds nothing costs zero LLM calls.
- **Cons**: couples us to Hermes' *storage* schema instead of its plugin API; "conversation
  boundary" becomes a policy rather than an event; needs claim/watermark bookkeeping.

### Option 4: Lower the 0.2 quality threshold

- **Pros**: one line.
- **Cons**: makes every turn write an entry, which is what the score exists to prevent.
  Rejected; the threshold is not the bug.

## Decision

Adopt **Option 3**, with the protocol below, and close the enqueue gap in the same change.

### 1. What an entry represents

An extracted entry is a **conversation slice**, not a session. A Slack thread is long-lived;
the measured 42-minute in-thread gap proves an idle timer cannot mean "the conversation
ended." Slices are therefore explicit, and continuation slices carry context:

- default `SESSION_IDLE_MINUTES = 90` — above the largest gap observed *inside* a conversation
  (42) and far below the smallest gap observed *between* them (292);
- a continuation slice prepends `SESSION_CONTEXT_OVERLAP = 4` trailing messages of the previous
  slice to the transcript, marked as context and excluded from scoring;
- a session with `ended_at IS NOT NULL` is finalized immediately, without waiting out the idle
  timer;
- `SESSION_MAX_LAG_HOURS = 24` forces extraction of a slice that never reaches
  `SESSION_MIN_MESSAGES`, so a short tail cannot sit unprocessed forever.

### 2. Consistent read protocol

One SQLite connection, opened `file:{path}?mode=ro` with `busy_timeout=5000`, and **both**
queries — candidate selection and message read — inside one `BEGIN DEFERRED` transaction, so
WAL gives them a single snapshot. The slice's upper bound is the `max(messages.id)` observed
*inside* that transaction and is stored with the watermark; anything Hermes appends later
belongs to the next slice by construction. Normal SQLite reader locking is retained; nothing
disables it, and nothing ever writes to `state.db`.

### 3. Idempotency: claim, publish, dispatch

State machine in PostgreSQL, table `session_extraction`, `UNIQUE (session_id, last_message_id)`:

1. **Claim** — `INSERT … ON CONFLICT DO NOTHING RETURNING id` for
   `(session_id, last_message_id)` with `status='claimed'`. Losing the race means another
   sweeper owns the slice; skip it. Candidate selection additionally uses
   `FOR UPDATE SKIP LOCKED` on the session's latest row so two sweepers never extract the
   same session concurrently.
2. **Extract** — the LLM call happens only after the claim exists, so it can never run twice
   for the same slice.
3. **Publish** — fabric filenames are deterministic per `(session_id, last_message_id, index)`,
   written to a temporary file and `os.replace()`d into place, so a retry overwrites atomically
   rather than multiplying entries.
4. **Dispatch** — ARQ `enqueue_job(..., _job_id=f"ingest:{session_id}:{last_message_id}:{i}")`.
   arq refuses a duplicate job id **only while that job's result key still exists**, and
   `keep_result` is 3600 s in `config/services.yaml`. So re-dispatch is a no-op for one hour
   after the first delivery, not forever; a replay later than that enqueues again, and because
   `ingest_memory` assigns a fresh `uuid.uuid4()` per point, the second run writes a duplicate
   Qdrant point rather than overwriting the first. What is guaranteed unconditionally: no
   second LLM call, and no second fabric file (deterministic filename, `os.replace`). Making
   the point id deterministic is the real fix; it belongs in the worker image
   (`docker/worker/tasks/ingestion.py`) and is tracked separately.
   The outbox row is marked `published` only after the enqueue returns; a crash in between
   leaves `status='extracted'` **with the entry text and its fabric fields stored on the claim
   row**, and every sweep drains those before doing new work — rewriting the fabric files and
   re-dispatching under the same job ids, paying no second LLM call. The payload carries both
   halves deliberately: when it held only the job text, a failed fabric write ended with the
   memory in Qdrant and no entry on disk.
5. **Reclaim** — a row stuck at `claimed` for more than `STALE_CLAIM_HOURS` (a crash between
   claim and extraction) is re-claimable, as is a `failed` row. Without that rule the unique
   key turns one crash into one permanently lost conversation, because `watermarks()` counts
   the abandoned row. The reclaim cannot live in `claim()`'s `ON CONFLICT` alone: the stuck
   row is *why* the slice never becomes a candidate, so `claim()` is never called with that
   key and the conflict never happens. `expire_stale_claims()` therefore runs as the first
   statement of every sweep, before `watermarks()` is read, rewriting stale `claimed` rows to
   `failed`; the existing failed-row machinery then re-offers the slice unchanged.

### 4. Transcript fidelity

`role IN ('user','assistant','tool')`, ordered by `id`, `active <> 0`:

- assistant rows with empty `content` and non-empty `tool_calls` render as a one-line
  `[tool: name(args…)]` marker rather than being dropped;
- `role='tool'` rows contribute their result content, truncated;
- `role='session_meta'` is skipped;
- `compacted=1` rows are included as ordinary content. Compaction is **unobserved** on this
  installation (0 rows); the sweeper logs a warning the first time it meets one, so the gap
  is discovered by a log line rather than by a missing memory.

This is strictly richer than the hook's 200/500-character copy, which is the only thing being
replaced. The hook's extraction is disabled; its creative-memory write stays.

### 5. Compatibility and observability

- The reader records `schema_version` (26 today) and compares it against
  `hermes_state.KNOWN_SCHEMA_VERSION`. An unknown version is not fatal but is logged as
  `SCHEMA-DRIFT` once per run, with both numbers, and recorded in the status row, because
  refusing to sweep on an upgrade would be a silent memory outage of a different shape.
  Recording the number without comparing it would not have been the promise: nothing would
  have raised its voice.
- Every run writes a `sweeper_status` row: timestamp, candidates seen, slices extracted,
  entries written, jobs enqueued, slices re-dispatched, and the last error. `redispatched` is
  there because a backlog that never drains is a stall of its own shape: those slices are
  re-offered every sweep, so a count that never falls to zero means the broker is not taking
  them. **"Stalled" becomes a query, not an
  inspection** — which is what was missing when `memoryos-reflection-trigger` failed 32 times
  unnoticed.
- Backfill and rollback are one statement each: delete rows from `session_extraction` for a
  session to re-extract it; set `status='published'` to suppress it.

### 6. Lineage

The retrieval path records `lineage`, and `register_lineage()` moves to keyword-only
arguments — its current signature puts `generation_context_hash` before `generation_model`, so
a positional call stores the hash in the model column with no error. Without this the third
acceptance counter cannot move, and the gate would be half-matched to the architecture.

Defaults: `SESSION_IDLE_MINUTES=90`, `SESSION_MIN_MESSAGES=4`, `SESSION_CONTEXT_OVERLAP=4`,
`SESSION_MAX_LAG_HOURS=24`, `SESSION_MAX_PER_RUN=3`, sweep every 15 minutes.

## Consequences

### Positive

- Extraction stops depending on an interface we do not control; if upstream repairs the plugin
  hook, the claim table keeps the two from double-writing.
- Works on Slack, where no session-end event exists, and in gateway mode, where the current
  implementation blends concurrent threads.
- Closes the `points_count` gap that would have survived any hook-side fix.
- A sweep that finds nothing costs zero LLM calls; cost tracks finished conversations, not
  scheduler frequency.
- Every run leaves a status row, so the component's characteristic failure — running perfectly
  while producing nothing — becomes visible without a human reading logs.

### Negative

- Conversation boundaries are policy, not event. 90 minutes is derived from two threads of
  evidence; more data may move it.
- We are coupled to Hermes' storage schema. Version 26 is what we read; drift is detected and
  logged, not prevented.
- One more table, one more scheduled job, one more thing to migrate on the fleet.

### Risks

- **Compaction rewriting history** before a slice is swept. Unobserved today; mitigated by the
  24-hour lag ceiling and by a warning the first time a compacted row appears.
- **Cost spike** if many threads go quiet at once — bounded by `SESSION_MAX_PER_RUN`.
- **Threshold wrong for a different client's rhythm.** It is a configuration key, and the
  status row records what was sliced, so the evidence to retune it accumulates.

## Implementation Notes

- SQLite is opened read-only by URI; the sweeper never opens it read-write and never migrates
  it.
- The sweeper is a `--no-agent --script` cron job: no inference for scheduling itself.
- All thresholds are `${VAR:default}` keys in `config/services.yaml`, matching the existing
  convention.
- The PostgreSQL table is created by the sweeper on first run (`CREATE TABLE IF NOT EXISTS`),
  since this repository has no migration runner for host-side scripts.

## Review history

**Revision 2, 2026-08-22** — after an adversarial review (Codex, verdict *needs-attention*,
five findings). What changed and why:

| Finding | Verdict after checking the code and the data | Change |
| --- | --- | --- |
| Side effects across fs + PostgreSQL + ARQ are not atomic | **Valid.** `arq 0.28` does expose `_job_id`, so idempotent dispatch is available rather than theoretical | §3 claim/extract/publish/dispatch state machine |
| The 30-minute idle default splits a measured conversation | **Valid, and the ADR's own data proved it** — 42-minute in-thread gap | Default raised to 90 min, slice semantics stated, context overlap, `ended_at` fast path, lag ceiling |
| `mode=ro` does not give a consistent snapshot | **Valid.** The original text also said "never with a lock", which read as disabling reader locking | §2 single connection, `BEGIN DEFERRED`, `busy_timeout`, observed upper bound |
| Disabling the hook assumes transcript equivalence | **Partly valid.** Equivalence with the hook is a low bar — it stores `user[:200]`/`assistant[:500]` and today writes nothing at all. But the underlying point about tool rows is real: `role='tool'` rows carry the results, and dropping empty-content rows would have lost the call announcements | §4 explicit role handling, measured counts, warning on first `compacted` row |
| The design swaps an unstable API for an undocumented storage contract | **Valid in kind, over-scoped in remedy.** Version-gated adapters per image is more machinery than one image and one fleet justify | §5 `schema_version` recorded, drift logged, status row makes "stalled" queryable |

## Related Decisions

- Deployment-side record for the semitora host: `docs/carried-forward.md` §18 (component),
  §18.7 (acceptance red), §18.8 (import-order defect), §18.9 (service-link env collision) in
  `semitora-agent-prerequisites`.
- Fleet impact: `tasks/handover-2026-08-21-memory-os.md` in the same repository — eight agents
  run this stack.

## References

- `agent/turn_finalizer.py:805-812`, `agent/memory_provider.py:251` (Hermes 0.20.4, read in the
  running image).
- Probes behind every measurement here ran in `agent-ai-001-0`, 2026-08-21/22.
