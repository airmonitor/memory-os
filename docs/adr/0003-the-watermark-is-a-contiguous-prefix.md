# ADR-0003: The watermark is a contiguous prefix, and the ceiling is keyed to where a slice starts

**Status**: Proposed
**Date**: 2026-08-22
**Deciders**: operator (semitora), icarus/memory-os maintainers
**Extends**: [ADR-0001](0001-session-extraction-via-state-db-sweeper.md), [ADR-0002](0002-hardening-the-session-sweeper.md)

## Context

Two findings came out of ADR-0002's whole-branch review. They were filed separately, as issues
#14 and #15, and reading them apart is what made each look survivable. They are one problem.

### The stranding (#14)

`watermarks()` is `MAX(last_message_id) … WHERE status <> 'failed'`. A `failed` row sitting
*below* a `published` one is a hole that `MAX` cannot express:

1. Sweep A claims `(S, 1..10)`, commits, releases the transaction-scoped lock, starts extracting.
2. Sweep B locks S, re-reads the watermark: A's row is `claimed`, so it counts — `watermarks()[S] = 10`.
   B's slice therefore starts at 11, `10 >= 11` is false, the slice is not stale, and B claims
   `(S, 11..20)`, extracts and publishes.
3. A's extraction fails transiently. `(S, 10)` becomes `failed`.
4. `watermarks()[S]` is now **20**. `find_candidates` offers nothing below 20.

Messages 1–10 are never read again. The row stays `failed` forever with nothing to reclaim it,
and no counter anywhere says so.

Under the deployed topology this needs an operator's `--session` repair run racing the cron —
the installed cron line takes `flock -n`, the manual one does not. That is a narrow door, not a
closed one.

### The ceiling that never bites (#15)

`attempts` lives on the `(session_id, last_message_id)` row. Every retry rebuilds the slice from
the watermark, so **one new message between two failing sweeps produces a new key with
`attempts = 0`**. Because slices always start at the watermark, the message that broke the
model is re-included every time, so the failure repeats and the ceiling never fires. It works
only when the boundary is stable — proven against a real PostgreSQL in
`test_a_stale_claim_with_a_prior_failure_keeps_its_attempts_count`, where nothing new arrives.

### Why they are one problem

The obvious fix for #14 is to stop the watermark from skipping a hole: hold it at the failed
slice until that slice resolves. That is correct, and on its own it converts #15 from *"the
ceiling is weaker than advertised"* into *"a session can be blocked forever"* — because the
thing meant to bound the block is exactly the counter that resets. Fixing #14 without #15
is worse than fixing neither.

## Decision Drivers

- A memory is written once and read for months; losing one silently is the worst outcome here.
- A conversation that keeps arriving must not be held up indefinitely by one slice that will
  never parse.
- Whatever bounds the block must be a counter that cannot be reset by ordinary activity.
- No new table. This component already has two, and a third would need its own migration story.

## Decisions

### 1. The watermark is the contiguous consumed prefix

```sql
SELECT session_id,
       COALESCE(
           MIN(first_message_id) FILTER (WHERE status = 'failed') - 1,
           MAX(last_message_id)  FILTER (WHERE status <> 'failed')
       ) AS watermark
FROM session_extraction
GROUP BY session_id
```

A `failed` row caps its session's watermark just below where that slice starts. Everything above
the hole stays unclaimable until the hole resolves — which is the point: those messages were
already published, their rows still exist, and their unique keys still refuse a second claim.

**What this costs, stated plainly:** while a slice is `failed`, its session makes no forward
progress. That is a deliberate trade — a conversation paused for a few sweeps is recoverable, a
conversation with a permanent hole under a published watermark is not.

**`quarantined` still counts.** That is the whole mechanism: `failed` is a hole and blocks,
`quarantined` is terminal and lets the session move on. Which is why decision 2 has to work.

### 2. `attempts` is keyed to where the slice STARTS, not where it ends

On insert, a new row inherits the highest `attempts` recorded for the same
`(session_id, first_message_id)`:

```sql
INSERT INTO session_extraction (session_id, first_message_id, last_message_id, message_count,
                                attempts)
VALUES (%s, %s, %s, %s,
        COALESCE((SELECT MAX(attempts) FROM session_extraction
                  WHERE session_id = %s AND first_message_id = %s), 0))
ON CONFLICT …
```

A retry of a hole always begins at the same message — the watermark is pinned by decision 1 —
so the count now accumulates across retries even as the slice's tail grows with the
conversation. Three deterministic failures retire the slice as `quarantined`, the watermark
advances past it, and the session resumes.

**Why not count per session.** A session is not the failing unit; a slice is. Counting per
session would retire a conversation because of one bad passage in it.

**Why not a stable slice id.** Keying on `first_message_id` is the same thing without a new
column: for a hole, the start is stable by construction.

## Consequences

### Positive

- A failed slice can no longer be buried under a published watermark. The hole is visible, it
  blocks, and it resolves — one way or the other, in a bounded number of sweeps.
- The ceiling becomes real for exactly the case it was written for: a slice that cannot be
  parsed, in a conversation that is still alive.
- No schema change beyond what `ADD COLUMN IF NOT EXISTS` already established.

### Negative

- A session with a `failed` slice stops producing until that slice succeeds or is retired —
  up to `max_attempts` sweeps, 45 minutes at the default cadence. Before this change it would
  have kept producing and quietly lost the hole. That is the trade, and it is the right way
  round.
- `watermarks()` becomes a slightly more expensive query. It is one grouped scan of a table with
  one row per slice; irrelevant at this size, worth remembering at a hundred thousand.

### Risks

- **A `failed` row nobody retries.** If `find_candidates` stops offering a session for an
  unrelated reason — a bug in the idle logic, say — the block becomes permanent instead of
  bounded. Mitigated by `sweeper_status`, which now records `aborted`, `locked_out` and
  `stale_slices`, and by the lag ceiling that forces an aged slice to be offered.
- **Inherited attempts on a genuinely different slice.** Two slices can share a
  `first_message_id` only if the watermark was pinned there, which is precisely the retry case.
  A session whose watermark legitimately returns to the same start after a quarantine will
  inherit a count it did not earn — bounded by the quarantine that just happened, and visible
  in the row.

## Implementation Notes

- Both changes are in `scripts/session_store.py`; the sweeper needs none.
- The integration suite is where this gets proven, not the fakes: `FakePg` computes watermarks
  in Python and would happily agree with whatever the code does. Required cases, against a real
  PostgreSQL: a `failed` row below a `published` one caps the watermark; the capped session
  offers the failed slice again; `attempts` survives a growing tail; three deterministic
  failures on a growing session reach `quarantined`; and the watermark then advances past it.
- `FakePg.watermarks()` must be updated to the same rule, or the sweeper tests will keep
  asserting the old semantics and pass while production does something else.

## Related

- Issues #14 and #15 in `semitora/semitora-agent-prerequisites`.
- ADR-0002 decision 4 (classification, breakers, ceiling) — this ADR makes its ceiling reachable.
