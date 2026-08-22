# ADR-0003: A failed slice is retried at its own range, and the watermark is the frontier

**Status**: Proposed (revision 2 — revision 1 was reviewed and rejected; see "What revision 1 got wrong")
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

**How wide the door is — corrected.** ADR-0002 and revision 1 of this ADR both assert that the
installed cron line takes `flock -n`, which makes this need a manual `--session` run racing the
cron. **That flock does not exist.** Measured 2026-08-22 on the reference host:
`/opt/data/scripts/memoryos-session-sweeper.sh` is 4 lines of `unset`/`cd`/`exec` and
`grep -c flock` on it returns `0`; the template it is rendered from,
`roles/memory_os/templates/memoryos-cron.sh.j2`, contains no `flock` either, and neither does
any script in this repo. So the race needs no operator at all: a sweep that runs longer than the
15-minute cron cadence — several LLM calls at a 120 s timeout will do it — is overlapped by the
next tick. Whether `hermes cron` serialises its own executions was not determined and is not
worth determining: decision 4 below makes the question moot.

### The ceiling that never bites (#15)

`attempts` lives on the `(session_id, last_message_id)` row. Every retry rebuilds the slice from
the watermark, so **one new message between two failing sweeps produces a new key with
`attempts = 0`**. Because slices always start at the watermark, the message that broke the
model is re-included every time, so the failure repeats and the ceiling never fires. It works
only when the boundary is stable — proven against a real PostgreSQL in
`test_a_stale_claim_with_a_prior_failure_keeps_its_attempts_count`, where nothing new arrives.

### Why they are one problem

Both are the same mistake: **a failed slice is not retried, it is re-derived.** Nothing in the
system remembers that `(S, 1..10)` is a unit of work that owes an answer; the next sweep just
asks "what is unconsumed?" and builds a fresh slice from that. #14 is what happens when the
answer to that question skips the hole. #15 is what happens when the answer to it drifts.

Fix the re-derivation and both stop.

## What revision 1 got wrong

Revision 1 kept the re-derivation and tried to steer it: it capped the watermark at
`MIN(first_message_id) FILTER (WHERE status='failed') - 1`, so the fresh path would be aimed at
the hole. An adversarial review rejected it, and every finding was confirmed against the code
before this rewrite:

1. **It deadlocks the exact case it repairs.** `hermes_state.read_slice(con, s, after_id=…)` is
   unbounded — it returns *every* active message above the watermark. With the cap at 0 and rows
   `(S,1..10,failed)`, `(S,11..20,published)`, the fresh path builds `(S, 1..20)`, not
   `(S, 1..10)`. `claim()` then conflicts on `(session_id, last_message_id=20)`, whose row is
   `published`, and `_CLAIM_RECLAIM_SQL`'s `DO UPDATE … WHERE status='failed' OR (stale claimed)`
   refuses it. `won=False`, "already claimed", skip — every sweep, forever. And once message 21
   arrives the key is free, so the sweeper re-extracts 11..20 alongside 1..21 as one growing
   superslice: duplicated memories on top of a hole that still never closes.
2. **Attempt inheritance by `first_message_id` has no generation.** It would hand a brand-new,
   larger slice the count an older one earned, retiring messages that never got their own attempts.
3. **The bound was not a bound.** `_EXPIRE_STALE_SQL` rewrites a crashed claim to `failed`
   without touching `attempts`, and transient and unclassified failures call
   `mark_failed(count_attempt=False)`. Under revision 1 every one of those blocked its session
   while being unable to ever reach the ceiling — the 45-minute worst case was wrong before the
   two-hour stale-claim window is even considered.

Revision 2 keeps neither decision.

## Decision Drivers

- A memory is written once and read for months; losing one silently is the worst outcome here.
- A conversation that keeps arriving must not be held up by one slice that will never parse.
- Whatever bounds a retry must be a counter that ordinary activity cannot reset.
- No new table. A column via `ADD COLUMN IF NOT EXISTS` is the established migration path here.

## Decisions

### 1. A `failed` row is retried at its own recorded range

The row already stores `first_message_id` and `last_message_id`. The retry reads exactly that
range out of `state.db` — `read_slice_range(con, session_id, first_id, last_id)`, a new sibling
of `read_slice` that bounds both ends — instead of re-deriving a slice from the watermark.

Everything that was hard becomes mechanical:

- The claim key is the row's own `(session_id, last_message_id)`. `_CLAIM_RECLAIM_SQL`'s
  `WHERE session_extraction.status = 'failed'` arm is the one that fires. No new SQL.
- `attempts` accumulates on that one persistent row, because `DO UPDATE` touches only `status`
  and the timestamps. **Decision 2 of revision 1 disappears with no replacement** — the key is
  stable by construction, so there is nothing to inherit.
- A hole under a published watermark is offered again without the watermark having to express it.

**The retry path runs before the fresh path**, so a session that owes an answer gives it before
it takes on more work.

**Retry slices skip the `idle_seconds` and `min_messages` gates.** Those gates ask "has this
conversation finished growing" — a question already answered for a range that was claimed once.
They still count against `max_per_run` and against both circuit breakers, because a retry costs
the same LLM call as any other slice and fails the same systemic ways.

**An empty range is terminal, not a loop.** If `read_slice_range` comes back empty — every
message in it deactivated or rewritten by compaction since the claim — the row is `quarantined`
with `range no longer readable`. There is nothing left to extract and no number of retries will
change that.

### 2. The fresh path's watermark is the frontier

```sql
SELECT session_id, MAX(last_message_id) FROM session_extraction GROUP BY session_id
```

All statuses, `failed` included. This is not cosmetic — without it decision 1 double-claims.
With `(S,1..10,failed)` and a tail that has grown to 25, the old rule excludes the failed row,
so the fresh path sees `after = 0` and builds `(S, 1..25)`: a *different* key from `(S,10)`, so
the UNIQUE constraint cannot catch it, and messages 1–10 are now claimed twice at once — by the
retry path and by the fresh path.

Excluding `failed` was only ever there to make a failed slice reclaimable. Decision 1 is now
what makes it reclaimable, so the exclusion has no job left and is actively harmful.

Both cases check out:

| | frontier | retry path offers | fresh path offers |
|---|---|---|---|
| hole: `(1..10,failed)`, `(11..20,published)` | 20 | `1..10` | `21…` |
| top failure: `(1..10,failed)`, tail to 25 | 10 | `1..10` | `11..25` |

Note what the second row costs: **nothing**. The tail is not held hostage by the hole, which is
revision 1's entire "negative consequence" section deleted rather than accepted.

### 3. Every producer of `failed` gets a bounded exit

`attempts` counts *classified deterministic* failures only, and that is right — an outage must
not spend the budget that exists for unparseable content (ADR-0002 decision 4). But it leaves
three producers of `failed` with no exit at all: `expire_stale_claims`, transient failures, and
unclassified ones. Under decision 1 those rows are retried forever.

A second column, `retries INTEGER NOT NULL DEFAULT 0`, incremented on **every** transition into
`failed` regardless of classification. At `memory_os_session_retry_ceiling` (default 20) the row
is `quarantined` with `exhausted after N retries`. `rollback_attempt` refunds `attempts` only —
a run the breaker aborted still consumed a retry, because it did.

**Why a counter and not a maximum age.** An age bound punishes the wrong outage: a stack that
was down for two days would quarantine every open slice on the first boot after it, having never
retried any of them. A counter only advances when work was actually attempted.

At the 15-minute cadence, 20 retries is five hours of continuous failure before a slice is
retired — long enough to ride out a model outage, short enough that an operator reading
`sweeper_status` sees a bounded number.

### 4. Mutual exclusion moves into the CLI

`session_sweeper.py` takes an exclusive `fcntl.flock` on a lockfile under the repo root, at
startup, for every invocation — cron and manual `--session` alike — and exits `0` with one log
line if it cannot get it. `--no-lock` exists for a controlled recovery run and is the only way
past it.

This is where the lock belongs and the cron wrapper is not: the wrapper cannot cover a manual
run, and a manual run racing the cron is the documented shape of #14. A host-local file lock is
sufficient because `state.db` is host-local — two sweepers on different hosts is not a topology
this component has. It also moots the unanswered question above about whether `hermes cron`
serialises: with the lock unconditional, it does not matter.

Decisions 1–3 stay regardless. This closes the door; those repair what already came through it.

## Consequences

### Positive

- A failed slice can no longer be buried under a published watermark, and it no longer blocks
  the messages above it either.
- The ceiling becomes real for the case it was written for, because the key it counts on stops
  moving.
- Every path into `failed` now has a way out.
- No schema change beyond one more `ADD COLUMN IF NOT EXISTS`.

### Negative

- Two candidate sources instead of one — the retry path and the fresh path — with a defined
  precedence between them. That is real complexity, in exchange for deleting the watermark
  arithmetic that revision 1 needed.
- `retries` and `attempts` are two counters that a reader must not confuse. Their column comments
  say which is which and why they cannot be one.
- Messages 1–10 in the hole case are extracted with less following context than they would have
  had; `context_tail` is a *preceding* window. A retry sees the conversation as it stood, which
  is what the original claim saw too.

### Risks

- **A retry that keeps succeeding at nothing.** A range that extracts cleanly but yields zero
  entries resolves as `published` and closes the hole, which is correct — but an operator reading
  entry counts should know a hole can close quietly. `sweeper_status` counts retried slices
  separately for that reason.
- **The frontier hides a genuinely stuck row from the fresh path.** If decision 1's retry path
  itself regresses, the failed row now consumes the watermark and its messages are unreachable
  from either path. That is the failure mode to write the integration test against first.

## Implementation Notes

- `scripts/session_store.py`: `watermarks()` → frontier; new `failed_slices(conn)`; `retries`
  column plus its increment in `mark_failed` and `expire_stale_claims`; the ceiling check.
- `icarus/hermes_state.py`: `read_slice_range(con, session_id, *, first_id, last_id)`.
- `scripts/session_sweeper.py`: the retry pass ahead of the fresh pass; the empty-range
  quarantine; the CLI lock.
- **`FakePg.watermarks()` in `tests/test_session_sweeper.py` must move to the frontier rule in
  the same change**, or the sweeper's unit tests keep asserting the old semantics and pass while
  production does something else. There are four such fakes (lines 48, 396, 1015, 1055).
- Proven against a real PostgreSQL, not the fakes: a `failed` row below a `published` one is
  offered at its own range and closes; a `failed` row with a growing tail does not double-claim
  with the fresh path; `attempts` accumulates across retries on the same row; three deterministic
  failures quarantine it and the frontier is unaffected; 20 transient failures quarantine it too;
  an emptied range quarantines on the first retry.

## Related

- Issues #14 and #15 in `semitora/semitora-agent-prerequisites`.
- ADR-0002 decision 4 (classification, breakers, ceiling) — this ADR makes its ceiling reachable
  and corrects its line 139 claim about `flock -n`.
