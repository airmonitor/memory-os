# Architecture Decision Records

Decisions that shape MemoryOS and the icarus plugin, with the measurements behind them.

## Index

| ADR | Title | Status | Date |
| --- | --- | --- | --- |
| [0001](0001-session-extraction-via-state-db-sweeper.md) | Extract sessions by sweeping Hermes' `state.db`, not from the plugin hook | Proposed | 2026-08-22 |
| [0002](0002-hardening-the-session-sweeper.md) | Hardening the session sweeper — sanitisation, idempotent ingestion, single-writer, and a failure ceiling | Proposed | 2026-08-22 |

## Creating a new ADR

1. Copy the shape of 0001: context first, and every claim in it measured rather than reasoned.
2. Number it `NNNN-title-with-dashes.md`.
3. Add a row above.

## Status values

- **Proposed** — written, not yet agreed.
- **Accepted** — agreed and being implemented.
- **Superseded** — replaced; the replacement links back.
- **Rejected** — considered and turned down. Kept, because the reasoning is the value.
