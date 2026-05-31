# Layer 3 — Structured Facts (Holographic Memory)

> **File:** `$HERMES_HOME/memory_store.db` (SQLite + HRR + FTS5 + trust scoring)
> **Tool:** `fact_store` (CRUD: add, search, probe, reason, update, remove, contradict)
> **Feedback:** `fact_feedback` (helpful/unhelpful)

## What it stores

Durable, structured facts with entity resolution and trust scoring:

```
facts table:
  fact_id, content, category (user_pref|project|tool|general),
  entities (JSON array), tags, trust_score, retrieval_count,
  helpful_count, created_at, last_accessed_at
```

## How the agent uses it

6 actions:

| Action | What it does | Example |
|--------|-------------|---------|
| `add` | Store a new fact | `fact_store(action='add', content='...', entities=['Qdrant'])` |
| `search` | Keyword lookup (FTS5) | `fact_store(action='search', query='Qdrant named vectors')` |
| `probe` | All facts about an entity | `fact_store(action='probe', entity='Docker')` |
| `reason` | Compositional: facts connected to multiple entities | `fact_store(action='reason', entities=['Qdrant', 'Docker'])` |
| `contradict` | Find conflicting claims | `fact_store(action='contradict')` |
| `update/remove` | CRUD maintenance | `fact_store(action='update', fact_id=87, trust_delta=0.1)` |

## Trust scoring

The system tracks which facts are actually useful:

- `retrieval_count` — how many times the fact was retrieved
- `helpful_count` — how many times it was marked helpful
- `trust_score` — calculated from ratio + Bayesian prior (starts at 0.50)

**Critical rule:** When you retrieve a fact via `probe`, `search`, or `reason` and reference it in your response, you MUST call `fact_feedback` in the same turn. Without feedback, `trust_score` is ornamental and fact quality degrades silently.

## Context injection (Icarus enhancement)

The Icarus fork adds `_search_facts()` — FTS5 search during `pre_llm_call` that injects relevant facts on the **first turn only** (to avoid per-turn cost). Labeled `[facts]` in prompt.

## Configuration

No configuration needed. DB path is managed by Hermes.

## Pitfalls

- **`retrieval_count` was broken for `probe()`/`reason()`** — only `search()` incremented it. Fixed in current code
- **Trust scores need feedback to move:** Without `fact_feedback` calls, every fact stays at 0.50
- **HRR is not exposed to the agent directly:** It powers entity resolution internally — the agent sees keyword search + entity linking, not the holographic vectors
