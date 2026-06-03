# Memory OS — Scripts

Standalone Python scripts that maintain Postgres / Qdrant / Valkey and the wiki pipeline. All scripts read service hosts/models from `config/services.yaml` via the shared `memos_config` loader; secrets come from `.env`.

## Postgres

| Script | What it does | Run |
|--------|-------------|-----|
| `init_schema.sql` | DDL for the `lineage` + `reflection_budget` tables. `gen_random_uuid()` PK, `tsvector GENERATED ALWAYS` over `query`, GIN on `query_tsv` + `retrieved_chunk_ids jsonb_path_ops`. | One-shot on bootstrap (`psql -d memos -f init_schema.sql`) |
| `db.py` | `get_conn()` — sync psycopg helper. Reads DSN from `config.postgres`. Used by `context_enhancer.py`, `reflection_trigger.py`, `migrate_to_postgres.py`. | Library — import only |
| `migrate_to_postgres.py` | One-shot copy of `lineage` + `reflection_budget` from a legacy `state.db` SQLite into Postgres. Idempotent (`ON CONFLICT DO NOTHING/UPDATE`). | Once during migration |

## Qdrant Maintenance

| Script | What it does | Run |
|--------|-------------|-----|
| `decay_scanner.py` | Archives low-importance, aged AI content based on half-life decay | Weekly cron |
| `backfill_decay_metadata.py` | Populates missing `importance_score`, `last_accessed_at`, `confidence_score` in Qdrant points | Run once before enabling decay scanner |
| `semantic_dedup.py` | Merges near-duplicate points (cosine >0.92) | Monthly cron |

## Context Injection

| Script | What it does | Used by |
|--------|-------------|---------|
| `context_enhancer.py` | Embedding pipeline: query → LiteLLM embed → Qdrant hybrid search (4-level fallback ends at Postgres FTS over `lineage.query_tsv`). Also provides BM25 sparse embedding via FastEmbed and registers each query in Postgres `lineage`. | Icarus `pre_llm_call` hook |

## Wiki Pipeline

| Script | What it does | Run |
|--------|-------------|-----|
| `wiki_continuous_ingest.py` | SHA-256 diff detection: finds new/modified wiki files, enqueues ARQ jobs in Valkey | Hourly cron |
| `bulk_wiki_ingest.py` | One-shot bulk ingestion of all wiki files into Qdrant (via LiteLLM embedding) | After initial setup or collection rebuild |

## Quality Control

| Script | What it does | Run |
|--------|-------------|-----|
| `pre_validator.py` | Pre-flight semantic linter against `knowledge_base` (embeds via LiteLLM, searches Qdrant). | Before destructive actions / decision points |
| `reflection_trigger.py` | Idle detection for ARQ worker — enqueues micro-reflection when queue is empty and within hourly Postgres-tracked budget | Every 5 min cron |

## Monitoring

| Script | What it does | Run |
|--------|-------------|-----|
| `dlq_manager.py` | Dead letter queue monitoring and reporting | Every 6h cron |

## Configuration

All scripts read `config/services.yaml` via `from memos_config import config`. Inspect resolved values with:

```bash
python -m memos_config
```

Required secrets in `.env`:

- `POSTGRES_USER_PASSWORD` — Postgres auth
- `LITELLM_API_KEY` — LiteLLM proxy auth
- `VALKEY_PASSWORD` — only if Valkey has auth
- `QDRANT_API_KEY` — only if Qdrant has auth

Optional env overrides (see `infrastructure/architecture.md` for the full list): `POSTGRES_IP/PORT/USER_NAME/DB`, `VALKEY_IP/PORT`, `QDRANT_URL/HOST/PORT/COLLECTION`, `LITELLM_URL`, `EMBEDDING_MODEL`, `CHAT_MODEL`, etc.
