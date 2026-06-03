# Setup Guide

> Step-by-step installation of the Memory OS stack. Assumes Hermes Agent is already installed and configured.

## Prerequisites

- Hermes Agent 0.14.0+ (tested on 0.15.2)
- Python 3.11+
- Docker 24.0+ (only the ARQ worker runs locally; Qdrant/Valkey/Postgres live elsewhere)
- Reachable **Postgres 14+**, **Valkey 7+ or Redis 7+**, **Qdrant 1.17+** (can all sit on the same NAS, separate hosts, or `localhost`)
- LiteLLM-compatible proxy or OpenAI-compatible API (for embedding + chat models)
- 16 GB RAM recommended on the worker host (8 GB minimum)

## 1. Configure services

All host/port/model values live in `config/services.yaml` (single source of truth). Secrets live in `.env` (gitignored). The Python code never reads service hosts via `os.environ.get(...)` directly — it imports `from memos_config import config` and reads `config.postgres.host`, `config.litellm.api_key`, etc.

Edit `config/services.yaml`:

```yaml
postgres:
  host: ${POSTGRES_IP:192.168.1.134}
  port: ${POSTGRES_PORT:5432}
  user: ${POSTGRES_USER_NAME:postgres}
  password: ${POSTGRES_USER_PASSWORD}
  database: ${POSTGRES_DB:memos}

valkey:
  host: ${VALKEY_IP:192.168.1.134}
  port: ${VALKEY_PORT:6389}
  password: ${VALKEY_PASSWORD}

qdrant:
  url: ${QDRANT_URL:http://192.168.1.135:6333}
  collection: ${QDRANT_COLLECTION:knowledge_base}

litellm:
  base_url: ${LITELLM_URL:https://litellm.airmonitor.pl/v1}
  api_key: ${LITELLM_API_KEY}
  models:
    embedding:
      name: ${EMBEDDING_MODEL:rapid-mlx-qwen3-embedding-8b}
      dimensions: ${EMBEDDING_DIMENSIONS:4096}
    chat:
      name: ${CHAT_MODEL:lm-studio-qwen3.6}
    extraction:
      name: ${EXTRACTION_MODEL:lm-studio-qwen3.6}
```

Create `.env` from the template:

```bash
cp .env.example .env
# fill in: POSTGRES_USER_PASSWORD, VALKEY_PASSWORD (if any),
#         QDRANT_API_KEY (if any), LITELLM_API_KEY, HERMES_HOME, FABRIC_DIR, ...
```

Verify the config resolves correctly:

```bash
python -m memos_config
# expect a full JSON dump with no ${...} placeholders left
```

Local override (optional, gitignored): `config/services.local.yaml` deep-merges on top of the base file — useful for dev vs prod splits.

## 2. Bootstrap Postgres

```bash
psql -h <POSTGRES_HOST> -U <POSTGRES_USER> -c "CREATE DATABASE memos;"
psql -h <POSTGRES_HOST> -U <POSTGRES_USER> -d memos -f scripts/init_schema.sql
```

If you have no `psql` locally, the same can be done via Python:

```bash
python -c "
import psycopg
with psycopg.connect('postgresql://<user>:<pw>@<host>:5432/postgres', autocommit=True) as c:
    c.cursor().execute('CREATE DATABASE memos')
"
python -c "
import psycopg
with open('scripts/init_schema.sql') as f: sql = f.read()
with psycopg.connect('postgresql://<user>:<pw>@<host>:5432/memos') as c:
    c.cursor().execute(sql); c.commit()
"
```

This creates two tables:

| Table | Purpose |
|-------|---------|
| `lineage` | Generation provenance: session_id, query (with `tsvector` GIN index), retrieved chunk IDs (JSONB), generation model |
| `reflection_budget` | Hourly micro-reflection counter — cross-process (host cron + Docker worker) UPSERT target |

Both tables are Memory OS-owned. Hermes-owned tables (`sessions`, `messages`, `facts`) stay in their SQLite homes.

## 3. Bootstrap Qdrant + Valkey

Qdrant: nothing manual. The worker calls `ensure_collection()` on first start (creates `knowledge_base` with dense 4096d Cosine + sparse BM25 IDF).

Valkey/Redis: nothing manual. ARQ creates its queue keys on first job.

Quick health check:

```bash
curl -s <QDRANT_URL>/healthz
redis-cli -h <VALKEY_HOST> -p <VALKEY_PORT> -a "$VALKEY_PASSWORD" PING
curl -s -H "Authorization: Bearer $LITELLM_API_KEY" $LITELLM_URL/models | head
```

## 4. Migrate existing SQLite data (optional)

If you ran an earlier Memory OS version that stored `lineage` / `reflection_budget` in `state.db`, copy them to Postgres:

```bash
python scripts/migrate_to_postgres.py
# idempotent; safe to re-run. SQLite source is not deleted.
```

## 5. Icarus Plugin (bundled)

```bash
cp -r icarus/ ~/.hermes/plugins/icarus/
```

Enable Icarus in `~/.hermes/config.yaml`:

```yaml
enabled:
  - icarus       # required — fabric tools + context injection hooks
```

Restart the gateway:

```bash
hermes gateway restart
hermes status   # → icarus v0.3.0 (16 tools, 4 hooks)
```

## 6. Hermes-side `.env` (gateway profile)

Add to your Hermes profile `.env` (e.g. `~/.hermes/.env`):

```bash
# Same secrets as the Memory OS repo .env — the Icarus hook reads them too
POSTGRES_USER_PASSWORD=...
LITELLM_API_KEY=sk-...
FABRIC_DIR=/home/your-user/vault/fabric
HERMES_HOME=/home/your-user/.hermes

# Optional: tell the hook where the config lives, if outside the default
# (memos_config walks up looking for config/services.yaml; explicit override is safer)
CONFIG_PATH=/home/your-user/PycharmProjects/memory-os/config/services.yaml
```

**Use absolute paths.** The Hermes gateway runs under systemd — `~` is not expanded.

## 7. ARQ Worker

```bash
cd docker
docker compose up -d --build worker
docker compose logs -f worker
```

You should see:

```
Worker starting...
Connected to Qdrant at <host>:<port>
Creating collection knowledge_base with dense=4096 dims + sparse BM25  # first run only
Qdrant connection validated
Postgres pool ready: <host>:<port>/memos (min=1 max=4)
Starting worker for 5 functions: process_ingestion, process_reflection, ...
```

No traceback = good. Worker is now consuming from the Valkey queue.

## 8. Core File Modifications

Apply the changes documented in [modifications/soul-rulebook.md](../modifications/soul-rulebook.md):

- Add Ground Truth level 2 (injected memory) to `SOUL.md`
- Add memory architecture documentation to `rulebook.md`
- Add context injection convention to `SOUL.md`

These modifications ensure the agent trusts its injected memory as authoritative.

## 9. Wiki Setup

```bash
mkdir -p $VAULT_PATH/wiki/{raw,concepts,entities,comparisons,_meta,_archive}
# Copy SCHEMA.md template, create initial index.md and log.md
```

The wiki starts empty. Add source documents to `raw/` and the wiki-agent cronjob will begin extracting structured pages.

## 10. Cronjobs

Add to crontab (`crontab -e`):

```cron
# Wiki ingestion — enqueues new/changed wiki files to Valkey
0 * * * *   /usr/bin/python3 /path/to/scripts/wiki_continuous_ingest.py

# Qdrant maintenance
0 3 * * 0   /usr/bin/python3 /path/to/scripts/decay_scanner.py

# Dead letter queue monitoring
0 */6 * * * /usr/bin/python3 /path/to/scripts/dlq_manager.py

# Semantic dedup (first Sunday of month)
0 3 * * 0   [ $(date +\%d) -le 7 ] && /usr/bin/python3 /path/to/scripts/semantic_dedup.py

# Idle micro-reflection trigger
*/5 * * * * /usr/bin/python3 /path/to/scripts/reflection_trigger.py
```

Cron jobs read the same `config/services.yaml` as the worker — no extra env wiring needed beyond the `.env` secrets.

## 11. Verify

Inside Hermes chat:

```
/plugins
# → icarus v0.3.0 (16 tools, 4 hooks)

fabric_brief()
# → recent fabric entries (initially empty)

qdrant_search("test query")
# → results from knowledge_base (if wiki has content)

fact_store(action='probe', entity='test')
# → empty (no facts stored yet)
```

End-to-end smoke from the host:

```bash
python scripts/context_enhancer.py "test query"
# embedding succeeds (LiteLLM) + Qdrant search runs + lineage row inserted
psql <DSN> -c "SELECT lineage_id, query, created_at FROM lineage ORDER BY created_at DESC LIMIT 1;"
```

## What to expect

**Day 1:** Infrastructure running. Fabric entries begin accumulating at session end. Qdrant indexing starts as wiki files are added.

**Week 1:** Context injection active. Agent references past decisions automatically. Wiki pipeline producing curated pages from raw documents.

**Month 1:** Decay scanner has aged content to evaluate. Structured facts accumulating with trust scores.

## Troubleshooting

### Qdrant collection shows 0 points
- Confirm `config.litellm.models.embedding.dimensions` matches the collection schema. Mismatch → vectors rejected silently.
- Confirm the worker logs `"Creating collection ... with dense=N dims"` on first start.

### Fabric entries are truncated
- Confirm `config.litellm.models.extraction.max_tokens >= 4096` (defaults to 4096). `1024` causes truncation.
- Restart the gateway after edits.

### Memory tool reports "Icarus write conflict"
- Icarus is writing to MEMORY.md instead of CREATIVE.md. Verify the bundled (forked) Icarus is installed, not upstream `esaradev`.

### Context injection not working
- `python -m memos_config` should print a resolved config with no `${...}` placeholders.
- Check that `LITELLM_API_KEY` is set in `.env` (and reachable from the gateway host).
- Restart the gateway after edits to `hooks.py` or `config/services.yaml`.

### `database "memos" does not exist`
- Run step 2 (Bootstrap Postgres).

### Worker keeps restarting with `AuthenticationError: AUTH <password> called without any password configured`
- The Valkey password in `.env` ends with a trailing comment (`VALKEY_PASSWORD=          # empty`). `python-dotenv` treats the whole line as the value — strip it.

### Decay scanner produces "0 archived" every week
- Point payloads are missing `last_accessed_at` or `importance_score`. Run `scripts/backfill_decay_metadata.py` before enabling decay.

### Migrating from older SQLite-backed Memory OS
- Run `python scripts/migrate_to_postgres.py` once. After ~1 week of monitoring, drop the old SQLite tables: `sqlite3 ~/.hermes/state.db "DROP TABLE lineage; DROP TABLE reflection_budget;"` (sessions/messages stay — Hermes Gateway owns them).
