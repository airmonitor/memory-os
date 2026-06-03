# Infrastructure

> External services (Postgres, Valkey, Qdrant), the local ARQ worker, the cron jobs, and the configuration layer that holds the stack together.

## Configuration

All hosts/ports/model names live in a single YAML file. Secrets stay in `.env`. The loader interpolates `${ENV_VAR}` placeholders at startup.

```
config/services.yaml        committed   → hosts, ports, model names
config/services.local.yaml  gitignored  → deep-merged dev/prod override
.env                        gitignored  → secrets (passwords, API keys)
```

Every script and the worker import the same loader:

```python
from memos_config import config
config.postgres.host            # → "192.168.1.134"
config.valkey.port              # → 6389
config.qdrant.collection        # → "knowledge_base"
config.litellm.models.chat.name # → "lm-studio-qwen3.6"
```

Nothing reads service hosts/ports/model IDs via `os.environ.get(...)` directly — single source of truth. Sanity-check resolution any time with:

```bash
python -m memos_config
```

## External services

Postgres, Valkey/Redis, and Qdrant are not provisioned by `docker compose`. They are expected to be reachable at the hosts declared in `config/services.yaml`. They can live on a NAS, a remote VM, or `localhost`.

| Service | Role | Default in `services.yaml` |
|---|---|---|
| **Postgres 14+** | `lineage` (FTS via tsvector GIN) + `reflection_budget` (hourly counter). DDL in `scripts/init_schema.sql`. | `192.168.1.134:5432/memos` |
| **Valkey 7+** (or Redis 7+) | ARQ broker — embedding/ingestion/reflection jobs. | `192.168.1.134:6389` |
| **Qdrant 1.17+** | `knowledge_base` collection — dense 4096d Cosine + sparse BM25 IDF. Created automatically by the worker on first start. | `192.168.1.135:6333` |
| **LiteLLM proxy** | OpenAI-compatible embed + chat. Brokers to local MLX / OpenRouter / Anthropic / Ollama / etc. | `https://litellm.airmonitor.pl/v1` |

## Docker (worker-only)

Only the ARQ worker lives in `docker compose`. The build context is the repo root so that `memos_config/` and `config/` are baked into the image.

```yaml
# docker/docker-compose.yml
services:
  worker:
    build:
      context: ..
      dockerfile: docker/worker/Dockerfile
    restart: unless-stopped
    env_file:
      - ../.env
    environment:
      CONFIG_PATH: /app/config/services.yaml
      WIKI_PATH: /wiki
    volumes:
      - ${WIKI_PATH:-./wiki}:/wiki:ro
      - ${HERMES_HOME:-~/.hermes}:/hermes:rw
```

```bash
cd docker
docker compose up -d --build worker
docker compose logs -f worker
```

Expected startup logs:

```
Starting ARQ worker...
Starting worker for 5 functions: process_ingestion, process_reflection, process_micro_reflection, process_wiki_file, cron:process_reflection
Worker starting...
Connected to Qdrant at <qdrant-host>:6333
Creating collection knowledge_base with dense=4096 dims + sparse BM25    # first run only
Qdrant connection validated
Postgres pool ready: <pg-host>:5432/memos (min=1 max=4)
```

## Cron jobs

All cron scripts read the same `config/services.yaml`. They need no extra env wiring beyond `.env` secrets.

| Job | Recommended schedule | What it does |
|-----|---------------------|--------------|
| **wiki-continuous-ingest** | Hourly (:00) | SHA-256 diff detection → enqueues ARQ jobs to Valkey |
| **wiki-raw-ingest-monitor** | 2x/week | Read raw/ files → extract concepts/entities/comparisons → wiki pages |
| **vault-curator-weekly** | Weekly | Frontmatter enrichment + semantic linking + INDEX.md |
| **decay-scanner** | Weekly | Archive low-importance, aged AI content from Qdrant |
| **dlq-auto-report** | Every 6h | Dead letter queue monitoring and reporting |
| **reflection-trigger** | Every 5m | Idle detection + budget gate → enqueues `process_micro_reflection` |
| **maas-heartbeat** | Every 6h | Infrastructure health check |
| **holographic-memory-backup** | Weekly | Backup workspace memory files + SQLite DBs + Postgres dump |
| **monitor-credit-balance** | Daily | Credit/quota check for the LLM provider behind LiteLLM |

**Interactions:**
- `wiki-raw-ingest-monitor` creates wiki pages → next `wiki-continuous-ingest` picks them up and enqueues them via Valkey
- `vault-curator-weekly` enriches all vault files (frontmatter, semantic links, INDEX.md)
- `reflection_trigger.py` UPSERTs `reflection_budget` in Postgres — the **same row** the Docker worker writes; this is the cross-process write hotspot that motivated the SQLite → Postgres migration

## Environment variables

`config/services.yaml` references env vars with `${NAME:default}` syntax. The only values required in `.env`:

### Required

| Variable | Purpose | Example |
|----------|---------|---------|
| `POSTGRES_USER_PASSWORD` | Postgres auth | `s3cret!` |
| `LITELLM_API_KEY` | LiteLLM proxy auth | `sk-...` |
| `FABRIC_DIR` | Where Icarus writes fabric entries (absolute path; systemd does not expand `~`) | `/home/your-user/vault/fabric` |

### Optional (override `services.yaml` defaults)

| Variable | Used in YAML as | Default |
|----------|-----------------|---------|
| `POSTGRES_IP`, `POSTGRES_PORT`, `POSTGRES_USER_NAME`, `POSTGRES_DB` | postgres section | `192.168.1.134:5432`, `postgres`, `memos` |
| `VALKEY_IP`, `VALKEY_PORT`, `VALKEY_PASSWORD` | valkey section | `192.168.1.134:6389`, empty |
| `QDRANT_URL`, `QDRANT_HOST`, `QDRANT_PORT`, `QDRANT_API_KEY`, `QDRANT_COLLECTION` | qdrant section | `192.168.1.135:6333`, empty, `knowledge_base` |
| `LITELLM_URL` | litellm.base_url | `https://litellm.airmonitor.pl/v1` |
| `EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS`, `EMBEDDING_CONTEXT_LENGTH` | embedding model | `rapid-mlx-qwen3-embedding-8b`, 4096, 32768 |
| `CHAT_MODEL`, `EXTRACTION_MODEL`, `ICARUS_EXTRACTION_MAX_TOKENS` | chat / extraction models | `lm-studio-qwen3.6`, 4096 |
| `CONFIG_PATH` | absolute override for the YAML location | `/repo/config/services.yaml` |
| `HERMES_HOME`, `STATE_DB_PATH`, `MEMORY_STORE_DB`, `VAULT_PATH`, `WIKI_ROOT`, `TELEMETRY_LOG_PATH`, `REFLECTION_LOG_PATH` | paths section | `~/.hermes/...` |

## File locations

| Component | Path |
|-----------|------|
| Workspace memory (Layer 1) | `$HERMES_HOME/memories/` |
| Session DB (Layer 2, Hermes-owned) | `$HERMES_HOME/state.db` |
| Fact store DB (Layer 3, Hermes-owned) | `$HERMES_HOME/memory_store.db` |
| Memory OS tables (Layer 5 fallback + budget) | Postgres `memos` database |
| Icarus plugin | `$HERMES_HOME/plugins/icarus/` |
| Fabric entries (Layer 4) | `$FABRIC_DIR` |
| Wiki files (Layer 6) | `$VAULT_PATH/wiki/` |
| Qdrant data | NAS / Qdrant host volume |
| Docker compose | `docker/docker-compose.yml` |
| Cron scripts | `scripts/` |

## System requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| Worker host RAM | 4 GB | 8 GB |
| NAS / Qdrant host RAM | 8 GB | 16 GB (depends on vector count) |
| Worker host disk | 5 GB | 20 GB |
| Docker | 24.0+ | Latest stable |
| Python | 3.11+ | 3.11 (tested) |
| Hermes Agent | 0.14.0+ | 0.15.2 (tested) |
| Postgres | 14+ | 16 (tested) |
| Valkey / Redis | 7+ | 7.2 (tested) |
| Qdrant | 1.17+ | 1.17.1 (tested) |
