# Infrastructure

> Docker services, cronjobs, and environment configuration that support the 6 memory layers.

## Docker Services

The vector and pipeline layers run as Docker containers:

```yaml
# docker-compose.yml
services:
  qdrant:
    image: qdrant/qdrant:v1.17.1
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - ./qdrant_data:/qdrant/storage
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --requirepass ${REDIS_PASSWORD}
    restart: unless-stopped

  worker:
    build: ./worker
    depends_on:
      - qdrant
      - redis
    environment:
      - QDRANT_URL=http://qdrant:6333
      - REDIS_URL=redis://:${REDIS_PASSWORD}@redis:6379/0
      - EMBEDDING_DIMS=${EMBEDDING_DIMS:-4096}
      - COLLECTION_NAME=${COLLECTION_NAME:-knowledge_base}
      - OPENROUTER_API_KEY=${OPENROUTER_API_KEY}
    restart: unless-stopped
```

**Key configuration:**
- `EMBEDDING_DIMS=4096` — must match Qdrant collection schema
- `COLLECTION_NAME=knowledge_base` — target collection for all wiki ingestion
- Redis password required — set in `.env`, used by both Redis container and worker

## Cronjobs

| Job | Recommended schedule | What it does |
|-----|---------------------|--------------|
| **wiki-continuous-ingest** | Hourly (:00) | SHA-256 diff detection → embed new wiki files → Qdrant |
| **wiki-raw-ingest-monitor** | 2x/week | Read raw/ files → extract concepts/entities/comparisons → create wiki pages |
| **vault-curator-weekly** | Weekly | Phase 1 (frontmatter enrichment) + Phase 2 (semantic linking) + Phase 3 (INDEX.md) |
| **decay-scanner** | Weekly | Archive low-importance, aged AI content from Qdrant |
| **dlq-auto-report** | Every 6h | Dead letter queue monitoring and reporting |
| **maas-heartbeat** | Every 6h | Infrastructure health check |
| **holographic-memory-backup** | Weekly | Backup of workspace memory files and databases |
| **monitor-openrouter-balance** | Daily | OpenRouter credit balance check |

**Interaction between jobs:**
- `wiki-raw-ingest-monitor` creates new wiki pages → next `wiki-continuous-ingest` picks them up and sends to Qdrant
- `vault-curator-weekly` enriches ALL vault files — adds frontmatter, semantic links, and INDEX.md

## Environment Variables

### Required

| Variable | Purpose | Example |
|----------|---------|---------|
| `FABRIC_DIR` | Where Icarus writes fabric entries | `/home/your-user/vault/fabric` |
| `OPENROUTER_API_KEY` | Embedding + LLM extraction | `sk-or-...` |
| `REDIS_PASSWORD` | Redis authentication | (generated) |

### Strongly recommended

| Variable | Default | Recommended | Why |
|----------|---------|-------------|-----|
| `ICARUS_EXTRACTION_MAX_TOKENS` | 1024 | **4096** | 1024 causes fabric truncation |
| `ICARUS_EXTRACTION_MODEL` | deepseek-v4-flash | same | Any OpenRouter chat model works |
| `EMBEDDING_DIMS` | varies | **4096** | Must match Qdrant collection schema |

### Optional

| Variable | Purpose |
|----------|---------|
| `ICARUS_OBSIDIAN=1` | Enable Obsidian wikilinks and daily notes |
| `OBSIDIAN_VAULT_PATH` | Vault root (if fabric is a subfolder) |
| `ICARUS_RESULT_MAX_CHARS` | Fallback truncation limit (default 500) |
| `ICARUS_TASK_MAX_CHARS` | Fallback task truncation (default 300) |
| `TOGETHER_API_KEY` | For training/eval tools |
| `OPENROUTER_FULL_API_KEY` | Alternative key for LLM extraction |
| `OPENROUTER_DS_API_KEY` | Alternative key for LLM extraction |
| `CURATOR_LOG_LEVEL` | Logging level for Vault Curator |
| `VAULT_PATH` | Path to vault root for Vault Curator |

## File locations

| Component | Path |
|-----------|------|
| Workspace memory | `$HERMES_HOME/memories/` |
| Session DB | `$HERMES_HOME/state.db` |
| Fact store DB | `$HERMES_HOME/memory_store.db` |
| Icarus plugin | `$HERMES_HOME/plugins/icarus/` |
| Fabric entries | `$FABRIC_DIR` |
| Wiki files | `$VAULT_PATH/wiki/` |
| Qdrant data | `./qdrant_data/` (Docker volume) |
| Docker compose | Project root |
| Cron scripts | Project scripts directory |

## System requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 8 GB | 16 GB (Qdrant + Redis + ARQ worker) |
| Disk | 20 GB | 50 GB (Qdrant vectors + wiki files) |
| Docker | 24.0+ | Latest stable |
| Python | 3.11+ | 3.11 (tested) |
| Hermes Agent | 0.14.0+ | 0.15.2 (tested) |
| Qdrant | 1.17+ | 1.17.1 (tested) |
