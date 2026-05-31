# Memory OS — Scripts

Standalone Python scripts that maintain the Qdrant vector database and wiki pipeline.

## Qdrant Maintenance

| Script | What it does | Run |
|--------|-------------|-----|
| `decay_scanner.py` | Archives low-importance, aged AI content based on half-life decay | Weekly cron |
| `backfill_decay_metadata.py` | Populates missing `importance_score`, `last_accessed_at`, `confidence_score` in Qdrant points | Run once before enabling decay scanner |
| `semantic_dedup.py` | Merges near-duplicate points (cosine >0.92) | Monthly cron |

## Context Injection

| Script | What it does | Used by |
|--------|-------------|---------|
| `context_enhancer.py` | Embedding pipeline: query → embed → search Qdrant (4-level fallback). Also provides BM25 sparse embedding via FastEmbed. | Icarus `pre_llm_call` hook |

## Wiki Pipeline

| Script | What it does | Run |
|--------|-------------|-----|
| `wiki_continuous_ingest.py` | SHA-256 diff detection: finds new/modified wiki files, enqueues ARQ jobs in Redis | Hourly cron |
| `bulk_wiki_ingest.py` | One-shot bulk ingestion of all wiki files into Qdrant | After initial setup or collection rebuild |

## Quality Control

| Script | What it does | Run |
|--------|-------------|-----|
| `pre_validator.py` | Pre-flight validation of wiki documents: YAML frontmatter, required fields, link targets | Before ingestion |
| `reflection_trigger.py` | Idle detection for ARQ worker — enqueues micro-reflection when queue is empty and within hourly budget | Every 5min cron |

## Monitoring

| Script | What it does | Run |
|--------|-------------|-----|
| `dlq_manager.py` | Dead letter queue monitoring and reporting | Every 6h cron |

## Environment variables

All scripts read configuration from environment variables. See `.env.example` in the project root for the full reference.

Key variables:
- `OPENROUTER_API_KEY` — embeddings (required)
- `WIKI_PATH` — wiki root directory (default: `~/vault/wiki`)
- `COLLECTION_NAME` — Qdrant collection (default: `knowledge_base`)
- `EMBEDDING_DIMS` — vector dimensions (default: 4096)
- `REDIS_PASSWORD` — Redis auth (required for wiki ingest)
- `QDRANT_URL` — Qdrant endpoint (default: `http://localhost:6333`)
