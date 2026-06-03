# Layer 5 — Vector Database (Qdrant)

> **Service:** Qdrant 1.17+ (reachable at `config.qdrant.url`)
> **Collection:** `config.qdrant.collection` (4096d Cosine + BM25 sparse by default)
> **Embedding:** any LiteLLM-brokered OpenAI-compatible embedding model. Default: `rapid-mlx-qwen3-embedding-8b` (local MLX serving Qwen3-Embedding-8B)
> **Endpoint:** declared in `config/services.yaml`

## Embedding model

The default `rapid-mlx-qwen3-embedding-8b` (Qwen3-Embedding-8B served via LiteLLM):

1. **Multilingual** — strong performance across 50+ languages
2. **Quality** — 4096-dimensional embeddings; consistently near the top of the MTEB leaderboard
3. **Speed** — fast inference at 8B parameters; suitable for hourly ingestion pipelines
4. **Local-friendly** — runs on Apple-Silicon MLX, or via any OpenAI-compatible backend behind LiteLLM (Ollama, vLLM, llama.cpp, OpenAI, OpenRouter, etc.)

Switching models requires only two edits to `config/services.yaml`:

```yaml
litellm:
  models:
    embedding:
      name: my-embedding-model
      dimensions: 1024
```

…and recreating the Qdrant collection so the `dense` vector size matches. The dimension validation in `docker/worker/services/embedding.py` raises immediately on a mismatch.

## What it stores

All knowledge that benefits from semantic search — wiki pages, session transcripts, raw documents, technical references. Content is ingested via the continuous ingest pipeline (hourly) and the wiki agent (scheduled).

## How the agent uses it

**Two access patterns:**

1. **Explicit** — `qdrant_search(query="memory architecture", top_k=3)` → agent calls the tool directly
2. **Automatic** — Icarus `pre_llm_call` injects relevant Qdrant results into the system prompt every turn (via `_search_qdrant()`)

**Search uses 4-level fallback cascade:**

```
1. Hybrid: dense (4096d cosine) + sparse (BM25) → RRF fusion
2. Dense-only: if sparse fails → pure vector search
3. Lexical: if Qdrant offline → markdown file search in vault
4. Postgres FTS: if vault inaccessible → tsvector @@ to_tsquery over lineage.query
5. None: all exhausted → return empty (fail-open)
```

## Collection schema

```json
{
  "vectors": {
    "dense": { "size": 4096, "distance": "Cosine" }
  },
  "sparse_vectors": {
    "sparse": { "index": { "on_disk": false } }
  }
}
```

**Named vectors:** Points must use `{"dense": [...], "sparse": SparseVector(...)}`. Must match schema exactly — unnamed vectors are rejected silently.

## Decay and dedup

| Mechanism | Schedule | What it does |
|-----------|----------|--------------|
| **Decay scanner** | Weekly | Archives AI-generated points with low importance and high age. Exempts human-generated content and high-importance (>0.7) points. Formula: `decay_score = exp(-ln(2) * age_days / half_life)` |
| **Semantic dedup** | Monthly | Merges near-duplicate points (cosine > 0.92). Tags union, priority by source_type. |

## Context injection (Icarus enhancement)

```python
_search_qdrant(query, top_k=2, threshold=0.55):
    1. Embed user message via context_enhancer pipeline (LiteLLM)
    2. search_with_fallback() → 4-level cascade
    3. Results labeled [qdrant] in system prompt
    4. Per-session dedup by point ID
```

**Auth:** `context_enhancer.py` reads `config.litellm.api_key` (resolved from `${LITELLM_API_KEY}` in `.env`). No per-process env propagation needed — the hook and the script read the same `config/services.yaml`.

**Social closer gate:** Trivial messages ("ok", "thanks", emoji-only) skip Qdrant search entirely — no point embedding small talk.

## Embedding pipeline

```
File in $VAULT_PATH/wiki/
    │
    ▼
wiki-continuous-ingest (hourly cron)
    │ SHA-256 diff detection
    ▼
Valkey queue (ARQ job)
    │
    ▼
ARQ Worker (Docker)
    │ embed via LiteLLM (config.litellm.models.embedding.name)
    │ get_sparse_embedding() → BM25 (fastembed, local)
    ▼
Qdrant upsert (with dedup check)
```

## Pitfalls

- **Named vectors are mandatory in Qdrant 1.17+:** `upsert` requires `"vector": {"dense": vec}`, plain vectors are rejected silently
- **`AsyncQdrantClient.search()` doesn't exist in qdrant-client 1.18:** Use REST API `POST /collections/{name}/points/search`
- **Embedding dimension mismatch is caught early:** `docker/worker/services/embedding.py` validates `len(vec) == config.litellm.models.embedding.dimensions` and raises immediately. Set `dimensions` in YAML to match the model AND the Qdrant collection schema before re-indexing.
- **Decay scanner is inert without payload metadata:** Requires `importance_score`, `last_accessed_at`, `confidence_score` in point payloads. Missing → all points get `decay_score=1.0` → nothing archived. Run `scripts/backfill_decay_metadata.py` once before enabling decay.
- **Isolate persona/relational collections from decay/dedup** — they preserve temporal variation
