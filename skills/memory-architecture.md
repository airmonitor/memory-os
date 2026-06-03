---
name: memory-architecture
description: "Configure, maintain, and troubleshoot the Qdrant vector database layer of Memory OS. Covers collection setup, embedding pipeline, hybrid search, decay/archival, semantic dedup, fallback cascade, and all production pitfalls."
version: 1.0.0
triggers:
  - Setting up Qdrant from scratch
  - Debugging why Qdrant returns empty results
  - Configuring the decay scanner for the first time
  - Diagnosing why semantic dedup finds no duplicates
  - Understanding the 4-level fallback cascade
  - Adding a new embedding model
  - Migrating from unnamed to named vectors
  - Fixing Qdrant search that returns 400 errors
  - Tuning context injection thresholds
  - Verifying the ingestion pipeline is working
---

# Memory Architecture — Qdrant Vector Database

## Overview

Memory OS uses Qdrant as its vector database layer. The `knowledge_base` collection stores all wiki pages, session transcripts, and raw documents as dense (4096d Cosine) + sparse (BM25) vectors.

## Collection Schema

The collection uses **named vectors** — required by Qdrant 1.17+ for hybrid search:

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

**Critical:** Points must use `{"dense": [...], "sparse": SparseVector(...)}`. Unnamed vectors are rejected silently with no error logged.

### Creating the collection

In normal operation the ARQ worker calls `ensure_collection()` on first start, so this is rarely needed by hand. Manual command (substitute `$QDRANT_URL` and `$QDRANT_COLLECTION` from `config/services.yaml`):

```bash
curl -s -X PUT $QDRANT_URL/collections/$QDRANT_COLLECTION \
  -H "Content-Type: application/json" \
  -d '{
    "vectors": {
      "dense": { "size": 4096, "distance": "Cosine" }
    },
    "sparse_vectors": {
      "sparse": { "index": { "on_disk": false } }
    }
  }'
```

## Embedding Pipeline

| Step | Component | Details |
|------|-----------|---------|
| Dense embedding | LiteLLM-brokered model (default `rapid-mlx-qwen3-embedding-8b` → Qwen3-Embedding-8B) | 4096 dims, 32K context |
| Sparse embedding | FastEmbed BM25 (local) | Runs in-process, no API call |
| Queue | Valkey / Redis (ARQ) | Async job queue for ingestion |
| Worker | Docker (ARQ Worker) | Processes embedding + Qdrant upsert |

### Configuration alignment checklist

Two places must agree on the embedding dimensions:

1. `config/services.yaml` → `litellm.models.embedding.dimensions: 4096`
2. Qdrant collection `vectors.dense.size` → `4096`

The worker validates this on every embedding call (`docker/worker/services/embedding.py`) and raises if `len(vec) != dimensions`. Switching models is a YAML edit + collection rebuild.

**Batch embeddings aggressively.** OpenAI-compatible `/v1/embeddings` endpoints accept arrays. Sending 50 texts per request (instead of 1) reduces total time from ~4 hours to ~30-40 minutes for 30K items.

**Falsification experiment before scale.** Embed a pilot of 50-100 items, index them, verify search works, confirm payload structure, THEN scale.

## 4-Level Fallback Cascade

Every query goes through this cascade until one succeeds:

```
1. Hybrid: dense (4096d cosine) + sparse (BM25) → RRF fusion
2. Dense-only: if sparse fails → pure vector search
3. Lexical: if Qdrant offline → markdown file search in vault
4. Postgres FTS: if vault inaccessible → tsvector @@ to_tsquery over lineage.query
5. None: all exhausted → return empty (fail-open)
```

**Search via REST API (Qdrant 1.17):**

```bash
curl -s -X POST $QDRANT_URL/collections/$QDRANT_COLLECTION/points/search \
  -H "Content-Type: application/json" \
  -d '{
    "vector": {"name": "dense", "vector": [0.0156, ...]},
    "limit": 10,
    "with_payload": true
  }'
```

**Pitfall:** `POST /collections/{name}/points/query` (RRF fusion, hybrid prefetch) was introduced in Qdrant 1.18. On 1.17, it returns `400: Not existing vector name error`. Use `points/search` with named vectors instead.

## Decay Scanner

The decay scanner archives low-importance, aged AI content weekly. It checks every point's payload for three fields:

| Field | Default when missing | Effect |
|-------|---------------------|--------|
| `importance_score` | 0.5 | Controls half-life selection |
| `last_accessed_at` | `created_at` → `now_iso()` | **Missing means age=0, decay_score stays 1.0** |
| `confidence_score` | 1.0 | Falls into alert bucket, never archived |

### Decay formula

```
decay_score = exp(-ln(2) * age_days / half_life)
```

- `half_life = 90` days if `importance_score >= 0.3`
- `half_life = 30` days if `importance_score < 0.3`
- Archive if `decay_score < 0.1` AND `importance_score < 0.7`
- Alert (report, don't archive) if `decay_score < 0.1` AND `confidence >= 0.7`

**🚨 Half-life inversion bug:** A common mistake is setting `half_life = 30` for `importance >= 0.3` and `90` otherwise — making important chunks decay faster. Correct: higher importance → longer half-life.

**Exempt from decay:** Points with `source_type = human` or `source_type = procedural` are never archived.

### Backfill procedure

Run this once before enabling the decay scanner:

```bash
python3 scripts/backfill_decay_metadata.py --dry-run
# Review results, then:
python3 scripts/backfill_decay_metadata.py --commit
```

Do NOT remove `--dry-run` from the crontab until backfill is complete and shows real archival behavior.

## Semantic Dedup

The dedup scanner runs monthly. It finds near-duplicate points (cosine > 0.92) and merges them:

- Tags: union of both points
- `source_type`: priority (human > procedural > ai)
- `last_accessed_at`: max of both
- `importance_score`: max of both
- `lineage_ids`: union

**Pitfall:** Brute-force pairwise comparison is O(n²). For collections over 5000 points, use `--max-points` to limit the batch or use Qdrant's built-in nearest-neighbor search per point.

## Context Injection Thresholds (Icarus pre_llm_call)

The Icarus plugin injects relevant context before every LLM turn. Four sources, each gated:

| Source | Threshold | Top-K | Label |
|--------|-----------|-------|-------|
| Fabric | overlap > 0.85 → skip | 3 | `[fabric]` |
| Qdrant | threshold ≥ 0.55 | 2 | `[qdrant]` |
| Sessions (FTS5) | tokens ≥ 4 chars | 2 | `[sessions]` |
| Facts | first turn only | 3 | `[facts]` |

**Overlap gate:** Was set to 0.6 originally — this suppressed ALL injection from turn 2 onward in long single-topic sessions. The agent went blind to Qdrant/Fabric/sessions midway through focused conversations. Raised to 0.85: only suppress near-literal repetition.

**Social closer gate:** Messages shorter than 6 chars, emoji-only, or in the social closers set skip Qdrant/sessions/facts search entirely:

```python
_SOCIAL_CLOSERS = frozenset({
    "ok", "thanks", "yes", "no", "👍", "👌", "✅", "done", "got it"
})
```

**Per-session dedup:** Module-level sets reset on `on_session_start` prevent the same result from appearing twice in one session. Dedup key: prefer the entry/point id; fall back to a stable content slice.

## Key Pitfalls

### DeepSeek + `response_format: json_object` → `content: null`

**🚨 Applies to ALL DeepSeek models via OpenRouter.** When OpenRouter passes `response_format: {"type": "json_object"}`, DeepSeek returns `content: null` — a provider-level incompatibility.

**Fix:** Remove `response_format` from the payload entirely. Use a prompt instruction ("Return ONLY valid JSON array, no other text") and a robust parser:

```python
def _parse_json_robust(raw):
    """Extract JSON from LLM output with markdown tolerances."""
    if not raw or not raw.strip():
        return None
    text = raw.strip()
    for fence in ("```json", "```"):
        if text.startswith(fence):
            text = text[len(fence):].lstrip()
        if text.endswith("```"):
            text = text[:-3].rstrip()
    for start_char in ("[", "{"):
        idx = text.find(start_char)
        if idx != -1:
            text = text[idx:]
            break
    for _ in range(20):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            if text:
                text = text[:-1]
    return None
```

### AsyncQdrantClient.search() does not exist

`qdrant-client` 1.17+ async client lacks `.search()`. Use REST API directly via `httpx.AsyncClient` posting to `/collections/{name}/points/search`.

### Batch payload update uses /points/batch, NOT /points/payload

For heterogeneous operations (different payload per point group), use `POST /collections/{name}/points/batch` with `{"operations": [{"set_payload": {"payload": {...}, "points": [...]}}]}`. The `/points/payload` endpoint only serves uniform payload across all points.

### qdrant-client v1.18+ BREAKING: search() → query_points()

See the official Qdrant migration guide. The async client replaces `.search()` with `.query_points()` and changes return types.

### Python environment mismatch with FastEmbed/ONNX Runtime

FastEmbed depends on `onnxruntime` which loads compiled NumPy extensions. Cross-version `sys.path.insert` triggers `ModuleNotFoundError: No module named 'numpy._core._multiarray_umath'`. Always execute scripts directly with the venv's Python binary: `/path/to/venv/bin/python script.py`.

### RRF score threshold

Dense scores range ~0.5–0.95, RRF scores range ~0.1–0.6. A `score_threshold` of 0.55 silently drops everything from a hybrid RRF query. Use `score_threshold ≈ 0.15` for RRF.

### Forced segment merge

Qdrant's automatic optimizer won't merge segments below `indexing_threshold` (10K points). To reduce segment count on small collections (<10K pts), temporarily set `default_segment_number: 1` via `PATCH /collections/{name}`, wait for background merge, then restore to `0`.
