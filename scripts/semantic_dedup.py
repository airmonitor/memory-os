#!/usr/bin/env python3
"""
semantic_dedup.py
Monthly scanner for near-duplicates in knowledge_base_hybrid via cosine similarity.
Runs on the first Sunday of each month (cron: 0 3 1 * *).

⚠️  WARNING: This performs O(n²) brute-force pairwise comparisons. For large
    collections (e.g. 100K+ points), this can be extremely slow and memory-heavy.
    Use --max-points to limit processing, or prefer Qdrant's built-in
    nearest-neighbor search on a random sample where feasible.

Rules:
- Ignores gabi_* collections
- Does not delete automatically — only emits a JSON report of candidates
- Similarity threshold: 0.92 (configurable)
- Merge is handled via upserts in file_ingestion.py (pre-write dedup)
- This script does the retrospective scan of the entire collection
  (capped by MAX_POINTS)

Usage:
  python3 semantic_dedup.py [--collection knowledge_base_hybrid] [--threshold 0.92] [--dry-run] [--max-points 5000]
"""

import os
import sys
import json
import math
import argparse
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# ─── Config (config/services.yaml) ──────────────────────────────────────────
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from memos_config import config  # noqa: E402

QDRANT_URL = config.qdrant.url
COLLECTION = config.qdrant.collection
SCROLL_LIMIT = 50  # Qdrant pagination (avoids timeout on large collections)
SIMILARITY_THRESHOLD = 0.92
TOP_NEIGHBORS = 10

LOG_DIR = Path(
    os.environ.get("HERMES_LOG_DIR", str(Path(config.paths.hermes_home) / "logs"))
)
LOG_FILE = LOG_DIR / "semantic_dedup.log"
REPORT_FILE = LOG_DIR / "semantic_dedup_report.json"

# Safety cap — limit processed points to avoid O(n²) blowup on large collections
MAX_POINTS = int(os.environ.get("DEDUP_MAX_POINTS", "5000"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_message(msg: str):
    ts = now_iso()
    line = f"[{ts}] {msg}"
    print(line)
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ─── Qdrant Operations ────────────────────────────────────────────────────

def scroll_all_chunks(collection: str) -> List[Dict]:
    """
    Load all points from the collection, paginating via scroll.
    Returns a list of {id, vector, payload}.
    """
    all_chunks = []
    offset = None
    scanned = 0

    while True:
        payload = {
            "limit": SCROLL_LIMIT,
            "with_payload": True,
            "with_vector": True,
        }
        if offset is not None:
            payload["offset"] = offset

        try:
            resp = requests.post(
                f"{QDRANT_URL}/collections/{collection}/points/scroll",
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            result = data.get("result", {})
            points = result.get("points", [])

            if not points:
                break

            for point in points:
                # Get only the dense vector for similarity
                vector = point.get("vector")
                dense = None
                if isinstance(vector, dict):
                    dense = vector.get("dense")
                elif isinstance(vector, list):
                    dense = vector  # fallback: simple vector

                if dense:
                    all_chunks.append({
                        "id": point.get("id"),
                        "vector": dense,
                        "payload": point.get("payload", {}),
                    })

            scanned += len(points)
            offset = result.get("next_page_offset")
            if offset is None:
                break

        except Exception as e:
            log_message(f"❌ Error in Qdrant scroll: {e}")
            break

    log_message(f"📊 Total chunks loaded: {len(all_chunks)} / {scanned} scanned")
    return all_chunks


def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    """Calculate cosine similarity between two vectors."""
    if len(v1) != len(v2):
        return 0.0

    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return dot / (norm1 * norm2)


def find_near_duplicates(chunks: List[Dict], threshold: float = SIMILARITY_THRESHOLD) -> List[Dict]:
    """
    Find near-duplicate pairs via brute-force cosine similarity.
    Optimization: upper-triangular matrix comparison.
    Returns list of {chunk_id_a, chunk_id_b, similarity}.
    """
    n = len(chunks)
    if n < 2:
        return []

    candidates = []
    ids_seen = set()  # avoid duplicates (A,B) and (B,A)

    for i in range(n):
        for j in range(i + 1, n):
            # Fast heuristic: skip if texts differ greatly in size
            text_len_i = len(chunks[i]["payload"].get("text", ""))
            text_len_j = len(chunks[j]["payload"].get("text", ""))
            if text_len_i > 0 and text_len_j > 0:
                ratio = min(text_len_i, text_len_j) / max(text_len_i, text_len_j)
                if ratio < 0.5:  # Very different sizes, skip
                    continue

            sim = cosine_similarity(chunks[i]["vector"], chunks[j]["vector"])
            if sim >= threshold:
                pair_key = tuple(sorted([str(chunks[i]["id"]), str(chunks[j]["id"])]))
                if pair_key not in ids_seen:
                    ids_seen.add(pair_key)
                    candidates.append({
                        "chunk_id_a": chunks[i]["id"],
                        "chunk_id_b": chunks[j]["id"],
                        "similarity": round(sim, 6),
                        "source_a": chunks[i]["payload"].get("source", "unknown"),
                        "source_b": chunks[j]["payload"].get("source", "unknown"),
                        "title_a": chunks[i]["payload"].get("title", "")[:60],
                        "title_b": chunks[j]["payload"].get("title", "")[:60],
                        "text_preview_a": chunks[i]["payload"].get("text", "")[:100],
                        "text_preview_b": chunks[j]["payload"].get("text", "")[:100],
                    })

    # Sort by descending similarity
    candidates.sort(key=lambda x: x["similarity"], reverse=True)
    return candidates


def generate_report(candidates: List[Dict], collection: str, threshold: float, scanned: int) -> Dict:
    """Generate structured JSON report."""
    return {
        "timestamp": now_iso(),
        "collection": collection,
        "threshold": threshold,
        "scanned_chunks": scanned,
        "near_duplicate_pairs": len(candidates),
        "candidates": candidates,
        "recommendation": (
            f"{len(candidates)} near-duplicate pairs found. "
            "Review manually and apply merge via Qdrant point update if approved."
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="Semantic Dedup Scanner")
    parser.add_argument("--collection", default=COLLECTION, help="Qdrant collection name")
    parser.add_argument("--threshold", type=float, default=SIMILARITY_THRESHOLD, help="Cosine similarity threshold")
    parser.add_argument("--max-points", type=int, default=MAX_POINTS, help="Max points to process (cap O(n²))")
    parser.add_argument("--dry-run", action="store_true", help="Scan only, do not save report")
    args = parser.parse_args()

    collection = args.collection

    # Skip gabi_* collections
    if collection.startswith("gabi_"):
        log_message(f"⏭️ Collection '{collection}' is exempt (gabi_*). Exiting.")
        return

    log_message(f"🚀 Starting semantic dedup (collection={collection}, threshold={args.threshold}, dry_run={args.dry_run})")

    # Load chunks (capped by --max-points to avoid O(n²) blowup)
    chunks = scroll_all_chunks(collection)
    if args.max_points and len(chunks) > args.max_points:
        log_message(f"⚠️  Collection has {len(chunks)} points, truncating to {args.max_points} (use --max-points to change)")
        chunks = chunks[:args.max_points]

    if not chunks:
        log_message("⚠️ No chunks found in the collection.")
        return

    # Find near-duplicates
    log_message(f"🔍 Analyzing similarity among {len(chunks)} chunks...")
    candidates = find_near_duplicates(chunks, threshold=args.threshold)

    # Generate report
    report = generate_report(candidates, collection, args.threshold, len(chunks))

    log_message("=" * 60)
    log_message("📊 SEMANTIC DEDUP REPORT")
    log_message("=" * 60)
    log_message(f"  Chunks scanned:          {report['scanned_chunks']}")
    log_message(f"  Near-duplicate pairs:    {report['near_duplicate_pairs']}")

    if candidates:
        log_message(f"  Top similarity:          {candidates[0]['similarity']:.4f}")
        log_message(f"  Top pair:                {candidates[0]['chunk_id_a']} ↔ {candidates[0]['chunk_id_b']}")
    else:
        log_message("  No near-duplicates found.")

    log_message("=" * 60)

    # Save JSON report
    if not args.dry_run and candidates:
        try:
            REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(REPORT_FILE, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            log_message(f"📄 Report saved: {REPORT_FILE}")
        except Exception as e:
            log_message(f"❌ Error saving report: {e}")

    # Output JSON to stderr (parseable)
    print(json.dumps(report, ensure_ascii=False, indent=2), file=sys.stderr)

    log_message("✅ Semantic dedup complete.")


if __name__ == "__main__":
    main()
