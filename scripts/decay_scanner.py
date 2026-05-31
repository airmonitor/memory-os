#!/usr/bin/env python3
"""
decay_scanner.py
Selective archiving script for low-importance AI-generated chunks.
Runs via weekly cron (0 3 * * 0).

Rules:
- source_type in ["human", "procedural"] → exempt (never archive)
- importance_score >= 0.7 → exempt
- archived == True → skip (already archived)
- half_life: 90d if importance_score >= 0.3, else 30d
- decay_score < 0.1:
  - If confidence_score >= 0.7 → alert (report, don't archive)
  - Otherwise → archive (archived = True)
- gabi_* collections are completely ignored

Usage:
  python3 decay_scanner.py [--collection knowledge_base_hybrid] [--dry-run]
"""

import os
import sys
import json
import math
import argparse
import requests
from datetime import datetime, timezone
from pathlib import Path

# ─── Config ────────────────────────────────────────────────────────────────
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "knowledge_base")
SCROLL_LIMIT = 100  # Qdrant pagination
LOG_DIR = Path(os.environ.get("HERMES_LOGS_DIR", str(Path.home() / ".hermes" / "logs")))
LOG_FILE = LOG_DIR / "decay_scanner.log"

# ─── Helpers ──────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def calculate_decay_score(last_accessed_at: str, importance_score: float) -> float:
    """
    Calculate exponential decay: score = exp(-ln(2) * age_days / half_life).
    More important chunks persist longer (larger half-lives).
    """
    try:
        last = datetime.fromisoformat(last_accessed_at.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        # If timestamp is invalid, assume now (hasn't decayed yet)
        return 1.0
    
    now = datetime.now(timezone.utc)
    age_days = max(0, (now - last).total_seconds() / 86400)
    
    # Fix: LARGER half-life for more important chunks
    if importance_score >= 0.3:
        half_life = 90  # medium/high chunks → 90 days
    else:
        half_life = 30  # low chunks → 30 days
    
    decay_score = math.exp(-math.log(2) * age_days / half_life)
    return decay_score


def ensure_log_dir():
    """Create log directory if it doesn't exist."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def log_message(msg: str):
    """Log to stdout and append to log file."""
    ts = now_iso()
    line = f"[{ts}] {msg}"
    print(line)
    ensure_log_dir()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ─── Qdrant Operations ────────────────────────────────────────────────────

def scroll_chunks(collection: str, limit: int = SCROLL_LIMIT):
    """
    Generator that iterates over all points in the collection via scroll.
    Avoids loading the entire collection into memory.
    """
    offset = None
    total_scanned = 0
    
    while True:
        payload = {
            "limit": limit,
            "with_payload": True,
            "with_vector": False,
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
                yield point
                total_scanned += 1
            
            offset = result.get("next_page_offset")
            if offset is None:
                break
                
        except Exception as e:
            log_message(f"❌ Qdrant scroll error: {e}")
            break
    
    log_message(f"📊 Total chunks scanned: {total_scanned}")


def update_point_archived(point_id: str, collection: str, decay_score: float, dry_run: bool = False):
    """Update point payload: archived=True + calculated decay_score."""
    if dry_run:
        log_message(f"  [DRY-RUN] Would archive point {point_id} (decay_score={decay_score:.4f})")
        return True
    
    try:
        resp = requests.post(
            f"{QDRANT_URL}/collections/{collection}/points/payload",
            headers={"Content-Type": "application/json"},
            json={
                "points": [point_id],
                "payload": {
                    "archived": True,
                    "decay_score": decay_score,
                },
            },
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log_message(f"  ❌ Failed to archive point {point_id}: {e}")
        return False


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Decay Scanner — Selective chunk archiving")
    parser.add_argument("--collection", default=COLLECTION, help="Qdrant collection name")
    parser.add_argument("--dry-run", action="store_true", help="Simulation — does not modify anything")
    parser.add_argument("--threshold", type=float, default=0.1, help="Decay threshold for archiving")
    args = parser.parse_args()
    
    collection = args.collection
    
    # Ignore gabi_* collections
    if collection.startswith("gabi_"):
        log_message(f"⏭️ Collection '{collection}' is exempt (gabi_*). Exiting.")
        return
    
    log_message(f"🚀 Starting decay scanner (collection={collection}, threshold={args.threshold}, dry_run={args.dry_run})")
    
    # Metrics
    stats = {
        "scanned": 0,
        "archived": 0,
        "alerted": 0,
        "skipped_human": 0,
        "skipped_procedural": 0,
        "skipped_high_importance": 0,
        "skipped_already_archived": 0,
        "failed": 0,
    }
    
    alerts = []  # List of alerts (decay < threshold but confidence >= 0.7)
    
    for point in scroll_chunks(collection):
        stats["scanned"] += 1
        
        point_id = point.get("id")
        payload = point.get("payload", {})
        
        source_type = payload.get("source_type", "unknown")
        importance_score = payload.get("importance_score", 0.5)
        archived = payload.get("archived", False)
        last_accessed_at = payload.get("last_accessed_at", payload.get("created_at", now_iso()))
        confidence_score = payload.get("confidence_score", 1.0)
        
        # Skip: already archived
        if archived:
            stats["skipped_already_archived"] += 1
            continue
        
        # Skip: human (exempt)
        if source_type == "human":
            stats["skipped_human"] += 1
            continue
        
        # Skip: procedural (exempt)
        if source_type == "procedural":
            stats["skipped_procedural"] += 1
            continue
        
        # Skip: high importance
        if importance_score >= 0.7:
            stats["skipped_high_importance"] += 1
            continue
        
        # Calculate decay
        decay_score = calculate_decay_score(last_accessed_at, importance_score)
        
        # Check threshold
        if decay_score < args.threshold:
            # Decay-confidence rule: if confidence is high, alert instead of archiving
            if confidence_score >= 0.7:
                stats["alerted"] += 1
                alerts.append({
                    "point_id": point_id,
                    "decay_score": round(decay_score, 4),
                    "confidence_score": round(confidence_score, 2),
                    "importance_score": round(importance_score, 2),
                    "age_days": round((datetime.now(timezone.utc) - datetime.fromisoformat(last_accessed_at.replace("Z", "+00:00"))).total_seconds() / 86400, 1),
                    "reason": "decay < threshold but confidence >= 0.7 — manual review recommended",
                })
                log_message(f"  ⚠️ ALERT: point {point_id} (decay={decay_score:.4f}, confidence={confidence_score:.2f}) — manual review recommended")
            else:
                # Archive
                ok = update_point_archived(point_id, collection, decay_score, args.dry_run)
                if ok:
                    stats["archived"] += 1
                    log_message(f"  📦 Archived: point {point_id} (decay={decay_score:.4f}, importance={importance_score:.2f})")
                else:
                    stats["failed"] += 1
    
    # Structured JSON report
    report = {
        "timestamp": now_iso(),
        "collection": collection,
        "threshold": args.threshold,
        "dry_run": args.dry_run,
        "scanned": stats["scanned"],
        "archived": stats["archived"],
        "alerted": stats["alerted"],
        "skipped_human": stats["skipped_human"],
        "skipped_procedural": stats["skipped_procedural"],
        "skipped_high_importance": stats["skipped_high_importance"],
        "skipped_already_archived": stats["skipped_already_archived"],
        "failed": stats["failed"],
        "alerts": alerts,
    }
    
    log_message("=" * 60)
    log_message("📊 DECAY SCANNER REPORT")
    log_message("=" * 60)
    log_message(f"  Scanned:                {stats['scanned']}")
    log_message(f"  Archived:               {stats['archived']}")
    log_message(f"  Alerts (decay+conf.):   {stats['alerted']}")
    log_message(f"  Skipped human:          {stats['skipped_human']}")
    log_message(f"  Skipped procedural:     {stats['skipped_procedural']}")
    log_message(f"  Skipped high imp.:      {stats['skipped_high_importance']}")
    log_message(f"  Skipped archived:       {stats['skipped_already_archived']}")
    log_message(f"  Failures:               {stats['failed']}")
    log_message("=" * 60)
    
    # JSON report to stderr (parseable)
    print(json.dumps(report, ensure_ascii=False, indent=2), file=sys.stderr)
    
    log_message("✅ Decay scanner complete.")


if __name__ == "__main__":
    main()
