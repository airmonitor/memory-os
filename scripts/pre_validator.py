#!/usr/bin/env python3
"""
Semantic Pre-Validator — Decision linter based on the knowledge_base.
Queries the vault before I/O actions or API calls.

Usage:
  python3 pre_validator.py "POST to Qdrant upsert"                      # should find pitfalls
  python3 pre_validator.py --json "use Claude from Anthropic"            # JSON output
  python3 pre_validator.py --domain qdrant,api "modify docker-compose"   # restrict search

Exit codes:
  0 = pass/warn  (action may proceed)
  1 = blocked    (action must be aborted)

Fail-open: if OpenRouter or Qdrant is offline, allows execution with a warning.
"""

import sys
import json
import re
import requests
from typing import List, Dict, Optional
from pathlib import Path

# ─── Config (config/services.yaml) ──────────────────────────────────────────
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from memos_config import config  # noqa: E402

LITELLM_URL = config.litellm.base_url.rstrip("/")
LITELLM_KEY = config.litellm.api_key or ""
EMBEDDING_MODEL = config.litellm.models.embedding.name
EMBEDDING_DIMS = int(config.litellm.models.embedding.dimensions)

QDRANT_URL = config.qdrant.url
COLLECTION = config.qdrant.collection

TOP_K = 5
SCORE_THRESHOLD = 0.60
WARN_THRESHOLD = 0.75          # pure wiki docs need a higher score for a warning
BLOCK_SEVERITIES = {"critical", "high"}
WARN_SEVERITIES  = {"medium"}
RULE_SOURCES = {"reflection", "decision", "rule", "pitfall", "insight"}
REQUEST_TIMEOUT = int(config.search.request_timeout)

# ─── Restriction Patterns in wiki text ─────────────────────────────────────
RESTRICTION_KEYWORDS = [
    "do not use", "must not", "cannot", "never use", "avoid",
    "forbidden", "not recommended", "anti-pattern", "common mistake",
    "caution", "warning", "important:", "⚠️", "🚫",
    "must use", "must always", "requires", "mandatory",
    "keep", "do not change", "do not modify", "freeze",
]

def contains_restriction(text: str) -> bool:
    """Check whether text contains restriction/decision patterns."""
    if not text:
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in RESTRICTION_KEYWORDS)

# ─── Domain Tag Inference ─────────────────────────────────────────────────
DOMAIN_PATTERNS = {
    "docker"     : ["docker", "compose", "container", "image", "dockerfile"],
    "qdrant"     : ["qdrant", "collection", "points", "upsert", "vector", "vectors", "embedding"],
    "redis"      : ["redis", "arq", "queue", "job", "worker", "broker"],
    "openrouter" : ["openrouter", "embedding", "api_key", "openai", "api_base", "model"],
    "hermes"     : ["hermes", "config.yaml", "skill", "cron", "gateway", "cli"],
    "wiki"       : ["wiki", "raw/", "ingest", "vault", "obsidian", "knowledge_base"],
    "webui"      : ["webui", "open-webui", "frontend", "chat", "rag"],
    "infra"      : ["deploy", "server", "systemd", "service", "port", "host"],
    "security"   : ["password", "secret", "token", "auth", "permission", "sudo"],
    "maas"       : ["maas", "memory", "cognitive", "agent"],
}

def infer_domain_tags(description: str) -> List[str]:
    d = description.lower()
    found = set()
    for domain, pats in DOMAIN_PATTERNS.items():
        if any(p in d for p in pats):
            found.add(domain)
    return sorted(found)

# ─── Core ───────────────────────────────────────────────────────────────────

def embed_text(text: str) -> Optional[List[float]]:
    """Generate embedding via LiteLLM. Fail-open: return None on any error."""
    try:
        headers = {"Content-Type": "application/json"}
        if LITELLM_KEY:
            headers["Authorization"] = f"Bearer {LITELLM_KEY}"
        r = requests.post(
            f"{LITELLM_URL}/embeddings",
            headers=headers,
            json={
                "model": EMBEDDING_MODEL,
                "input": text[:8000],
                "dimensions": EMBEDDING_DIMS,
            },
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()["data"][0]["embedding"]
    except Exception as e:
        print(f"[PV-ERROR] Embedding failed: {e}", file=sys.stderr)
        return None

def search_knowledge_base(vector: List[float], domain_tags: List[str]) -> List[Dict]:
    try:
        r = requests.post(
            f"{QDRANT_URL}/collections/{COLLECTION}/points/search",
            headers={"Content-Type": "application/json"},
            json={"vector": vector, "limit": TOP_K * 3, "with_payload": True},
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
        hits = []
        for item in r.json().get("result", []):
            pld = item.get("payload", {})
            src = str(pld.get("source", "")).lower()
            sev = str(pld.get("severity", pld.get("decision_severity", "low"))).lower()
            tags = [str(t).lower() for t in pld.get("tags", [])]
            score = item.get("score", 0)
            
            # If domain filters requested, require overlap
            if domain_tags:
                dom_low = [d.lower() for d in domain_tags]
                if not set(dom_low) & set(tags):
                    continue
            
            hits.append({
                "id"     : str(item.get("id", "")),
                "score"  : score,
                "title"  : pld.get("title", "Untitled"),
                "text"   : (pld.get("text", "") or "")[:400],
                "source" : src,
                "severity": sev,
                "tags"   : tags,
            })
        hits.sort(key=lambda x: x["score"], reverse=True)
        return hits[:TOP_K]
    except Exception as e:
        print(f"[PV-ERROR] Qdrant search failed: {e}", file=sys.stderr)
        return []

def is_rule_hit(hit: Dict) -> bool:
    """Return True if the hit contains an explicit rule (reflection/decision/rule/insight/pitfall)."""
    return any(s in hit["source"] for s in RULE_SOURCES)

def classify_hit(hit: Dict, action_desc: str) -> str:
    """
    Return hit category: 'block', 'warn', 'info', or 'none'.
    Considers both source=reflection/decision/rule and restriction patterns
    embedded in wiki document text.
    """
    sev = hit.get("severity", "low")
    is_rule = is_rule_hit(hit) or contains_restriction(hit.get("text", ""))
    score = hit.get("score", 0)
    
    # If text contains restriction, give it more weight
    restriction_bonus = 0.08 if contains_restriction(hit.get("text", "")) else 0
    effective_score = score + restriction_bonus
    
    # Proximity: if the action term (e.g. "POST") appears near a keyword in the text
    action_terms = set(action_desc.lower().split())
    text_lower = (hit.get("text", "") or "").lower()
    text_words = set(text_lower.split())
    proximity_match = len(action_terms & text_words) > 0
    
    # If restriction + proximity → elevate severity
    has_restriction = contains_restriction(hit.get("text", "")) and proximity_match
    
    if is_rule or has_restriction:
        if sev in BLOCK_SEVERITIES or (has_restriction and effective_score >= 0.65):
            return "block"
        elif sev in WARN_SEVERITIES or (has_restriction and effective_score >= SCORE_THRESHOLD):
            return "warn"
    
    # For normal wiki documents, only warn if score is very high
    if effective_score >= WARN_THRESHOLD:
        return "warn"
    if effective_score >= SCORE_THRESHOLD:
        return "info"
    return "none"

def validate_action(action_description: str, domain_tags: Optional[List[str]] = None) -> Dict:
    try:
        dom = domain_tags or infer_domain_tags(action_description)
        vec = embed_text(action_description)
        if vec is None:
            return {"status": "pass", "blocked": False, "message": "⚠️  Validator offline. Proceeding with caution.", "action": action_description}
        
        hits = search_knowledge_base(vec, dom)
        blockers = []
        warnings = []
        infos = []
        
        for h in hits:
            cat = classify_hit(h, action_description)
            if cat == "block":
                blockers.append(h)
            elif cat == "warn":
                warnings.append(h)
            elif cat == "info":
                infos.append(h)
        
        if blockers:
            lines = [f"🚫 ACTION BLOCKED — {len(blockers)} critical rule(s) in the vault:"]
            for b in blockers:
                lines.append(f"  • [{b['severity'].upper()}] {b['title']} (score: {b['score']:.2f})")
                lines.append(f"    {b['text'][:200]}...")
            lines.append("")
            lines.append("Override? Type 'force' (not recommended).")
            return {
                "status": "blocked", "blocked": True,
                "blockers": blockers, "warnings": warnings,
                "message": "\n".join(lines), "action": action_description, "domain": dom,
            }
        
        if warnings:
            lines = [f"⚠️  {len(warnings)} warning(s) found in the vault:"]
            for w in warnings:
                lines.append(f"  • [{w['severity'].upper()}] {w['title']} (score: {w['score']:.2f})")
                lines.append(f"    {w['text'][:200]}...")
            return {
                "status": "warn", "blocked": False,
                "warnings": warnings, "infos": infos,
                "message": "\n".join(lines), "action": action_description, "domain": dom,
            }
        
        if infos:
            return {
                "status": "info", "blocked": False,
                "infos": infos,
                "message": f"ℹ️  {len(infos)} relevant document(s), none critical.",
                "action": action_description, "domain": dom,
            }
        
        return {
            "status": "pass", "blocked": False,
            "message": "No relevant insights found. Execution authorized.",
            "action": action_description, "domain": dom,
        }
    except Exception as e:
        return {
            "status": "pass", "blocked": False,
            "message": f"Validator failed ({e}). Proceeding with caution.",
            "action": action_description, "domain": [],
        }

# ─── Main ───────────────────────────────────────────────────────────────────
def main():
    import argparse
    p = argparse.ArgumentParser(description="Semantic Pre-Validator")
    p.add_argument("action", nargs="?", help="Action description")
    p.add_argument("--domain", help="Comma-separated domain tags")
    p.add_argument("--json", action="store_true", help="JSON output")
    p.add_argument("--silent", action="store_true", help="Silent — exit code only")
    p.add_argument("--force-block", action="store_true", help="Force block (testing)")
    args = p.parse_args()
    
    action = args.action or sys.stdin.read().strip() or "POST to Qdrant upsert endpoint"
    dom = [x.strip() for x in args.domain.split(",")] if args.domain else None
    
    res = validate_action(action, dom)
    if args.force_block:
        res["blocked"] = True
        res["status"] = "blocked"
    
    if args.json:
        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
    elif not args.silent:
        print(res["message"])
        if res["blocked"]:
            print("\n(Use --force-block to test validator bypass)")
    
    sys.exit(1 if res["blocked"] else 0)

if __name__ == "__main__":
    main()
