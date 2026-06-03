# Layer 4 — Fabric (Cross-Session Memory)

> **Plugin:** Icarus ([bundled with Memory OS](../icarus/))
> **Storage:** `$FABRIC_DIR` (markdown files with YAML frontmatter)
> **Tools:** 16 (fabric_recall, fabric_write, fabric_brief, etc.)
> **Hooks:** 4 (on_session_start, pre_llm_call, post_llm_call, on_session_end)

## What it stores

Structured, cross-session entries — each session end produces one or more markdown files:

```markdown
---
id: "29914be5"
type: "resolution"
summary: "Fixed MEMORY.md corruption from dual-writer conflict"
training_value: "high"
status: "completed"
---
## Context
...
## Action/Decision
...
## Outcome
...
```

**Entry types:** decision, resolution, note, code-session, session, review, research, task

## How the agent uses it

| Tool | What it does |
|------|-------------|
| `fabric_recall` | Ranked retrieval from shared memory |
| `fabric_write` | Write entries with linking, evidence, and handoff fields |
| `fabric_search` | Keyword grep across all entries |
| `fabric_pending` | Show work assigned to this agent |
| `fabric_brief` | Daily brief: pending work, recent activity |
| `fabric_curate` | Set training value (high/normal/low) |
| `fabric_export` | Export training pairs for fine-tuning |
| `fabric_train` | Start fine-tune job on Together AI |
| `fabric_models` | List trained replacement models |

## Key enhancements over upstream

| Enhancement | What it fixes |
|-------------|---------------|
| **LLM-powered extraction** | Replaces upstream's `text[:500]` truncation with structured JSON extraction via OpenRouter |
| **Multi-source context injection** | Qdrant + sessions + facts injected automatically (upstream: fabric only) |
| **MEMORY.md → CREATIVE.md** | Fixes `§` delimiter corruption from dual-writer conflict |
| **Backtick sanitization** | Prevents orphaned backticks in learning lines |
| **System injection filter** | Prevents orchestrator preambles from being captured as tasks |
| **Social closer detection** | Skips trivial messages — avoids wasting embeddings on small talk |

See the [bundled Icarus source](../icarus/) for full details.

## Context injection flow

```
pre_llm_call(user_message):
  ├── _is_social_close(message)?
  │     └── Yes → skip all search-based injection
  ├── fabric_recall(query) → [fabric] results
  ├── _search_qdrant(query, threshold=0.55) → [qdrant] results
  ├── _search_sessions(query) → [sessions] results
  └── _search_facts(query) → [facts] results (first turn only)

Per-source dedup:
  _injected_fabric, _injected_qdrant, _injected_sessions
  → Reset on session start
  → Prevents same result injected twice in one session
```

## Configuration

Service hosts/models live in `config/services.yaml`; secrets in `.env`.

```bash
# .env
FABRIC_DIR=/absolute/path/to/fabric
LITELLM_API_KEY=sk-...

# Optional overrides (see config/services.yaml for defaults)
EXTRACTION_MODEL=lm-studio-qwen3.6
ICARUS_EXTRACTION_MAX_TOKENS=4096

# Optional (for Obsidian integration)
ICARUS_OBSIDIAN=1
OBSIDIAN_VAULT_PATH=/absolute/path/to/vault
```

```yaml
# config/services.yaml
litellm:
  base_url: https://litellm.airmonitor.pl/v1
  api_key: ${LITELLM_API_KEY}
  models:
    extraction:
      name: ${EXTRACTION_MODEL:lm-studio-qwen3.6}
      max_tokens: ${ICARUS_EXTRACTION_MAX_TOKENS:4096}
```

## Pitfalls

- **Config is loaded once at import time** — changing `config/services.yaml` or `.env` requires gateway restart (or `reload_config()` in interactive use)
- **`response_format: json_object` may produce `content: null`** depending on the model behind LiteLLM — the fork uses prompt-based JSON + `_parse_json_robust()` instead
- **`FABRIC_DIR` must be absolute path** — systemd does not expand `~`
- **Obsidian is optional** — Icarus writes plain markdown, Obsidian just reads it
- **Gateway restart required** after editing `hooks.py` or changing env vars
