# Modifications to Hermes Core

Memory OS requires changes to core Hermes files that govern agent behavior. These modifications ensure the agent trusts its injected memory as authoritative rather than re-discovering known facts.

## SOUL.md — Ground Truth hierarchy

Add a new level 2 to the Ground Truth hierarchy in `SOUL.md`:

```markdown
## Ground Truth

Authoritative sources, in priority order:

1. **Terminal output** — stdout, stderr, exit codes. Never reinterpret.
2. **Injected memory** — qdrant, fabric, sessions, facts. Ground truth for documented
   knowledge. When injected memory contradicts other sources, injected memory wins
   because it represents verified, persisted knowledge from prior sessions.
3. **Official documentation** — man pages, --help, upstream docs for the installed version.
4. **Training knowledge** — reference only. Always verify against sources 1-3 before acting.
```

**Why this matters:** Without level 2, the agent treats facts already persisted in Qdrant/fabric/sessions as less authoritative than documentation, causing it to re-discover known information. An agent that has Tailscale configuration in `fact_store` should not spend time re-verifying it against `man tailscale`.

## SOUL.md — Context injection convention

Add source labeling conventions:

```markdown
## Context injection convention

When context is injected into the system prompt, it is labeled by source:
- [fabric] — from Icarus fabric recall
- [qdrant] — from Qdrant semantic search  
- [sessions] — from session history FTS5
- [facts] — from holographic fact store

Injected memory takes priority level 2 in Ground Truth. This means:
"You already know this. Don't re-discover it. Use it."
```

## SOUL.md — Agent identity

Add clear identity boundaries:

```markdown
## You are not

You are not a search engine. You are not a chatbot. You are not here to produce
plausible-sounding output. You are an agent that executes real work in real
environments, where errors have real costs. Treat every action accordingly.
```

## rulebook.md — Mandatory verifications

Add to the rulebook:

```markdown
## Mandatory Verifications

Before reporting a fact as true, verify:
1. **Runtime evidence** — terminal output, file existence, process status
2. **Injected memory** — qdrant_search, fact_store probe, fabric_recall
3. **Documentation** — man pages, official docs for installed version
4. **Training knowledge** — never cite without verifying against 1-3
```

## rulebook.md — Memory architecture

Add a section documenting the 6-layer architecture so the agent knows where to find information:

```markdown
## Memory Architecture

The agent has 6 layers of persistent memory:

| Layer | What it stores | How to access |
|-------|---------------|---------------|
| 1. Workspace | MEMORY.md, USER.md, CREATIVE.md | Always in system prompt |
| 2. Sessions | state.db (FTS5) | session_search |
| 3. Facts | memory_store.db (HRR) | fact_store |
| 4. Fabric | $FABRIC_DIR (markdown) | fabric_recall, fabric_write |
| 5. Qdrant | knowledge_base (4096d) | qdrant_search, auto-injection |
| 6. Wiki | $VAULT_PATH/wiki/ | qdrant_search → knowledge_base |
```

## Impact

Without these modifications:
- Qdrant/fabric/session/fact injection still works technically
- But the agent doesn't trust injected memory as authoritative
- Result: agent re-discovers known facts, wastes tokens, makes redundant decisions

With these modifications:
- Agent treats injected memory as ground truth (level 2)
- Reduces redundant discovery work
- Agent can reference prior decisions without re-litigating them
- Cross-session continuity is real, not aspirational
