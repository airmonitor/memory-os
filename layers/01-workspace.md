# Layer 1 — Workspace Memory

> **Files:** `MEMORY.md`, `USER.md`, `CREATIVE.md` in `$HERMES_HOME/memories/`
> **Injection:** System prompt, every turn
> **Persistence:** Markdown on disk

## What it stores

| File | Writer | Format | Purpose |
|------|--------|--------|---------|
| **MEMORY.md** | `memory` tool | `§`-delimited entries | Agent's durable memory: environment facts, tool quirks, project conventions |
| **USER.md** | Manual (user) | Markdown | Static user profile: who the user is, preferences, workflow |
| **CREATIVE.md** | Icarus plugin (`state.py`) | Markdown headers + bullets | Agent's creative state: learnings, open questions, cycle counter |

## The `§` delimiter story

The `memory` tool uses `§` (paragraph sign, U+00A7) as an entry delimiter in MEMORY.md:

```
Entry about Qdrant named vectors
§
Entry about Tailscale configuration
§
Entry about project conventions
```

**What broke:** The Icarus plugin's `write_memory_file()` used to overwrite MEMORY.md with `.write_text()` on every session end, destroying all `§`-delimited entries. Two writers, one file, incompatible formats.

**Fix:** Icarus now writes to **CREATIVE.md** instead. Two writers, two files, zero conflicts. This fix is in our [Icarus fork](https://github.com/ClaudioDrews/icarus-plugin).

## Configuration

No additional configuration needed — these files are always injected by Hermes. To change injection behavior, edit `SOUL.md` or `rulebook.md`.

## Pitfalls

- **systemd does not expand `~`:** Paths in `.env` read by the gateway must use absolute paths
- **Never edit MEMORY.md manually:** Use `memory(action='add')` — the tool writes atomic `§`-delimited entries
- **Icarus conflict:** If MEMORY.md ever shows `cycles:` or markdown headers, Icarus overwrote it. Delete and restore from backup, then ensure Icarus is writing to CREATIVE.md
