# Layer 6 — LLM Wiki

> **Location:** `$VAULT_PATH/wiki/`
> **Pipeline:** wiki-raw-ingest-monitor (scheduled) + wiki-continuous-ingest (hourly)
> **Qdrant:** All files ingested into `knowledge_base`

## Directory structure

```
$VAULT_PATH/wiki/
├── raw/              Source documents
│   ├── articles/      (analyses, dossiers, tutorials)
│   ├── releases/      (software release notes, PR trackers)
│   ├── projects/      (project architecture, reports)
│   └── ...            (any other document source)
├── concepts/          Extracted ideas and patterns
├── entities/          Concrete things (tools, models, projects, people)
├── comparisons/       Side-by-side analyses
├── _meta/             Schema, templates, taxonomy
├── _archive/          Deprecated pages
├── index.md           Master catalog with one-line summaries
├── SCHEMA.md          Constitution — what merits a page, how to link, what tags to use
└── log.md             Logbook — every curation session recorded
```

## The two pipelines

| Pipeline | Trigger | What it does |
|----------|---------|--------------|
| **Wiki Agent** (curation) | Scheduled cron | Reads `raw/` files, extracts concepts/entities/comparisons, creates structured wiki pages |
| **Continuous Ingest** (Qdrant) | Hourly cron | SHA-256 diff detection, embeds new/modified files, upserts to `knowledge_base` |

They're independent: the Wiki Agent builds the curated knowledge graph; Continuous Ingest ensures Qdrant stays in sync.

## Wiki Agent pipeline

```
1. Cron triggers wiki-raw-ingest-monitor (scheduled)
2. check_raw_ingest.py → detects new files in raw/
3. Agent reads SCHEMA.md, index.md, log.md
4. For each new file:
   a. LLM analyzes content
   b. Decides: concept? entity? comparison? skip?
   c. Creates page in appropriate directory
   d. Frontmatter: type, tags (from closed taxonomy), sources (linking back to raw/)
5. Updates index.md and log.md
6. Lint: validates frontmatter, wikilinks, index coverage
```

## Continuous Ingest pipeline

```
1. Cron triggers wiki-continuous-ingest (hourly)
2. wiki_continuous_ingest.py:
   a. Scans $VAULT_PATH/wiki/ for *.md files
   b. Computes SHA-256 hash for each file
   c. Compares with state file
   d. New or modified → enqueues ARQ job in Redis
3. ARQ Worker (Docker):
   a. process_wiki_file → reads file content
   b. parse_frontmatter → extracts metadata
   c. get_embedding() → Qwen3-Embedding-8B (4096d)
   d. get_sparse_embedding() → BM25 (fastembed, local)
   e. upsert_with_dedup() → Qdrant knowledge_base
```

**State file:** JSON file tracking `{file_path, sha256_hash, ingested_at}` for each indexed file. Prevents re-ingestion of unchanged files.

## Page quality standards

Every wiki page must have:
- Valid YAML frontmatter with `type`, `tags`, `sources`, `confidence`
- At least one wikilink to another wiki page
- A one-line summary (used in index.md)
- `sources` linking back to the raw file(s) it was extracted from

**Closed taxonomy:** Tags come from a fixed set of categories defined in SCHEMA.md. No ad-hoc tags allowed.

## Pitfalls

- **Wiki Agent and Vault Curator are different cronjobs** — don't conflate them. Wiki Agent creates pages; Vault Curator enriches frontmatter + adds semantic links to existing files
- **raw/ files are source material, not curated knowledge** — they feed the pipeline but aren't themselves structured wiki pages
- **SCHEMA.md is the constitution** — any change to page structure or taxonomy must be reflected there first
- **log.md is the audit trail** — if a page looks wrong, check log.md to see which session created it and why
