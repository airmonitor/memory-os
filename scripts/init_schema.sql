-- Memory OS — Postgres schema for Memory OS-owned tables.
-- Apply once on the target database:
--   psql -h <host> -U <user> -d memos -f scripts/init_schema.sql
-- Re-running is safe (IF NOT EXISTS guards).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── lineage ────────────────────────────────────────────────────────────────
-- Generation provenance: every context_enhancer query that produced a model
-- response. Originally state.db:lineage. Migrated for safe concurrent writes.

CREATE TABLE IF NOT EXISTS lineage (
    lineage_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id              TEXT NOT NULL,
    query                   TEXT NOT NULL,
    retrieved_chunk_ids     JSONB NOT NULL DEFAULT '[]'::jsonb,
    generation_model        TEXT NOT NULL DEFAULT 'unknown',
    generation_context_hash TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    query_tsv               tsvector GENERATED ALWAYS AS
                                (to_tsvector('simple', query)) STORED
);

CREATE INDEX IF NOT EXISTS lineage_session_idx
    ON lineage (session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS lineage_created_idx
    ON lineage (created_at DESC);
CREATE INDEX IF NOT EXISTS lineage_query_tsv_idx
    ON lineage USING GIN (query_tsv);
CREATE INDEX IF NOT EXISTS lineage_chunks_gin_idx
    ON lineage USING GIN (retrieved_chunk_ids jsonb_path_ops);

-- ── reflection_budget ──────────────────────────────────────────────────────
-- Hourly micro-reflection counter. Cross-process writer (host cron + Docker
-- worker) — primary motivation for migrating off SQLite.

CREATE TABLE IF NOT EXISTS reflection_budget (
    hour_window TEXT PRIMARY KEY,             -- "YYYY-MM-DDTHH" UTC
    count       INTEGER NOT NULL DEFAULT 0,
    tokens_used BIGINT  NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
