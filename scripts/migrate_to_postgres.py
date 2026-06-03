#!/usr/bin/env python3
"""One-shot migration: copy `lineage` + `reflection_budget` from SQLite to Postgres.

Idempotent — safe to re-run. Does NOT delete from SQLite source. Source path comes
from config.paths.state_db (override via $STATE_DB_PATH).

Usage:
    python scripts/migrate_to_postgres.py
    python scripts/migrate_to_postgres.py --dry-run
    python scripts/migrate_to_postgres.py --source /custom/path/state.db
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

# Make repo root importable so `memos_config` resolves when running from anywhere
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import psycopg
from psycopg.types.json import Jsonb

from memos_config import config

BATCH = 1000


def _pg_dsn() -> str:
    pg = config.postgres
    pw = pg.password or ""
    return (
        f"postgresql://{pg.user}:{pw}@{pg.host}:{pg.port}/{pg.database}"
    )


def _open_sqlite(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _has_table(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def migrate_lineage(sqlite_con: sqlite3.Connection, pg_con: psycopg.Connection, dry_run: bool) -> dict:
    if not _has_table(sqlite_con, "lineage"):
        return {"table": "lineage", "skipped_reason": "no source table"}

    src = sqlite_con.execute(
        "SELECT lineage_id, session_id, query, retrieved_chunk_ids, "
        "generation_model, generation_context_hash, created_at FROM lineage"
    )

    migrated = skipped = total = 0
    batch: list[tuple] = []

    with pg_con.cursor() as cur:
        for row in src:
            total += 1
            try:
                chunk_ids = json.loads(row["retrieved_chunk_ids"]) if row["retrieved_chunk_ids"] else []
            except (TypeError, ValueError):
                chunk_ids = []
            batch.append((
                row["lineage_id"],
                row["session_id"],
                row["query"] or "",
                Jsonb(chunk_ids),
                row["generation_model"] or "unknown",
                row["generation_context_hash"] or "",
                row["created_at"],
            ))
            if len(batch) >= BATCH:
                m, s = _flush_lineage(cur, batch, dry_run)
                migrated += m
                skipped += s
                batch.clear()

        if batch:
            m, s = _flush_lineage(cur, batch, dry_run)
            migrated += m
            skipped += s

    if not dry_run:
        pg_con.commit()

    return {"table": "lineage", "total": total, "migrated": migrated, "skipped": skipped}


def _flush_lineage(cur, batch: list[tuple], dry_run: bool) -> tuple[int, int]:
    if dry_run:
        return len(batch), 0
    cur.executemany(
        """
        INSERT INTO lineage
            (lineage_id, session_id, query, retrieved_chunk_ids,
             generation_model, generation_context_hash, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (lineage_id) DO NOTHING
        """,
        batch,
    )
    inserted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    return inserted, len(batch) - inserted


def migrate_reflection_budget(sqlite_con: sqlite3.Connection, pg_con: psycopg.Connection, dry_run: bool) -> dict:
    if not _has_table(sqlite_con, "reflection_budget"):
        return {"table": "reflection_budget", "skipped_reason": "no source table"}

    rows = sqlite_con.execute(
        "SELECT hour_window, count, tokens_used FROM reflection_budget"
    ).fetchall()

    if dry_run:
        return {"table": "reflection_budget", "total": len(rows), "migrated": len(rows), "skipped": 0, "dry_run": True}

    with pg_con.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO reflection_budget (hour_window, count, tokens_used)
            VALUES (%s, %s, %s)
            ON CONFLICT (hour_window) DO UPDATE
            SET count = EXCLUDED.count,
                tokens_used = EXCLUDED.tokens_used,
                updated_at = now()
            """,
            [(r["hour_window"], r["count"] or 0, r["tokens_used"] or 0) for r in rows],
        )
    pg_con.commit()
    return {"table": "reflection_budget", "total": len(rows), "migrated": len(rows), "skipped": 0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", help="SQLite source path (default: config.paths.state_db)")
    parser.add_argument("--dry-run", action="store_true", help="Read but don't write")
    args = parser.parse_args()

    source = Path(args.source or config.paths.state_db)
    if not source.exists():
        print(f"[migrate] source SQLite missing: {source} — fresh install, nothing to migrate")
        return 0

    print(f"[migrate] source: {source}")
    print(f"[migrate] target: {config.postgres.host}:{config.postgres.port}/{config.postgres.database}")
    if args.dry_run:
        print("[migrate] DRY RUN — no writes")

    sqlite_con = _open_sqlite(source)
    try:
        with psycopg.connect(_pg_dsn(), autocommit=False) as pg_con:
            r1 = migrate_lineage(sqlite_con, pg_con, args.dry_run)
            r2 = migrate_reflection_budget(sqlite_con, pg_con, args.dry_run)
    finally:
        sqlite_con.close()

    for r in (r1, r2):
        print(f"[migrate] {r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
