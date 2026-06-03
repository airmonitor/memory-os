"""Postgres connection helpers for host-side scripts (sync, psycopg).

Short-lived scripts (cron, per-turn sync calls) — no pool, just per-call
`with psycopg.connect()`. Pool overhead would only add startup cost.

Usage:
    from scripts.db import get_conn
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM lineage")
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import psycopg

from memos_config import config


def build_dsn() -> str:
    """Assemble libpq DSN from config.postgres."""
    pg = config.postgres
    pw = pg.password or ""
    return f"postgresql://{pg.user}:{pw}@{pg.host}:{pg.port}/{pg.database}"


def get_conn(**kwargs) -> psycopg.Connection:
    """Open a fresh sync connection. Caller is responsible for closing.

    Pass `autocommit=True` for one-shot read queries to skip transaction overhead.
    """
    return psycopg.connect(build_dsn(), **kwargs)
