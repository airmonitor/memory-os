"""Async Postgres pool for the ARQ worker.

Lazy singleton: created on first `get_pool()` call (typically from
`main.startup()`). Bounded to config.postgres.pool.{min_size,max_size}.

Closed via `close_pool()` in `main.shutdown()`.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import asyncpg

# Walk up until we find memos_config/loader.py (container: /app, dev: repo root)
_here = Path(__file__).resolve()
for _candidate in (_here.parent, *_here.parents):
    if (_candidate / "memos_config" / "loader.py").exists():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

from memos_config import config  # noqa: E402

logger = logging.getLogger("cognitive-worker.db")

_pool: Optional[asyncpg.Pool] = None


def _dsn() -> str:
    pg = config.postgres
    pw = pg.password or ""
    return f"postgresql://{pg.user}:{pw}@{pg.host}:{pg.port}/{pg.database}"


async def get_pool() -> asyncpg.Pool:
    """Return the singleton pool, creating it on first call."""
    global _pool
    if _pool is None:
        pg = config.postgres
        _pool = await asyncpg.create_pool(
            dsn=_dsn(),
            min_size=pg.pool.min_size,
            max_size=pg.pool.max_size,
        )
        logger.info(
            f"Postgres pool ready: {pg.host}:{pg.port}/{pg.database} "
            f"(min={pg.pool.min_size} max={pg.pool.max_size})"
        )
    return _pool


async def close_pool() -> None:
    """Close the pool if it exists. Idempotent."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Postgres pool closed")
