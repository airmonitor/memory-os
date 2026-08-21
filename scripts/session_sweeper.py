"""Extract finished conversation slices from Hermes' state.db.

Runs on a schedule (hermes cron --no-agent --script). See
docs/adr/0001-session-extraction-via-state-db-sweeper.md; the ordering
claim -> extract -> publish -> dispatch is the correctness argument.

`import memos_config` MUST precede anything vendored.
"""
from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from memos_config import config              # noqa: E402
from icarus import extraction, hermes_state  # noqa: E402
from icarus.state import write_entry as icarus_write_entry  # noqa: E402
from scripts import session_store            # noqa: E402
from scripts.db import get_conn               # noqa: E402

logger = logging.getLogger("session_sweeper")


@dataclass
class Deps:
    sqlite_conn: object
    pg: object
    extract: Callable[[str], list]
    write_entry: Callable[..., str]
    enqueue: Callable[..., str]
    now: Callable[[], float]


def job_id(session_id: str, last_message_id: int, index: int) -> str:
    return f"ingest:{session_id}:{last_message_id}:{index}"


def entry_suffix(session_id: str, last_message_id: int, index: int) -> str:
    raw = f"{session_id}:{last_message_id}:{index}".encode()
    return hashlib.sha256(raw).hexdigest()[:8]


def redispatch(deps: Deps) -> int:
    """Send jobs for slices that were extracted but never dispatched.

    This is the other half of the ordering guarantee. Publishing before
    dispatching means a broker outage cannot lose an entry — but only if
    something comes back for it. The payload was stored with the claim, so this
    costs no LLM call.
    """
    sent = 0
    for row in deps.pg.pending_dispatch():
        job_ids = []
        try:
            for item in row["payload"]:
                deps.enqueue("process_ingestion", item["text"], "session",
                             job_id=item["job_id"])
                job_ids.append(item["job_id"])
        except Exception as exc:
            logger.warning("re-dispatch of %s:%s failed: %s",
                           row["session_id"], row["last_message_id"], exc)
            continue
        deps.pg.mark_published(session_id=row["session_id"],
                               last_message_id=row["last_message_id"], jobs=job_ids)
        sent += 1
    return sent


def sweep(deps: Deps, cfg: dict) -> dict:
    now = deps.now()
    deps.pg.ensure_schema()
    stats = {"candidates": 0, "extracted": 0, "entries": 0, "jobs": 0,
             "redispatched": redispatch(deps)}
    marks = deps.pg.watermarks()

    with hermes_state.snapshot(deps.sqlite_conn):
        candidates = hermes_state.find_candidates(
            deps.sqlite_conn, now=now, idle_seconds=cfg["idle_seconds"],
            max_lag_seconds=cfg["max_lag_seconds"], watermarks=marks,
            min_messages=cfg["min_messages"], limit=cfg["max_per_run"])
        session_id_filter = cfg.get("session_id")
        if session_id_filter:
            candidates = [c for c in candidates if c.session_id == session_id_filter]
        stats["candidates"] = len(candidates)
        slices = []
        for cand in candidates:
            after = marks.get(cand.session_id, 0)
            messages = hermes_state.read_slice(deps.sqlite_conn, cand.session_id,
                                               after_id=after)
            if not messages:
                continue
            context = hermes_state.context_tail(deps.sqlite_conn, cand.session_id,
                                                before_id=after,
                                                limit=cfg["context_overlap"]) if after else []
            slices.append((cand, context, messages))

    for cand, context, messages in slices:
        first_id, last_id = messages[0].id, messages[-1].id
        if not deps.pg.claim(session_id=cand.session_id, first_message_id=first_id,
                             last_message_id=last_id, message_count=len(messages)):
            logger.info("slice %s:%s already claimed", cand.session_id, last_id)
            continue

        # Extraction phase: anything that goes wrong here leaves the row at
        # 'claimed', which watermarks() counts the same as any other non-failed
        # status — so a silent failure here would mean the slice is never
        # offered again. It must be marked FAILED instead, which both excludes
        # it from the watermark and makes it reclaimable by claim().
        try:
            exchanges = extraction.messages_to_exchanges(messages)
            score = extraction.score_exchanges(exchanges)
            if score["total"] < cfg["quality_threshold"]:
                deps.pg.mark_extracted(session_id=cand.session_id, last_message_id=last_id,
                                       entries=0, score=score["total"])
                deps.pg.mark_published(session_id=cand.session_id, last_message_id=last_id,
                                       jobs=[])
                continue
            entries = deps.extract(extraction.build_transcript(messages, context=context))
            stats["extracted"] += 1
            payload = [{"job_id": job_id(cand.session_id, last_id, i), "text": e["content"]}
                       for i, e in enumerate(entries)]
            deps.pg.mark_extracted(session_id=cand.session_id, last_message_id=last_id,
                                   entries=len(entries), score=score["total"],
                                   payload=payload)
        except Exception as exc:                    # one bad slice must not stop the sweep
            logger.warning("slice %s:%s failed during extraction: %s",
                           cand.session_id, last_id, exc)
            deps.pg.mark_failed(session_id=cand.session_id, last_message_id=last_id,
                                error=exc)
            continue

        # Dispatch phase: the slice is ALREADY 'extracted' and its payload is
        # already stored in Postgres by this point. A broker outage here must
        # NOT be marked failed — that would make the slice reclaimable and pay
        # for a second LLM call. Instead it stays 'extracted', and the next
        # sweep's redispatch() drains it from the stored payload at no
        # further LLM cost.
        try:
            job_ids = []
            for i, entry in enumerate(entries):
                deps.write_entry(entry_type=entry["type"], content=entry["content"],
                                 summary=entry["summary"], platform=cand.source or "cli",
                                 training_value=entry.get("training_value", "normal"),
                                 status="completed",
                                 suffix=entry_suffix(cand.session_id, last_id, i))
                stats["entries"] += 1
                jid = job_id(cand.session_id, last_id, i)
                deps.enqueue("process_ingestion", entry["content"], "session", job_id=jid)
                job_ids.append(jid)
                stats["jobs"] += 1
            deps.pg.mark_published(session_id=cand.session_id, last_message_id=last_id,
                                   jobs=job_ids)
        except Exception as exc:
            logger.warning("slice %s:%s failed during dispatch (will retry): %s",
                           cand.session_id, last_id, exc)
    return stats


# ─── CLI ─────────────────────────────────────────────────────────────────

def _load_cfg(cfg_module) -> dict:
    """Read session_extraction.* from config, or fall back to the ADR
    defaults. Task 9 adds the config block; until then this makes the CLI
    runnable on its own."""
    se = getattr(cfg_module, "session_extraction", None)

    def g(name, default):
        return getattr(se, name, default) if se is not None else default

    return dict(
        idle_seconds=int(g("idle_minutes", 90)) * 60,
        min_messages=int(g("min_messages", 4)),
        context_overlap=int(g("context_overlap", 4)),
        max_lag_seconds=int(g("max_lag_hours", 24)) * 3600,
        max_per_run=int(g("max_per_run", 3)),
        quality_threshold=float(g("quality_threshold", 0.2)),
    )


class _PgAdapter:
    """Binds scripts.session_store's free functions to one connection.

    session_store's functions take `conn` as their first positional argument;
    Deps.pg is called without one (see FakePg in tests/test_session_sweeper.py).
    """

    def __init__(self, conn):
        self.conn = conn

    def ensure_schema(self):
        return session_store.ensure_schema(self.conn)

    def watermarks(self):
        return session_store.watermarks(self.conn)

    def claim(self, **kw):
        return session_store.claim(self.conn, **kw)

    def mark_extracted(self, **kw):
        return session_store.mark_extracted(self.conn, **kw)

    def mark_published(self, **kw):
        return session_store.mark_published(self.conn, **kw)

    def mark_failed(self, **kw):
        return session_store.mark_failed(self.conn, **kw)

    def pending_dispatch(self):
        return session_store.pending_dispatch(self.conn)

    def record_run(self, **kw):
        return session_store.record_run(self.conn, **kw)


class _Enqueuer:
    """Sync-callable wrapper around one arq pool, for a script that otherwise
    has no event loop of its own. Opened once per CLI run, closed at the end."""

    def __init__(self, redis_settings):
        from arq import create_pool
        self._loop = asyncio.new_event_loop()
        self._pool = self._loop.run_until_complete(create_pool(redis_settings))

    def __call__(self, job, *args, job_id):
        return self._loop.run_until_complete(
            self._pool.enqueue_job(job, *args, _job_id=job_id))

    def close(self):
        self._loop.run_until_complete(self._pool.aclose())
        self._loop.close()


def _dry_run_stubs(deps: Deps, pg: _PgAdapter) -> None:
    """Log what would happen; write nothing — including to rows a PRIOR real
    run already created. `claim` returning True unconditionally only makes the
    new-candidate path a no-op (no row exists yet, so a later mark_* UPDATE
    would match nothing); it does NOT protect redispatch(), which reads
    genuinely existing 'extracted' rows via a real pending_dispatch(). Without
    also stubbing the mark_* calls, a dry run would silently flip those rows to
    'published' while enqueue_stub sends nothing — permanently losing slices a
    real run left mid-flight. So every write goes through the stub; only reads
    (watermarks, pending_dispatch, ensure_schema) stay real."""
    pg.claim = lambda **kw: True
    pg.mark_extracted = lambda **kw: logger.info(
        "[dry-run] would mark_extracted %s:%s", kw.get("session_id"), kw.get("last_message_id"))
    pg.mark_published = lambda **kw: logger.info(
        "[dry-run] would mark_published %s:%s", kw.get("session_id"), kw.get("last_message_id"))
    pg.mark_failed = lambda **kw: logger.info(
        "[dry-run] would mark_failed %s:%s", kw.get("session_id"), kw.get("last_message_id"))

    def extract_stub(transcript):
        logger.info("[dry-run] would extract from a %d-char transcript", len(transcript))
        return []

    def write_entry_stub(**kw):
        logger.info("[dry-run] would write fabric entry suffix=%s", kw.get("suffix"))
        return ""

    def enqueue_stub(job, *args, job_id):
        logger.info("[dry-run] would enqueue %s job_id=%s", job, job_id)
        return job_id

    deps.extract = extract_stub
    deps.write_entry = write_entry_stub
    deps.enqueue = enqueue_stub


def build_deps(*, dry_run: bool = False):
    """Build real Deps plus the resources the caller must close.

    Under --dry-run, `enqueue` is never called (see `_dry_run_stubs`), so no
    arq pool is opened either — a dry run must not require a reachable broker,
    since an unreachable broker is exactly the scenario one would dry-run to
    investigate.
    """
    sqlite_conn = hermes_state.connect_ro(config.paths.state_db)
    pg_conn = get_conn()
    pg = _PgAdapter(pg_conn)
    extract_fn = functools.partial(
        extraction.extract_entries, base_url=config.litellm.base_url,
        api_key=config.litellm.api_key, model=config.litellm.models.extraction.name,
        max_tokens=int(config.litellm.models.extraction.max_tokens),
        timeout=int(config.litellm.models.extraction.timeout))

    enqueuer = None
    enqueue = None
    if not dry_run:
        from arq.connections import RedisSettings
        enqueuer = _Enqueuer(RedisSettings(
            host=config.valkey.host, port=int(config.valkey.port),
            password=config.valkey.password or None, database=int(config.valkey.db)))
        enqueue = enqueuer

    deps = Deps(sqlite_conn=sqlite_conn, pg=pg, extract=extract_fn,
               write_entry=icarus_write_entry, enqueue=enqueue, now=time.time)
    if dry_run:
        _dry_run_stubs(deps, pg)
    return deps, pg_conn, sqlite_conn, enqueuer


def main(argv=None) -> dict:
    parser = argparse.ArgumentParser(
        description="Session sweeper — extract quiet Hermes conversations into fabric "
                    "entries and enqueue them for vector ingestion.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log what would happen; make no writes and no claim")
    parser.add_argument("--session", metavar="ID",
                        help="Only consider this session id, if it is a candidate")
    parser.add_argument("--verbose", action="store_true", help="Debug-level logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = _load_cfg(config)
    if args.session:
        cfg["session_id"] = args.session
        # find_candidates() cuts at max_per_run BEFORE the --session filter
        # runs, so a target session ranked below that cut would silently
        # report zero candidates. Widen the fetch for this one-off case.
        cfg["max_per_run"] = max(cfg["max_per_run"], 50)

    deps, pg_conn, sqlite_conn, enqueuer = build_deps(dry_run=args.dry_run)
    result, error = None, None
    try:
        result = sweep(deps, cfg)
        logger.info("sweep result: %s", result)
        return result
    except Exception as exc:
        error = exc
        raise
    finally:
        # "Stalled" must be a query, not an inspection (ADR-0001 §5): every
        # run — success or failure — leaves a sweeper_status row.
        r = result or {}
        try:
            session_store.record_run(
                pg_conn, candidates=r.get("candidates", 0), extracted=r.get("extracted", 0),
                entries=r.get("entries", 0), jobs=r.get("jobs", 0),
                schema_version=hermes_state.schema_version(sqlite_conn), error=error)
        except Exception:
            logger.exception("failed to record sweeper_status row")
        sqlite_conn.close()
        if enqueuer is not None:
            enqueuer.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
