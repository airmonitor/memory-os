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
import uuid
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


_POINT_NS = uuid.NAMESPACE_URL


def point_id(job: str) -> str:
    """Stable id for a slice's entry, so a replay upserts rather than adds.

    uuid4 stays the DEFAULT in the worker (see ingest_memory): content
    addressing would collapse two ingestions of identical text under Qdrant's
    upsert-by-id and erase the payload of the first. Only this caller knows
    the identity it wants — the ARQ job id already assigned to this slice's
    entry — so only this caller passes an explicit id (ADR-0002 decision 2).
    """
    return str(uuid.uuid5(_POINT_NS, job))


def entry_suffix(session_id: str, last_message_id: int, index: int) -> str:
    raw = f"{session_id}:{last_message_id}:{index}".encode()
    return hashlib.sha256(raw).hexdigest()[:8]


def payload_item(session_id: str, last_message_id: int, index: int, entry: dict,
                 platform: str) -> dict:
    """What `mark_extracted` stores per entry — BOTH halves of the publish.

    `text` and `job_id` are what the ARQ dispatch needs. The rest is what the
    fabric WRITE needs, and it is stored for exactly the same reason: a retry
    has to be able to redo both. When the payload carried only the dispatch
    half, a `write_entry` failure ended with the next sweep's `redispatch()`
    enqueuing the memory into Qdrant while the file it describes was never
    written — the one asymmetry this ordering exists to prevent.
    """
    return {
        "job_id": job_id(session_id, last_message_id, index),
        "text": entry["content"],
        "entry_type": entry["type"],
        "summary": entry["summary"],
        "training_value": entry.get("training_value", "normal"),
        "platform": platform,
        "suffix": entry_suffix(session_id, last_message_id, index),
    }


def write_payload_entry(deps: Deps, item: dict) -> bool:
    """Write one fabric file from a stored payload item.

    False means the item predates the enriched shape above — a row an older
    sweeper left at 'extracted', carrying only `job_id`/`text`. Those are
    dispatch-only by construction and must not be treated as an error, or a
    single legacy row would block its own drain forever.
    """
    if "entry_type" not in item:
        logger.info("payload item %s predates the fabric fields — dispatch only",
                    item.get("job_id"))
        return False
    deps.write_entry(entry_type=item["entry_type"], content=item["text"],
                     summary=item["summary"], platform=item.get("platform") or "cli",
                     training_value=item.get("training_value", "normal"),
                     status="completed", suffix=item["suffix"], origin="session-sweeper")
    return True


def redispatch(deps: Deps) -> int:
    """Finish slices that were extracted but never published.

    This is the other half of the ordering guarantee. Publishing before
    dispatching means a broker outage cannot lose an entry — but only if
    something comes back for it. The payload was stored with the claim, so this
    costs no LLM call.

    Files first, then jobs, in that order and for the same reason as the fresh
    path: whatever failed the first time, this must never leave a memory in
    Qdrant whose fabric entry does not exist on disk.
    """
    sent = 0
    for row in deps.pg.pending_dispatch():
        job_ids = []
        # mark_published is INSIDE this try on purpose: it is a Postgres call
        # like any other, so it can raise too, and redispatch() runs before
        # candidate selection even begins — an uncaught raise here would abort
        # the whole sweep, not just this one pending row.
        try:
            for item in row["payload"]:
                write_payload_entry(deps, item)
            for item in row["payload"]:
                job = item.get("job_id")
                deps.enqueue("process_ingestion", item["text"], "session",
                             job_id=item["job_id"], point_id=point_id(job) if job else None)
                job_ids.append(item["job_id"])
            deps.pg.mark_published(session_id=row["session_id"],
                                   last_message_id=row["last_message_id"], jobs=job_ids)
            sent += 1
        except Exception as exc:
            logger.warning("re-dispatch of %s:%s failed: %s",
                           row["session_id"], row["last_message_id"], exc)
            continue
    return sent


def sweep(deps: Deps, cfg: dict) -> dict:
    now = deps.now()
    deps.pg.ensure_schema()

    # BEFORE watermarks(), and the order is the whole fix. A hard crash leaves a
    # row at 'claimed'; watermarks() counts it; find_candidates then sees zero
    # pending messages for that session and skips it — so claim() is never
    # called with that key and its stale-claim ON CONFLICT branch can never
    # fire. Expiring here drops the row out of the watermark, which puts the
    # slice back in front of find_candidates, and claim()'s existing 'failed'
    # arm re-claims it. Measured before the fix: a stuck 'claimed' row at
    # (s, 12) with 12 unextracted messages reported `candidates: 0` forever.
    expired = deps.pg.expire_stale_claims()
    if expired:
        logger.warning("expired %d stale claim(s) — slices they held are offered again",
                       expired)

    live_schema = hermes_state.schema_version(deps.sqlite_conn)
    if live_schema is not None and live_schema != hermes_state.KNOWN_SCHEMA_VERSION:
        # ADR-0001 §5: drift is logged, never fatal. Recording the number in the
        # status row was never the promise on its own — nothing compared it.
        logger.warning("SCHEMA-DRIFT: state.db schema_version is %s, this sweeper "
                       "was written against %s — reading it anyway",
                       live_schema, hermes_state.KNOWN_SCHEMA_VERSION)

    stats = {"candidates": 0, "extracted": 0, "entries": 0, "jobs": 0,
             "quarantined": 0, "aborted": False, "locked_out": 0, "stale_slices": 0,
             "redispatched": redispatch(deps)}
    marks = deps.pg.watermarks()
    warned_compacted = False

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
            # ADR-0001 §4 promises a warning the first time a compacted row is
            # met: compaction has never run on this installation (0 rows
            # measured), so its effect on a slice is unobserved rather than
            # known-safe. `compacted` was read into Message and then never
            # looked at, which made the promise a no-op.
            if not warned_compacted and any(m.compacted for m in messages):
                warned_compacted = True
                logger.warning("COMPACTED-ROWS: session %s carries compacted messages; "
                               "Hermes may have rewritten history before this slice "
                               "was swept (ADR-0001 §4, unobserved until now)",
                               cand.session_id)
            context = hermes_state.context_tail(deps.sqlite_conn, cand.session_id,
                                                before_id=after,
                                                limit=cfg["context_overlap"]) if after else []
            slices.append((cand, context, messages))

    # Run-local circuit breaker state (ADR-0002 decision 4). `transient_streak`
    # counts CONSECUTIVE transient failures — any non-transient outcome resets
    # it, since "consecutive" is the whole signal: an isolated timeout is
    # noise, an unbroken run of them is an outage. `det_sessions`/`det_rows`
    # track EVERY deterministic failure this run, by distinct session id: two
    # different sessions failing deterministically in one run is what a
    # misrouting gateway looks like, since its HTTP-200 door is indistinguishable
    # per slice from a model that genuinely wrote nonsense. The two breakers
    # have SEPARATE thresholds (`cfg["transient_abort"]` and
    # `cfg["deterministic_sessions_abort"]`, fix round 1) — sharing one key
    # meant raising it to tolerate a flakier network also raised
    # `max_per_run`'s ceiling on how many distinct sessions a single run can
    # ever touch, silently disabling the systemic-gateway protection.
    transient_streak = 0
    det_sessions: set[str] = set()
    det_rows: list[tuple[str, int]] = []

    for cand, context, messages in slices:
        first_id, last_id = messages[0].id, messages[-1].id

        # ADR-0002 decision 3: pg_try_advisory_xact_lock, keyed on this
        # session, BEFORE claim(). One bad round trip must not stop the
        # sweep — fail open, same as claim() below.
        try:
            locked = deps.pg.try_session_lock(session_id=cand.session_id)
        except Exception as exc:
            logger.warning("session lock for %s failed: %s", cand.session_id, exc)
            continue
        if not locked:
            logger.info("session %s is being swept by another process — skipping",
                       cand.session_id)
            stats["locked_out"] += 1
            continue

        # THE RE-READ IS THE FIX, NOT THE LOCK. `marks` (used to build this
        # slice) was read before ANY sweeper took this lock, so a concurrent
        # sweeper could have read the same stale watermark, built a slice for
        # the same messages, won the lock first, and already published —
        # advancing the watermark past `first_id`. Its last_message_id and
        # ours differ (different watermark at build time), so the UNIQUE
        # constraint would never catch this: only re-reading after the lock
        # can. `>=` because a moved watermark means this slice's earliest
        # message has already been consumed by the other winner.
        fresh_marks = deps.pg.watermarks()
        if fresh_marks.get(cand.session_id, 0) >= first_id:
            logger.info("slice %s:%s is stale — the watermark moved past it while "
                       "waiting for the lock", cand.session_id, last_id)
            stats["stale_slices"] += 1
            continue

        # claim() is a Postgres round trip like any other and can raise (a
        # dropped connection, a lock timeout). One bad slice must not stop the
        # sweep, so a raise here is logged and this slice is skipped — it will
        # be offered again on the next sweep, since nothing was claimed.
        #
        # Claiming ownership of a slice is not yet attempting it — claim()
        # never touches `attempts`. Only a classified deterministic failure
        # (below, via mark_failed) counts against the ceiling.
        try:
            won = deps.pg.claim(session_id=cand.session_id, first_message_id=first_id,
                                last_message_id=last_id, message_count=len(messages))
        except Exception as exc:
            logger.warning("claim for slice %s:%s failed: %s", cand.session_id, last_id, exc)
            continue
        if not won:
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
                transient_streak = 0
                continue
            entries = deps.extract(extraction.build_transcript(messages, context=context))
            stats["extracted"] += 1
            transient_streak = 0
            platform = cand.source or "cli"
            payload = [payload_item(cand.session_id, last_id, i, e, platform)
                       for i, e in enumerate(entries)]
            deps.pg.mark_extracted(session_id=cand.session_id, last_message_id=last_id,
                                   entries=len(entries), score=score["total"],
                                   payload=payload)
        except extraction.ExtractionFailed as exc:
            if exc.transient:
                # An outage, not a bad slice: never counted, so a proxy or
                # model outage spanning several sweeps cannot retire a
                # conversation (ADR-0002 decision 4).
                logger.warning("slice %s:%s failed transiently during extraction: %s",
                               cand.session_id, last_id, exc)
                deps.pg.mark_failed(session_id=cand.session_id, last_message_id=last_id,
                                    error=exc, count_attempt=False)
                transient_streak += 1
                if transient_streak >= cfg["transient_abort"]:
                    logger.warning(
                        "aborting sweep: %d consecutive transient extraction failures — "
                        "likely a gateway outage, not bad slices", transient_streak)
                    stats["aborted"] = True
                    break
                continue
            # Deterministic: the gateway answered like a gateway; the model's
            # own content did not parse or validate. This one counts.
            logger.warning("slice %s:%s failed deterministically during extraction: %s",
                           cand.session_id, last_id, exc)
            transient_streak = 0
            attempts = deps.pg.mark_failed(session_id=cand.session_id, last_message_id=last_id,
                                           error=exc, count_attempt=True)
            det_rows.append((cand.session_id, last_id))
            det_sessions.add(cand.session_id)
            if len(det_sessions) >= cfg["deterministic_sessions_abort"]:
                # Systemic, not three bad slices: a misrouting gateway answers
                # every slice's request with an unusable-but-200 body, and
                # that is indistinguishable per slice from a model that wrote
                # nonsense — only the cross-session pattern separates them.
                # Refund every attempt this run spent; the row is left exactly
                # as if the ceiling had never been touched.
                logger.warning(
                    "aborting sweep: deterministic failures span %d distinct sessions — "
                    "a misrouting gateway looks like bad model output per slice "
                    "(ADR-0002 decision 4)", len(det_sessions))
                for r_sid, r_last in det_rows:
                    deps.pg.rollback_attempt(session_id=r_sid, last_message_id=r_last)
                # Every quarantine this run recorded came from one of the rows
                # just rolled back — `det_rows` holds EVERY deterministic
                # failure this run, and all of them are refunded above. Zeroing
                # (not decrementing) is correct because there is no other
                # source `stats["quarantined"]` could have been incremented
                # from. Leaving it non-zero would report a slice as retired
                # when its row is back to plain 'failed' (fix round 1,
                # "phantom quarantine").
                stats["quarantined"] = 0
                stats["aborted"] = True
                break
            if attempts >= cfg["max_attempts"]:
                deps.pg.mark_quarantined(session_id=cand.session_id, last_message_id=last_id,
                                         error=exc)
                stats["quarantined"] += 1
            continue
        except Exception as exc:                    # one bad slice must not stop the sweep
            logger.warning("slice %s:%s failed during extraction: %s",
                           cand.session_id, last_id, exc)
            deps.pg.mark_failed(session_id=cand.session_id, last_message_id=last_id,
                                error=exc, count_attempt=False)
            # Deliberate: an exception this generic isn't classified transient
            # or deterministic, so it resets the streak rather than extending
            # it (fix round 1, Minor 5). Known consequence — an outage that
            # surfaces as this shape in one sweep and as a classified transient
            # `ExtractionFailed` in the next never accumulates a streak across
            # the two, so it can only trip the breaker if it repeats in the
            # SAME shape consecutively. Not fixed here; recorded so the next
            # reader doesn't mistake it for an oversight.
            transient_streak = 0
            continue

        # Dispatch phase: the slice is ALREADY 'extracted' and its payload is
        # already stored in Postgres by this point. A broker outage here must
        # NOT be marked failed — that would make the slice reclaimable and pay
        # for a second LLM call. Instead it stays 'extracted', and the next
        # sweep's redispatch() drains it from the stored payload at no
        # further LLM cost.
        #
        # Every fabric file for the slice is written FIRST, and only then is any
        # job enqueued. Interleaving the two meant a write_entry failure on
        # entry 2 of 3 left entry 1 already in Qdrant's queue, the slice at
        # 'extracted', and the next sweep replaying the dispatch half against a
        # file that was never written. Files first makes a write failure
        # dispatch nothing at all, and the stored payload (see payload_item)
        # lets the retry redo both halves.
        try:
            for item in payload:
                write_payload_entry(deps, item)
                stats["entries"] += 1
            job_ids = []
            for item in payload:
                job = item.get("job_id")
                deps.enqueue("process_ingestion", item["text"], "session",
                             job_id=item["job_id"], point_id=point_id(job) if job else None)
                job_ids.append(item["job_id"])
                stats["jobs"] += 1
            deps.pg.mark_published(session_id=cand.session_id, last_message_id=last_id,
                                   jobs=job_ids)
        except Exception as exc:
            logger.warning("slice %s:%s failed during publish/dispatch (will retry): %s",
                           cand.session_id, last_id, exc)
    return stats


# ─── CLI ─────────────────────────────────────────────────────────────────

def _load_cfg(cfg_module) -> dict:
    """Read session_extraction.* from config, or fall back to the ADR defaults.

    The config block exists (`config/services.yaml`, `session_extraction:`), so
    the fallback is no longer a placeholder — it is what keeps the CLI runnable
    against a config file written before this key was added, and what keeps the
    defaults in one readable place next to the ADR that justifies them."""
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
        max_attempts=int(g("max_attempts", 3)),
        transient_abort=int(g("transient_abort", 2)),
        deterministic_sessions_abort=int(g("deterministic_sessions_abort", 2)),
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

    def expire_stale_claims(self):
        return session_store.expire_stale_claims(self.conn)

    def try_session_lock(self, session_id):
        return session_store.try_session_lock(self.conn, session_id)

    def claim(self, **kw):
        return session_store.claim(self.conn, **kw)

    def mark_extracted(self, **kw):
        return session_store.mark_extracted(self.conn, **kw)

    def mark_published(self, **kw):
        return session_store.mark_published(self.conn, **kw)

    def mark_failed(self, **kw):
        return session_store.mark_failed(self.conn, **kw)

    def mark_quarantined(self, **kw):
        return session_store.mark_quarantined(self.conn, **kw)

    def rollback_attempt(self, **kw):
        return session_store.rollback_attempt(self.conn, **kw)

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

    def __call__(self, job, *args, job_id, point_id=None):
        kwargs = {"point_id": point_id} if point_id is not None else {}
        return self._loop.run_until_complete(
            self._pool.enqueue_job(job, *args, _job_id=job_id, **kwargs))

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
    real run left mid-flight. So every write — including schema DDL and the
    end-of-run status row — goes through a stub; only reads (watermarks,
    pending_dispatch) stay real."""
    pg.ensure_schema = lambda: logger.info("[dry-run] would ensure_schema")
    # An UPDATE like any other: a dry run must not flip a live 'claimed' row to
    # 'failed' behind an operator who is only looking.
    pg.expire_stale_claims = lambda: logger.info("[dry-run] would expire stale claims") or 0
    # A dry run must take no lock either — pg_try_advisory_xact_lock is still a
    # real round trip to a real database, and --dry-run promises neither.
    pg.try_session_lock = lambda **kw: True
    pg.claim = lambda **kw: True
    pg.mark_extracted = lambda **kw: logger.info(
        "[dry-run] would mark_extracted %s:%s", kw.get("session_id"), kw.get("last_message_id"))
    pg.mark_published = lambda **kw: logger.info(
        "[dry-run] would mark_published %s:%s", kw.get("session_id"), kw.get("last_message_id"))
    pg.mark_failed = lambda **kw: logger.info(
        "[dry-run] would mark_failed %s:%s", kw.get("session_id"), kw.get("last_message_id")) or 0
    pg.mark_quarantined = lambda **kw: logger.info(
        "[dry-run] would mark_quarantined %s:%s", kw.get("session_id"), kw.get("last_message_id"))
    pg.rollback_attempt = lambda **kw: logger.info(
        "[dry-run] would rollback_attempt %s:%s", kw.get("session_id"), kw.get("last_message_id"))
    pg.record_run = lambda **kw: logger.info("[dry-run] would record_run %s", kw)

    def extract_stub(transcript):
        logger.info("[dry-run] would extract from a %d-char transcript", len(transcript))
        return []

    def write_entry_stub(**kw):
        logger.info("[dry-run] would write fabric entry suffix=%s", kw.get("suffix"))
        return ""

    def enqueue_stub(job, *args, job_id, point_id=None):
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
        # run — success or failure — leaves a sweeper_status row. Routed
        # through deps.pg (not scripts.session_store directly against
        # pg_conn), so --dry-run's stubbing of deps.pg.record_run actually
        # covers this call instead of being bypassed by a second write path.
        r = result or {}
        try:
            deps.pg.record_run(
                candidates=r.get("candidates", 0), extracted=r.get("extracted", 0),
                entries=r.get("entries", 0), jobs=r.get("jobs", 0),
                redispatched=r.get("redispatched", 0), quarantined=r.get("quarantined", 0),
                schema_version=hermes_state.schema_version(sqlite_conn), error=error)
        except Exception:
            logger.exception("failed to record sweeper_status row")
        sqlite_conn.close()
        if enqueuer is not None:
            enqueuer.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
