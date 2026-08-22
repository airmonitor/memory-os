import sqlite3
import time

import pytest

from icarus import extraction, hermes_state as hs
from scripts import session_sweeper as sw
from tests.conftest import SESSION, MSG

HOUR = 3600
CFG = dict(idle_seconds=90 * 60, min_messages=2, context_overlap=2,
           max_lag_seconds=24 * HOUR, max_per_run=3, quality_threshold=0.2)


class FakePg:
    """Stands in for scripts.session_store. Watermarks are DERIVED from claims,
    the way the real table derives them, so a second sweep in a test sees what a
    second sweep in production would see."""

    def __init__(self, claimed=()):
        self.claimed = {k: "published" for k in claimed}
        self.calls, self.marks, self.payloads = [], [], {}
        # Keys whose 'claimed' row is older than STALE_CLAIM_HOURS. Wall-clock
        # ageing is what the real UPDATE does; a test says so directly.
        self.stale = set()

    def ensure_schema(self): self.calls.append("ensure_schema")

    def expire_stale_claims(self):
        expired = 0
        for key, status in list(self.claimed.items()):
            if status == "claimed" and key in self.stale:
                self.claimed[key] = "failed"
                self.stale.discard(key)
                self.marks.append(("expired", {"session_id": key[0],
                                               "last_message_id": key[1]}))
                expired += 1
        return expired

    def watermarks(self):
        out = {}
        for (sid, last), status in self.claimed.items():
            if status == "failed":
                continue
            out[sid] = max(out.get(sid, 0), last)
        return out

    def claim(self, **kw):
        key = (kw["session_id"], kw["last_message_id"])
        if key in self.claimed and self.claimed[key] != "failed":
            return False
        self.claimed[key] = "claimed"
        return True

    def mark_extracted(self, **kw):
        self.claimed[(kw["session_id"], kw["last_message_id"])] = "extracted"
        self.payloads[(kw["session_id"], kw["last_message_id"])] = list(kw.get("payload", []))
        self.marks.append(("extracted", kw))

    def mark_published(self, **kw):
        self.claimed[(kw["session_id"], kw["last_message_id"])] = "published"
        self.marks.append(("published", kw))

    def mark_failed(self, **kw):
        self.claimed[(kw["session_id"], kw["last_message_id"])] = "failed"
        self.marks.append(("failed", kw))

    def pending_dispatch(self):
        return [{"session_id": sid, "last_message_id": last,
                 "payload": self.payloads.get((sid, last), [])}
                for (sid, last), status in self.claimed.items() if status == "extracted"]

    def record_run(self, **kw): self.marks.append(("run", kw))


def rich_session(now):
    msgs = []
    for i in range(1, 7):
        msgs.append(MSG(2 * i - 1, "s", "user", "u" * 60))
        msgs.append(MSG(2 * i, "s", "assistant",
                        "we decided to use X. Result: measured, it works. " + "d" * 200))
    return [SESSION("s", last_activity_at=now - 2 * HOUR, message_count=len(msgs))], msgs


def make_deps(path, pg, *, entries=None, enqueue=None):
    written, jobs = [], []

    def write_entry(**kw):
        written.append(kw)
        return f"/fabric/{kw['suffix']}.md"

    def _enqueue(job, *args, job_id, point_id=None):
        jobs.append(job_id)
        return job_id
    return sw.Deps(sqlite_conn=hs.connect_ro(path), pg=pg,
                   extract=lambda transcript: entries if entries is not None else
                   [{"type": "decision", "summary": "s", "content": "c",
                     "training_value": "high"}],
                   write_entry=write_entry, enqueue=enqueue or _enqueue,
                   now=time.time), written, jobs


def test_a_quiet_substantive_session_is_extracted_published_and_dispatched(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), FakePg())
    result = sw.sweep(deps, CFG)
    assert result["extracted"] == 1 and result["entries"] == 1 and result["jobs"] == 1
    assert written[0]["suffix"] == sw.entry_suffix("s", 12, 0)
    assert jobs == [sw.job_id("s", 12, 0)]


def test_every_dispatched_job_carries_its_deterministic_point_id(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    captured = []

    def enqueue(job, *args, job_id, **kw):
        captured.append((job_id, kw.get("point_id")))
        return job_id

    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), FakePg(),
                           enqueue=enqueue)
    sw.sweep(deps, CFG)
    assert captured == [(sw.job_id("s", 12, 0), sw.point_id(sw.job_id("s", 12, 0)))]


def test_redispatch_also_carries_the_deterministic_point_id(hermes_db):
    """Controller decision: redispatch() must derive the same id from the
    stored payload's job_id, with no new payload field — verified, not assumed."""
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg()
    pg.claimed[("old", 5)] = "extracted"
    pg.payloads[("old", 5)] = [{"job_id": "ingest:old:5:0", "text": "stale"}]
    captured = []

    def enqueue(job, *args, job_id, **kw):
        captured.append((job_id, kw.get("point_id")))
        return job_id

    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), pg,
                           enqueue=enqueue)
    sw.sweep(deps, CFG)
    assert ("ingest:old:5:0", sw.point_id("ingest:old:5:0")) in captured


def test_a_lost_claim_skips_the_llm_call_entirely(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg(claimed={("s", 12)})
    calls = []
    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    deps.extract = lambda t: calls.append(t) or []
    result = sw.sweep(deps, CFG)
    assert calls == []
    assert result["extracted"] == 0


def test_low_scoring_chatter_is_consumed_so_it_is_never_offered_twice(hermes_db):
    now = time.time()
    path = hermes_db(sessions=[SESSION("s", last_activity_at=now - 2 * HOUR, message_count=2)],
                     messages=[MSG(1, "s", "user", "hi"), MSG(2, "s", "assistant", "hello")])
    pg = FakePg()
    deps, written, jobs = make_deps(path, pg)
    result = sw.sweep(deps, CFG)
    assert result["entries"] == 0 and written == [] and jobs == []
    # The watermark must advance, or every sweep forever re-reads the same chatter
    # and pays a claim for it.
    assert pg.watermarks() == {"s": 2}
    deps2, _, _ = make_deps(path, pg)
    assert sw.sweep(deps2, CFG)["candidates"] == 0


def test_a_slice_that_failed_to_dispatch_goes_out_on_the_next_sweep(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()

    def boom(job, *args, job_id, point_id=None):
        raise RuntimeError("valkey down")

    deps, _, _ = make_deps(path, pg, enqueue=boom)
    sw.sweep(deps, CFG)
    assert pg.pending_dispatch(), "the slice must remain dispatchable"

    sent = []

    def ok(job, *args, job_id, point_id=None):
        sent.append(job_id)
        return job_id

    deps2, _, _ = make_deps(path, pg, enqueue=ok)
    result = sw.sweep(deps2, CFG)
    # Same job id as the first attempt: arq dedups, so a double delivery is a
    # no-op rather than a second copy in Qdrant.
    assert sent == [sw.job_id("s", 12, 0)]
    assert result["redispatched"] == 1
    # And no second LLM call was paid for.
    assert result["extracted"] == 0


def test_dispatch_failure_leaves_the_slice_re_runnable(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)

    def boom(job, *args, job_id, point_id=None):
        raise RuntimeError("valkey down")
    pg = FakePg()
    deps, written, _ = make_deps(hermes_db(sessions=sessions, messages=messages), pg,
                                 enqueue=boom)
    result = sw.sweep(deps, CFG)
    assert result["jobs"] == 0
    statuses = [m[0] for m in pg.marks]
    assert "extracted" in statuses and "published" not in statuses


def test_max_per_run_bounds_the_number_of_llm_calls(hermes_db):
    now = time.time()
    sessions, messages = [], []
    for n in range(5):
        sid = f"s{n}"
        sessions.append(SESSION(sid, last_activity_at=now - 2 * HOUR, message_count=4))
        base = 100 * n
        for i in range(2):
            messages.append(MSG(base + 2 * i + 1, sid, "user", "u" * 60))
            messages.append(MSG(base + 2 * i + 2, sid, "assistant",
                                "decided. Result: works. " + "d" * 200))
    deps, _, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), FakePg())
    result = sw.sweep(deps, dict(CFG, max_per_run=2))
    assert result["extracted"] == 2


def test_a_slice_that_errors_during_extraction_is_marked_failed_not_left_claimed(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg()

    def boom_extract(transcript):
        raise RuntimeError("llm proxy unreachable")

    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    deps.extract = boom_extract
    result = sw.sweep(deps, CFG)
    assert result["extracted"] == 0 and result["entries"] == 0 and result["jobs"] == 0
    # 'claimed' counts toward watermarks() the same as any other non-failed
    # status, so a slice stuck there would never be offered again. It must be
    # 'failed' instead, which both excludes it from the watermark and makes it
    # reclaimable.
    assert pg.claimed[("s", 12)] == "failed"
    assert pg.watermarks() == {}


def test_context_tail_feeds_the_continuation_transcript(hermes_db):
    """Task 2 shipped icarus.hermes_state.context_tail with no test coverage, and
    this sweeper is its first consumer. Build a session, sweep it once so a
    watermark exists at message 8, then append a continuation and assert the
    second slice's transcript carries the earlier tail marked as CONTEXT."""
    now = time.time()
    messages = []
    for i in range(1, 5):
        messages.append(MSG(2 * i - 1, "s", "user", "u" * 60))
        messages.append(MSG(2 * i, "s", "assistant",
                            "decided. Result: works. " + "d" * 200))
    session = SESSION("s", last_activity_at=now - 2 * HOUR, message_count=len(messages))
    path = hermes_db(sessions=[session], messages=messages)
    pg = FakePg()
    deps, _, _ = make_deps(path, pg)
    sw.sweep(deps, CFG)
    assert pg.watermarks() == {"s": 8}

    # Hermes appends a continuation to the same (still-quiet) session.
    con = sqlite3.connect(path)
    con.execute(
        "INSERT INTO messages (id, session_id, role, content, tool_calls, tool_name,"
        " timestamp, active, compacted) VALUES (?, ?, ?, ?, '', NULL, ?, 1, 0)",
        (9, "s", "user", "w" * 60, now))
    con.execute(
        "INSERT INTO messages (id, session_id, role, content, tool_calls, tool_name,"
        " timestamp, active, compacted) VALUES (?, ?, ?, ?, '', NULL, ?, 1, 0)",
        (10, "s", "assistant", "decided once more. Result: works. " + "e" * 200, now))
    con.commit()
    con.close()

    captured = []
    deps2, _, _ = make_deps(path, pg)
    deps2.extract = lambda transcript: captured.append(transcript) or [
        {"type": "decision", "summary": "s2", "content": "c2", "training_value": "high"}]
    sw.sweep(deps2, CFG)

    assert captured, "the continuation slice must call extract"
    transcript = captured[0]
    assert "=== CONTEXT" in transcript and "=== CURRENT SLICE ===" in transcript
    # The last message of the FIRST slice (id 8) must appear in the CONTEXT
    # section, not the scored current slice.
    context_part, _, current_part = transcript.partition("=== CURRENT SLICE ===")
    assert "d" * 200 in context_part
    assert "d" * 200 not in current_part


def two_eligible_sessions(now):
    """Two quiet, substantive sessions — same shape as
    test_max_per_run_bounds_the_number_of_llm_calls, but only two of them, and
    with each session's last message id handed back so tests can assert on
    specific job/entry ids without recomputing the arithmetic."""
    sessions, messages, last_ids = [], [], {}
    for n in range(2):
        sid = f"s{n}"
        sessions.append(SESSION(sid, last_activity_at=now - 2 * HOUR, message_count=4))
        base = 100 * n
        for i in range(2):
            messages.append(MSG(base + 2 * i + 1, sid, "user", "u" * 60))
            messages.append(MSG(base + 2 * i + 2, sid, "assistant",
                                "decided. Result: works. " + "d" * 200))
        last_ids[sid] = base + 4
    return sessions, messages, last_ids


class RaisingPg:
    """Every write-path method raises. If dry-run stubbing is complete, none
    of these are ever reached — the reads (watermarks, pending_dispatch) are
    real, harmless no-ops, since dry-run must not need them stubbed too."""

    def ensure_schema(self):
        raise AssertionError("ensure_schema must not run under --dry-run")

    def watermarks(self):
        return {}

    def expire_stale_claims(self):
        raise AssertionError("expire_stale_claims must not run under --dry-run")

    def claim(self, **kw):
        raise AssertionError("claim must not run under --dry-run")

    def mark_extracted(self, **kw):
        raise AssertionError("mark_extracted must not run under --dry-run")

    def mark_published(self, **kw):
        raise AssertionError("mark_published must not run under --dry-run")

    def mark_failed(self, **kw):
        raise AssertionError("mark_failed must not run under --dry-run")

    def pending_dispatch(self):
        return []

    def record_run(self, **kw):
        raise AssertionError("record_run must not run under --dry-run")


def _boom(*a, **kw):
    raise AssertionError("must not run under --dry-run")


def test_dry_run_performs_no_postgresql_write(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = RaisingPg()
    deps = sw.Deps(
        sqlite_conn=hs.connect_ro(path), pg=pg,
        # A real extract, plus write_entry/enqueue that raise if reached,
        # prove the dry-run stubs actually replace them rather than merely
        # being layered on top.
        extract=lambda t: [{"type": "decision", "summary": "s", "content": "c",
                            "training_value": "high"}],
        write_entry=_boom, enqueue=_boom, now=time.time)
    sw._dry_run_stubs(deps, pg)
    result = sw.sweep(deps, CFG)
    # Every entry point on `pg`, plus the real extract/write_entry/enqueue
    # above, would raise if actually reached. Completing at all proves the
    # dry-run stubs intercepted every one of them.
    assert result["candidates"] == 1
    # extract_stub always returns [] regardless of the real `extract` supplied
    # above — a dry run must never depend on what the LLM would have said.
    assert result["extracted"] == 1 and result["entries"] == 0 and result["jobs"] == 0


class ClaimBoomOnFirst(FakePg):
    """Like FakePg, except claim() raises for one specific session — a
    Postgres round trip failing for one candidate, not a lost race."""

    def __init__(self, boom_session, **kw):
        super().__init__(**kw)
        self.boom_session = boom_session

    def claim(self, **kw):
        if kw["session_id"] == self.boom_session:
            raise RuntimeError("db down")
        return super().claim(**kw)


def test_a_claim_failure_does_not_stop_other_candidates(hermes_db):
    now = time.time()
    sessions, messages, last_ids = two_eligible_sessions(now)
    pg = ClaimBoomOnFirst("s0")
    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    result = sw.sweep(deps, CFG)
    assert result["candidates"] == 2
    assert result["extracted"] == 1
    # s0's claim raised before any row was created for it.
    assert ("s0", last_ids["s0"]) not in pg.claimed
    # s1 was claimed, extracted and dispatched normally.
    assert pg.claimed[("s1", last_ids["s1"])] == "published"
    assert jobs == [sw.job_id("s1", last_ids["s1"], 0)]


def test_a_mark_published_failure_in_redispatch_does_not_abort_the_sweep(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg()
    # A stale row a prior run left extracted but never dispatched.
    pg.claimed[("old", 5)] = "extracted"
    pg.payloads[("old", 5)] = [{"job_id": "ingest:old:5:0", "text": "stale"}]

    real_mark_published = pg.mark_published

    def boom_mark_published(**kw):
        if kw["session_id"] == "old":
            raise RuntimeError("db down during redispatch")
        return real_mark_published(**kw)
    pg.mark_published = boom_mark_published

    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    result = sw.sweep(deps, CFG)
    # redispatch()'s failure on the stale row must not prevent candidate
    # selection, or the new session's own extraction, from running.
    assert result["redispatched"] == 0
    assert result["candidates"] == 1
    assert result["extracted"] == 1
    assert pg.claimed[("old", 5)] == "extracted"  # left exactly as redispatch found it
    assert pg.claimed[("s", 12)] == "published"


def test_session_filter_limits_the_sweep_to_one_session(hermes_db):
    now = time.time()
    sessions, messages, last_ids = two_eligible_sessions(now)
    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), FakePg())
    result = sw.sweep(deps, dict(CFG, session_id="s1"))
    assert result["candidates"] == 1
    assert result["extracted"] == 1
    assert jobs == [sw.job_id("s1", last_ids["s1"], 0)]


# ── Fix wave, 2026-08-22 ───────────────────────────────────────────────────

def raising_opener(req, timeout=None):
    raise TimeoutError("the read operation timed out")


def real_extract_against(opener):
    """deps.extract wired to the REAL extract_entries. The pre-existing failure
    test used a fake that raises, which the real function never did — it caught
    everything and returned []. Going through the real one is the point."""
    return lambda transcript: extraction.extract_entries(
        transcript, base_url="http://x/v1", api_key="k", model="m",
        max_tokens=10, timeout=5, opener=opener)


def test_a_real_extraction_failure_marks_failed_and_the_slice_comes_back(hermes_db):
    """FIX 1. With extract_entries swallowing its own failures, this slice ended
    'published' at watermark 12 with sweeper_status.error NULL, and the next
    sweep saw no candidates — the conversation was consumed unread."""
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    deps, written, jobs = make_deps(path, pg)
    deps.extract = real_extract_against(raising_opener)

    result = sw.sweep(deps, CFG)
    assert result["extracted"] == 0 and result["entries"] == 0 and result["jobs"] == 0
    assert written == [] and jobs == []
    assert pg.claimed[("s", 12)] == "failed"
    # 'failed' is excluded from the watermark, which is what re-offers the slice.
    assert pg.watermarks() == {}
    failed = [kw for kind, kw in pg.marks if kind == "failed"]
    assert "timed out" in str(failed[0]["error"])

    deps2, written2, jobs2 = make_deps(path, pg)
    result2 = sw.sweep(deps2, CFG)
    assert result2["candidates"] == 1 and result2["extracted"] == 1
    assert jobs2 == [sw.job_id("s", 12, 0)]


def test_a_missing_api_key_does_not_consume_the_backlog(hermes_db):
    """FIX 1, the second reproduction: with no key at all the old code returned
    [] for every slice, so a sweep marked three conversations published per run
    without ever calling the model."""
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    deps, _, _ = make_deps(path, pg)
    deps.extract = lambda t: extraction.extract_entries(
        t, base_url="http://x/v1", api_key="", model="m", max_tokens=10,
        timeout=5, opener=raising_opener)
    sw.sweep(deps, CFG)
    assert pg.claimed[("s", 12)] == "failed"
    assert pg.watermarks() == {}


def test_a_stale_claim_is_expired_and_its_slice_extracted_again(hermes_db):
    """FIX 2. A hard crash between claim and extract leaves the row at
    'claimed'; watermarks() counts it, find_candidates then sees zero pending
    messages and skips the session, so claim() is never called with that key
    and its reclaim branch can never fire. Reproduced before the fix as
    `candidates: 0` forever."""
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    pg.claimed[("s", 12)] = "claimed"
    pg.stale.add(("s", 12))
    assert pg.watermarks() == {"s": 12}, "the stuck row is what hides the slice"

    deps, written, jobs = make_deps(path, pg)
    result = sw.sweep(deps, CFG)
    assert result["candidates"] == 1 and result["extracted"] == 1
    assert pg.claimed[("s", 12)] == "published"
    assert jobs == [sw.job_id("s", 12, 0)]


def test_a_fresh_claim_is_left_alone(hermes_db):
    """The other half of FIX 2: expiry must not steal a slice a concurrent
    sweeper is legitimately working on right now."""
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg()
    pg.claimed[("s", 12)] = "claimed"          # not in pg.stale
    deps, _, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    result = sw.sweep(deps, CFG)
    assert result["candidates"] == 0 and jobs == []
    assert pg.claimed[("s", 12)] == "claimed"


def two_entries():
    return [{"type": "decision", "summary": f"s{i}", "content": f"c{i}",
             "training_value": "high"} for i in range(2)]


def test_a_write_entry_failure_dispatches_nothing(hermes_db):
    """FIX 3. write_entry and enqueue used to interleave, so a failure on the
    second entry left the first already queued for Qdrant."""
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg()
    deps, _, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), pg,
                              entries=two_entries())
    written = []

    def failing_write(**kw):
        written.append(kw)
        if len(written) == 2:
            raise OSError("read-only file system")
        return "/fabric/x.md"
    deps.write_entry = failing_write

    result = sw.sweep(deps, CFG)
    assert jobs == [], "nothing may be dispatched when a fabric write failed"
    assert result["jobs"] == 0
    # Still re-runnable: 'extracted', not 'published', and not 'failed' either
    # (the LLM call succeeded and its payload is banked).
    assert pg.claimed[("s", 12)] == "extracted"


def test_the_next_sweep_writes_the_file_the_failed_write_never_produced(hermes_db):
    """FIX 3, the half a plain reordering would have missed: the retry path has
    to redo BOTH halves. When payload carried only job_id+text, redispatch sent
    the memory to Qdrant and the fabric file stayed missing forever."""
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    deps, _, _ = make_deps(path, pg, entries=two_entries())
    attempts = []

    def failing_write(**kw):
        attempts.append(kw)
        if len(attempts) == 2:
            raise OSError("read-only file system")
        return "/fabric/x.md"
    deps.write_entry = failing_write

    sw.sweep(deps, CFG)
    assert pg.claimed[("s", 12)] == "extracted"

    deps2, written2, jobs2 = make_deps(path, pg, entries=two_entries())
    result2 = sw.sweep(deps2, CFG)
    assert result2["redispatched"] == 1
    # Both fabric files, then both jobs — and no second LLM call.
    assert [w["suffix"] for w in written2] == [sw.entry_suffix("s", 12, 0),
                                               sw.entry_suffix("s", 12, 1)]
    assert jobs2 == [sw.job_id("s", 12, 0), sw.job_id("s", 12, 1)]
    assert result2["extracted"] == 0
    assert pg.claimed[("s", 12)] == "published"


def test_a_legacy_payload_row_still_drains(hermes_db):
    """Rows an older sweeper left at 'extracted' carry only job_id and text.
    They are dispatch-only by construction and must not block their own drain."""
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg()
    pg.claimed[("old", 5)] = "extracted"
    pg.payloads[("old", 5)] = [{"job_id": "ingest:old:5:0", "text": "stale"}]
    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    result = sw.sweep(deps, CFG)
    assert result["redispatched"] == 1
    assert "ingest:old:5:0" in jobs
    assert pg.claimed[("old", 5)] == "published"


class _FakePgConn:
    def close(self):
        pass


def test_redispatched_reaches_the_status_row(hermes_db, monkeypatch):
    """FIX 7. sweep() computed `redispatched` and main() dropped it: record_run
    had no such parameter and sweeper_status no such column, so a backlog that
    never drains was invisible to the one table built to make stalls queryable."""
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg()
    pg.claimed[("old", 5)] = "extracted"
    pg.payloads[("old", 5)] = [{"job_id": "ingest:old:5:0", "text": "stale"}]
    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    monkeypatch.setattr(sw, "build_deps",
                        lambda **kw: (deps, _FakePgConn(), deps.sqlite_conn, None))

    sw.main([])
    runs = [kw for kind, kw in pg.marks if kind == "run"]
    assert runs, "every run must leave a status row"
    assert runs[-1]["redispatched"] == 1
    assert runs[-1]["schema_version"] == 26


def test_schema_drift_is_warned_about_once_and_does_not_stop_the_sweep(hermes_db, caplog):
    """FIX 7. schema_version was recorded and never compared."""
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages, schema_version=99)
    deps, _, jobs = make_deps(path, FakePg())
    with caplog.at_level("WARNING"):
        result = sw.sweep(deps, CFG)
    drift = [r for r in caplog.records if "SCHEMA-DRIFT" in r.getMessage()]
    assert len(drift) == 1
    assert "99" in drift[0].getMessage()
    assert str(hs.KNOWN_SCHEMA_VERSION) in drift[0].getMessage()
    assert result["extracted"] == 1, "drift is logged, never fatal"


def test_a_compacted_row_is_warned_about_once_per_run(hermes_db, caplog):
    """FIX 7. `compacted` was read into Message and never inspected, so the
    ADR's promised warning was a no-op."""
    now = time.time()
    sessions, messages = rich_session(now)
    for m in messages:
        m["compacted"] = 1
    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), FakePg())
    with caplog.at_level("WARNING"):
        sw.sweep(deps, CFG)
    hits = [r for r in caplog.records if "COMPACTED-ROWS" in r.getMessage()]
    assert len(hits) == 1


def test_write_payload_entry_records_the_sweeper_as_origin():
    """ADR-0002 decision 1.4: an unwired origin field is worse than no field.
    `write_payload_entry` is the ONE place `deps.write_entry` is called — both
    the fresh-publish path (sweep) and the re-dispatch replay go through it —
    so asserting here covers both call sites at once."""
    calls = []
    deps = sw.Deps(sqlite_conn=None, pg=None, extract=None,
                   write_entry=lambda **kw: calls.append(kw), enqueue=None, now=None)
    item = {"job_id": "ingest:s:12:0", "text": "c", "entry_type": "decision",
            "summary": "s", "suffix": "deadbeef"}
    sw.write_payload_entry(deps, item)
    assert calls[0]["origin"] == "session-sweeper"
