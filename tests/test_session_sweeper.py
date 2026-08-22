import sqlite3
import time

import pytest

from icarus import extraction, hermes_state as hs
from scripts import session_sweeper as sw
from tests.conftest import SESSION, MSG

HOUR = 3600
CFG = dict(idle_seconds=90 * 60, min_messages=2, context_overlap=2,
           max_lag_seconds=24 * HOUR, max_per_run=3, quality_threshold=0.2,
           max_attempts=3, transient_abort=2, deterministic_sessions_abort=2)


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
        # ADR-0002 decision 4: how many DETERMINISTIC failures each row has
        # accumulated. Independent of `claimed` (status), the way the real
        # `attempts` column is independent of `status`.
        self.attempts = {}

    def ensure_schema(self): self.calls.append("ensure_schema")

    def try_session_lock(self, **kw):
        return True

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
        key = (kw["session_id"], kw["last_message_id"])
        self.claimed[key] = "failed"
        if kw.get("count_attempt"):
            self.attempts[key] = self.attempts.get(key, 0) + 1
        self.marks.append(("failed", kw))
        return self.attempts.get(key, 0)

    def mark_quarantined(self, **kw):
        self.claimed[(kw["session_id"], kw["last_message_id"])] = "quarantined"
        self.marks.append(("quarantined", kw))

    def rollback_attempt(self, **kw):
        key = (kw["session_id"], kw["last_message_id"])
        self.attempts[key] = max(0, self.attempts.get(key, 0) - 1)
        self.claimed[key] = "failed"
        self.marks.append(("rollback", kw))

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

    def _enqueue(job, *args, job_id, **kw):
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


def test_a_dispatch_arq_refused_leaves_the_slice_re_runnable(hermes_db):
    """arq returns None when the job id is already known - including for a job
    that FAILED, because keep_result keeps a failed result key exactly as long
    as a successful one. Counting that as sent is a silent loss: it happened on
    the semitora host, twice, to the same two memories.
    """
    now = time.time()
    sessions, messages = rich_session(now)
    pg = FakePg()

    def refused(job, *args, job_id, **kw):
        raise RuntimeError(f"arq refused job id {job_id}")

    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), pg,
                           enqueue=refused)
    result = sw.sweep(deps, CFG)
    assert result["jobs"] == 0
    # extracted, not published: the next sweep re-dispatches from the payload,
    # and once the result key expires it goes through.
    assert pg.claimed[("s", 12)] == "extracted"
    assert pg.pending_dispatch(), "the slice must remain dispatchable"


def test_every_dispatched_job_carries_its_conversation_provenance(hermes_db):
    """Without these two fields nothing in a Qdrant point says WHICH conversation
    it came from: `source` is the constant "session" for every point the sweeper
    writes. A re-extraction that yields a different number of entries then
    orphans the old points with no filter that can select them.
    """
    now = time.time()
    sessions, messages = rich_session(now)
    seen = []

    def enqueue(job, *args, job_id, **kw):
        seen.append((kw.get("session_id"), kw.get("last_message_id")))
        return job_id

    deps, _, _ = make_deps(hermes_db(sessions=sessions, messages=messages), FakePg(),
                           enqueue=enqueue)
    sw.sweep(deps, CFG)
    assert seen == [("s", 12)]


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

    def boom(job, *args, job_id, **kw):
        raise RuntimeError("valkey down")

    deps, _, _ = make_deps(path, pg, enqueue=boom)
    sw.sweep(deps, CFG)
    assert pg.pending_dispatch(), "the slice must remain dispatchable"

    sent = []

    def ok(job, *args, job_id, **kw):
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

    def boom(job, *args, job_id, **kw):
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

    def try_session_lock(self, **kw):
        raise AssertionError("try_session_lock must not run under --dry-run")

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

    def mark_quarantined(self, **kw):
        raise AssertionError("mark_quarantined must not run under --dry-run")

    def rollback_attempt(self, **kw):
        raise AssertionError("rollback_attempt must not run under --dry-run")

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


# ── Task 4: classify failures, count only the deterministic ones ─────────

def transient_boom(transcript):
    raise extraction.ExtractionFailed("proxy down", transient=True)


def deterministic_boom(transcript):
    raise extraction.ExtractionFailed("bad model output", transient=False)


def three_eligible_sessions(now):
    """Three quiet, substantive sessions — same shape as `two_eligible_sessions`,
    with a third so the run-level breaker can be checked against a candidate
    that must never even be claimed."""
    sessions, messages, last_ids = [], [], {}
    for n in range(3):
        sid = f"s{n}"
        sessions.append(SESSION(sid, last_activity_at=now - 2 * HOUR, message_count=4))
        base = 100 * n
        for i in range(2):
            messages.append(MSG(base + 2 * i + 1, sid, "user", "u" * 60))
            messages.append(MSG(base + 2 * i + 2, sid, "assistant",
                                "decided. Result: works. " + "d" * 200))
        last_ids[sid] = base + 4
    return sessions, messages, last_ids


def test_a_transient_failure_marks_failed_without_incrementing_attempts(hermes_db):
    """ADR-0002 decision 4: an outage must not spend one of the tries a
    genuinely bad slice gets. Goes through the REAL extract_entries (as the
    FIX-1 tests do), not a fake that raises — a fake never distinguished
    transient from deterministic."""
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    deps, _, _ = make_deps(path, pg)
    deps.extract = real_extract_against(raising_opener)

    result = sw.sweep(deps, CFG)
    assert pg.claimed[("s", 12)] == "failed"
    assert pg.attempts.get(("s", 12), 0) == 0
    assert result["aborted"] is False
    failed = [kw for kind, kw in pg.marks if kind == "failed"]
    assert failed[-1]["count_attempt"] is False


def test_a_deterministic_failure_at_the_ceiling_quarantines(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    pg.attempts[("s", 12)] = CFG["max_attempts"] - 1
    deps, _, _ = make_deps(path, pg)
    deps.extract = deterministic_boom

    result = sw.sweep(deps, CFG)
    assert pg.claimed[("s", 12)] == "quarantined"
    assert result["quarantined"] == 1
    assert result["aborted"] is False
    assert pg.attempts[("s", 12)] == CFG["max_attempts"]


def test_a_quarantined_slice_advances_the_watermark_and_later_messages_are_offered(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    pg.attempts[("s", 12)] = CFG["max_attempts"] - 1
    deps, _, _ = make_deps(path, pg)
    deps.extract = deterministic_boom
    sw.sweep(deps, CFG)
    assert pg.claimed[("s", 12)] == "quarantined"
    # Only 'failed' is excluded from the watermark — the retired slice must
    # not block the session's later messages behind it forever.
    assert pg.watermarks() == {"s": 12}

    con = sqlite3.connect(path)
    con.execute(
        "INSERT INTO messages (id, session_id, role, content, tool_calls, tool_name,"
        " timestamp, active, compacted) VALUES (?, ?, ?, ?, '', NULL, ?, 1, 0)",
        (13, "s", "user", "w" * 60, now))
    con.execute(
        "INSERT INTO messages (id, session_id, role, content, tool_calls, tool_name,"
        " timestamp, active, compacted) VALUES (?, ?, ?, ?, '', NULL, ?, 1, 0)",
        (14, "s", "assistant", "decided once more. Result: works. " + "e" * 200, now))
    con.commit()
    con.close()

    deps2, _, _ = make_deps(path, pg)
    result2 = sw.sweep(deps2, CFG)
    assert result2["extracted"] == 1
    assert pg.claimed[("s", 14)] == "published"


def test_two_consecutive_transient_failures_abort_the_sweep(hermes_db):
    now = time.time()
    sessions, messages, _ = three_eligible_sessions(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    deps, _, _ = make_deps(path, pg)
    deps.extract = transient_boom

    result = sw.sweep(deps, CFG)
    assert result["aborted"] is True
    assert result["candidates"] == 3
    # Two candidates were claimed and failed; the third was never claimed —
    # not just left alone, genuinely untouched.
    assert len(pg.claimed) == 2
    assert all(status == "failed" for status in pg.claimed.values())
    assert all(pg.attempts.get(key, 0) == 0 for key in pg.claimed)


def test_deterministic_failures_in_two_different_sessions_abort_and_roll_back(hermes_db):
    """ADR-0002 decision 4, the half a per-slice classifier cannot see on its
    own: a proxy answering 200 with an unusable body is indistinguishable
    from bad model output per slice. Two DIFFERENT sessions failing the same
    way in one run is what separates a misrouting gateway from two unlucky
    slices — so both rows return to exactly the state they had before this
    run touched them."""
    now = time.time()
    sessions, messages, _ = two_eligible_sessions(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    deps, _, _ = make_deps(path, pg)
    deps.extract = deterministic_boom

    result = sw.sweep(deps, CFG)
    assert result["aborted"] is True
    assert result["quarantined"] == 0
    assert len(pg.claimed) == 2
    assert all(status == "failed" for status in pg.claimed.values())
    assert all(pg.attempts.get(key, 0) == 0 for key in pg.claimed)


def test_a_quarantine_earlier_in_the_run_is_reverted_by_the_cross_session_breaker(hermes_db):
    """Finding 2 (fix round 1): `rollback_attempt` un-retires every row this
    run counted, but the run's own `stats["quarantined"]` must be reverted
    with it — or `sweeper_status.quarantined` reports a slice as retired when
    its row is actually back to plain 'failed'. Session `s0` sorts first
    (older last_activity_at) and is preloaded one short of the ceiling, so its
    slice is claimed and fails FIRST, reaching the ceiling and quarantining —
    before `s1`'s failure trips the cross-session breaker and rolls both back."""
    now = time.time()
    sessions = [SESSION("s0", last_activity_at=now - 3 * HOUR, message_count=4),
               SESSION("s1", last_activity_at=now - 2 * HOUR, message_count=4)]
    messages = []
    for n, sid in enumerate(("s0", "s1")):
        base = 100 * n
        messages.append(MSG(base + 1, sid, "user", "u" * 60))
        messages.append(MSG(base + 2, sid, "assistant", "decided. Result: works. " + "d" * 200))
        messages.append(MSG(base + 3, sid, "user", "u" * 60))
        messages.append(MSG(base + 4, sid, "assistant", "decided. Result: works. " + "d" * 200))
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    pg.attempts[("s0", 4)] = CFG["max_attempts"] - 1
    deps, _, _ = make_deps(path, pg)
    deps.extract = deterministic_boom

    result = sw.sweep(deps, CFG)
    assert result["aborted"] is True
    assert result["quarantined"] == 0
    assert pg.claimed[("s0", 4)] == "failed"
    assert pg.attempts[("s0", 4)] == CFG["max_attempts"] - 1


def test_a_deterministic_failure_reaches_quarantine_after_max_attempts_runs(hermes_db):
    """The other half of the ceiling: a genuinely bad slice, alone in its
    session across separate runs, still gets retired — the cross-session
    breaker only fires on failures spread across DIFFERENT sessions."""
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    pg = FakePg()
    for i in range(CFG["max_attempts"]):
        deps, _, _ = make_deps(path, pg)
        deps.extract = deterministic_boom
        result = sw.sweep(deps, CFG)
        if i < CFG["max_attempts"] - 1:
            assert result["aborted"] is False
            assert pg.claimed[("s", 12)] == "failed"
        else:
            assert pg.claimed[("s", 12)] == "quarantined"
            assert result["quarantined"] == 1
    assert pg.attempts[("s", 12)] == CFG["max_attempts"]


# ── Task 5: one sweeper per session, released by the transaction ──────────

class LockRefusedPg(FakePg):
    """Simulates another process already holding the advisory lock for one
    session. try_session_lock refusing must skip only that candidate — the
    others still run, and the skip is counted."""

    def __init__(self, refused_session, **kw):
        super().__init__(**kw)
        self.refused_session = refused_session

    def try_session_lock(self, **kw):
        return kw["session_id"] != self.refused_session


def test_a_locked_out_session_is_skipped_and_counted_but_others_still_run(hermes_db):
    now = time.time()
    sessions, messages, last_ids = two_eligible_sessions(now)
    pg = LockRefusedPg("s0")
    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    result = sw.sweep(deps, CFG)
    assert result["candidates"] == 2
    assert result["locked_out"] == 1
    assert result["extracted"] == 1
    # s0 was never claimed at all — another process owns its lock.
    assert ("s0", last_ids["s0"]) not in pg.claimed
    assert pg.claimed[("s1", last_ids["s1"])] == "published"


class LockBoomOnFirst(FakePg):
    """Like ClaimBoomOnFirst, but the round trip that fails is the lock
    itself — a dropped connection, not a lost race. Fail-open per slice."""

    def __init__(self, boom_session, **kw):
        super().__init__(**kw)
        self.boom_session = boom_session

    def try_session_lock(self, **kw):
        if kw["session_id"] == self.boom_session:
            raise RuntimeError("db down")
        return super().try_session_lock(**kw)


def test_a_lock_failure_does_not_stop_other_candidates(hermes_db):
    now = time.time()
    sessions, messages, last_ids = two_eligible_sessions(now)
    pg = LockBoomOnFirst("s0")
    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    result = sw.sweep(deps, CFG)
    assert result["candidates"] == 2
    assert result["extracted"] == 1
    assert ("s0", last_ids["s0"]) not in pg.claimed
    assert pg.claimed[("s1", last_ids["s1"])] == "published"


class WatermarksBoomOnSecondCallPg(FakePg):
    """The watermark RE-READ is a Postgres round trip too, and fix round 1
    found it sitting bare between try_session_lock and claim() — a dropped
    connection there escaped sweep() entirely and ended the whole run,
    losing every remaining candidate instead of deferring just this one.

    watermarks() is called once before the loop (building `marks`) and once
    per candidate after locking (the re-read), so the SECOND call overall is
    the first candidate's re-read — that is the one this raises on, proving
    the second candidate still runs to completion."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self._calls = 0

    def watermarks(self):
        self._calls += 1
        if self._calls == 2:
            raise RuntimeError("db down")
        return super().watermarks()


def test_a_watermark_reread_failure_does_not_stop_other_candidates(hermes_db):
    now = time.time()
    sessions, messages, last_ids = two_eligible_sessions(now)
    pg = WatermarksBoomOnSecondCallPg()
    deps, written, jobs = make_deps(hermes_db(sessions=sessions, messages=messages), pg)
    result = sw.sweep(deps, CFG)
    assert result["candidates"] == 2
    assert result["locked_out"] == 1
    assert result["extracted"] == 1
    # s0's re-read raised before any row was claimed for it.
    assert ("s0", last_ids["s0"]) not in pg.claimed
    assert pg.claimed[("s1", last_ids["s1"])] == "published"


class StaleAfterLockPg(FakePg):
    """Reproduces the race decision 3 exists for: two sweeps both read
    watermarks() before either locks, so they build slices against the SAME
    stale watermark. The other sweep wins the lock first, claims, extracts
    and publishes — advancing the watermark. By the time THIS process's
    try_session_lock succeeds, a re-read must see that its slice is already
    covered. A lock with no re-read would miss this entirely: this slice's
    last_message_id differs from the other sweep's, so the UNIQUE constraint
    never fires and both would win."""

    def __init__(self, advance_to, **kw):
        super().__init__(**kw)
        self._advance_to = advance_to
        self._locked = False

    def try_session_lock(self, **kw):
        self._locked = True
        return True

    def watermarks(self):
        marks = super().watermarks()
        if self._locked:
            marks["s"] = self._advance_to
        return marks


def test_a_slice_built_against_a_stale_watermark_is_dropped_after_locking(hermes_db):
    now = time.time()
    sessions, messages = rich_session(now)
    path = hermes_db(sessions=sessions, messages=messages)
    # rich_session's slice is first_id=1, last_id=12. advance_to must sit
    # STRICTLY between the two (not just >= first_id, which 12 also
    # satisfies) or a regression that compared against last_id instead of
    # first_id — the exact mistake decision 3 exists to catch — would still
    # pass this test (fix round 1, Finding 2).
    pg = StaleAfterLockPg(advance_to=6)
    deps, written, jobs = make_deps(path, pg)
    result = sw.sweep(deps, CFG)
    assert result["extracted"] == 0
    assert result["stale_slices"] == 1
    assert written == [] and jobs == []
    # Never claimed either — the other sweep already owns this slice.
    assert ("s", 12) not in pg.claimed
