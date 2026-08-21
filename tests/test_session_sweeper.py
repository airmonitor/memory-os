import sqlite3
import time

import pytest

from icarus import hermes_state as hs
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

    def ensure_schema(self): self.calls.append("ensure_schema")

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

    def _enqueue(job, *args, job_id):
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

    def boom(job, *args, job_id):
        raise RuntimeError("valkey down")

    deps, _, _ = make_deps(path, pg, enqueue=boom)
    sw.sweep(deps, CFG)
    assert pg.pending_dispatch(), "the slice must remain dispatchable"

    sent = []

    def ok(job, *args, job_id):
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

    def boom(job, *args, job_id):
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
