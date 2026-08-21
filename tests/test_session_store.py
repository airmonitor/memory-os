import pytest
from scripts import session_store


class FakeCursor:
    def __init__(self, log, results):
        self.log, self.results, self._row = log, results, None

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))
        self._row = self.results.pop(0) if self.results else None
        return self

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._row or []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, results=None):
        self.log, self.results, self.commits = [], list(results or []), 0

    def cursor(self):
        return FakeCursor(self.log, self.results)

    def commit(self):
        self.commits += 1


def test_claim_returns_true_when_the_row_is_new():
    conn = FakeConn(results=[(1,)])
    assert session_store.claim(conn, session_id="s", first_message_id=1,
                               last_message_id=9, message_count=5) is True
    sql, params = conn.log[0]
    assert "ON CONFLICT" in sql and "DO NOTHING" in sql
    assert params == ("s", 1, 9, 5)
    assert conn.commits == 1


def test_claim_returns_false_when_another_sweeper_owns_the_slice():
    conn = FakeConn(results=[None])
    assert session_store.claim(conn, session_id="s", first_message_id=1,
                               last_message_id=9, message_count=5) is False


def test_watermarks_uses_the_max_published_or_claimed_id_per_session():
    conn = FakeConn(results=[[("s1", 12), ("s2", 4)]])
    assert session_store.watermarks(conn) == {"s1": 12, "s2": 4}
    sql, _ = conn.log[0]
    assert "MAX(last_message_id)" in sql and "GROUP BY session_id" in sql


def test_mark_published_records_the_job_ids():
    conn = FakeConn(results=[None])
    session_store.mark_published(conn, session_id="s", last_message_id=9,
                                 jobs=["ingest:s:9:0"])
    sql, params = conn.log[0]
    assert "status = 'published'" in sql
    assert params[0] == '["ingest:s:9:0"]' or params[0] == ["ingest:s:9:0"]


def test_a_stale_claim_can_be_reclaimed_but_a_fresh_one_cannot():
    conn = FakeConn(results=[(1,)])
    session_store.claim(conn, session_id="s", first_message_id=1, last_message_id=9,
                        message_count=5, stale_hours=2)
    sql, params = conn.log[0]
    assert "DO UPDATE" in sql
    assert "status = 'failed'" in sql and "make_interval" in sql
    assert params[-1] == 2


def test_pending_dispatch_returns_the_payload_for_re_dispatch():
    conn = FakeConn(results=[[("s", 9, [{"job_id": "ingest:s:9:0", "text": "c"}])]])
    assert session_store.pending_dispatch(conn) == [
        {"session_id": "s", "last_message_id": 9,
         "payload": [{"job_id": "ingest:s:9:0", "text": "c"}]}]


def test_ensure_schema_is_idempotent_sql():
    conn = FakeConn()
    session_store.ensure_schema(conn)
    joined = " ".join(sql for sql, _ in conn.log)
    assert joined.count("CREATE TABLE IF NOT EXISTS") == 2
    assert "UNIQUE (session_id, last_message_id)" in joined
