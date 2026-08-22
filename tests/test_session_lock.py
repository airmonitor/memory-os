from scripts import session_store
from tests.test_session_store import FakeConn


def test_the_lock_is_transaction_scoped_and_keyed_on_the_session():
    conn = FakeConn(results=[(True,)])
    assert session_store.try_session_lock(conn, "s1") is True
    sql, params = conn.log[0]
    assert "pg_try_advisory_xact_lock" in sql
    assert params == ("s1",)
    # A session-scoped lock would outlive a hung process; an xact lock cannot.
    assert "pg_try_advisory_lock(" not in sql


def test_a_lost_lock_is_reported_not_raised():
    conn = FakeConn(results=[(False,)])
    assert session_store.try_session_lock(conn, "s1") is False


def test_the_lock_does_not_commit_its_own_transaction():
    """A commit here would release the xact-scoped lock immediately, before
    claim() ever runs in the same transaction — defeating decision 3 entirely.
    The lock must ride along with whatever the caller commits next."""
    conn = FakeConn(results=[(True,)])
    session_store.try_session_lock(conn, "s1")
    assert conn.commits == 0
