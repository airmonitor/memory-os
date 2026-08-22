import sqlite3
from tests.conftest import SESSION, MSG


def test_fixture_has_the_columns_the_reader_depends_on(hermes_db):
    path = hermes_db(sessions=[SESSION("s1")], messages=[MSG(1, "s1", "user", "hi")])
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    cols = {r[1] for r in con.execute("PRAGMA table_info(messages)")}
    assert {"id", "session_id", "role", "content", "tool_calls", "active", "compacted"} <= cols
    assert con.execute("SELECT version FROM schema_version").fetchone()[0] == 26
