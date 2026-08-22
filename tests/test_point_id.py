import uuid
from scripts import session_sweeper as sw


def test_point_id_is_a_pure_function_of_the_job_id():
    a = sw.point_id("ingest:s:12:0")
    assert a == sw.point_id("ingest:s:12:0")
    assert a != sw.point_id("ingest:s:12:1")
    uuid.UUID(a)
