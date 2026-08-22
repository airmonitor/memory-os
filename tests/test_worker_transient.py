"""The worker's half of "an outage must not become permanent memory loss".

The sweeper already refuses to retire a slice on a transient failure
(ADR-0002 decision 4). But it marks the row `published` as soon as Valkey
accepts the job, and `pending_dispatch()` reads only `extracted` rows — so
once the job is queued, the sweeper has no way back to it. arq 0.28 retries a
job ONLY on `Retry` or `CancelledError`; an ordinary exception finishes it as
failed, forever. An embedding timeout or a Qdrant 503 therefore lands the
entry in fabric and never in Qdrant, silently.

These tests pin the classification and the `Retry` that follows from it.
"""
import asyncio
import importlib.util
from pathlib import Path

import httpx
import pytest
from arq.worker import Retry

_MODULE = Path(__file__).resolve().parent.parent / "docker" / "worker" / "transient.py"
_spec = importlib.util.spec_from_file_location("worker_transient", _MODULE)
transient = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(transient)


def _status_error(code):
    request = httpx.Request("POST", "http://litellm/v1/embeddings")
    response = httpx.Response(code, request=request)
    return httpx.HTTPStatusError("boom", request=request, response=response)


@pytest.mark.parametrize("exc", [
    httpx.ConnectError("connection refused"),
    httpx.ReadTimeout("timed out"),
    httpx.RemoteProtocolError("server disconnected"),
    _status_error(500),
    _status_error(502),
    _status_error(429),
    ConnectionResetError("peer reset"),
    asyncio.TimeoutError(),
    # embedding.py raises this for a 200 whose body is not an embedding —
    # the same "HTTP-200 door a misrouting proxy walks through" the sweeper
    # classifies as transient.
    RuntimeError("unexpected embedding response shape"),
])
def test_transient_failures_are_retried(exc):
    assert transient.is_transient(exc) is True


@pytest.mark.parametrize("exc", [
    # ingest_memory's own guard: an empty payload is a bad job, and a fifth
    # attempt will find it just as empty.
    ValueError("memory_text cannot be empty"),
    # a producer sending a kwarg this worker does not accept — the ordering
    # trap of 2026-08-22. Retrying cannot make an old worker younger.
    TypeError("process_ingestion() got an unexpected keyword argument 'point_id'"),
    KeyError("qdrant"),
])
def test_payload_shaped_failures_are_not_retried(exc):
    assert transient.is_transient(exc) is False


def test_a_configuration_error_still_retries_but_is_named():
    """Fail-open, exactly as the sweeper does it.

    A rotated key (401) or a renamed model (404) will not fix itself, but a
    memory system must not drop conversations over a typo. It retries, and it
    says so.
    """
    exc = _status_error(401)
    assert transient.is_transient(exc) is True
    assert transient.is_configuration_error(exc) is True
    assert transient.is_configuration_error(_status_error(429)) is False
    assert transient.is_configuration_error(_status_error(503)) is False


def test_delay_backs_off_and_is_capped():
    delays = [transient.retry_delay(t) for t in range(1, 9)]
    assert delays[0] < delays[1] < delays[2]
    assert all(d <= transient.RETRY_DELAY_CAP for d in delays)
    assert delays[-1] == transient.RETRY_DELAY_CAP


def test_retry_or_raise_defers_a_transient_failure():
    with pytest.raises(Retry) as caught:
        transient.retry_or_raise(httpx.ReadTimeout("timed out"), job_try=2)
    assert caught.value.defer_score == transient.retry_delay(2) * 1000


def test_retry_or_raise_reraises_a_deterministic_failure():
    original = ValueError("memory_text cannot be empty")
    with pytest.raises(ValueError) as caught:
        transient.retry_or_raise(original, job_try=1)
    assert caught.value is original
