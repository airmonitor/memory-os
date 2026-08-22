"""Which ingestion failures deserve another attempt — and the `Retry` that says so.

arq 0.28 retries a job ONLY on `Retry` or `CancelledError` (`retry_jobs` in
`arq/worker.py`); an ordinary exception finishes the job as failed and nothing
ever looks at it again. That matters here because of where the producer's
bookkeeping ends: `session_sweeper` marks a slice `published` the moment Valkey
accepts the job, and its redispatch pass reads only `extracted` rows. Once the
job is queued, the sweeper cannot reach it. So an embedding timeout or a Qdrant
503 used to write the fabric entry, lose the Qdrant point, and leave every
counter reading healthy — a transient outage converted into permanent memory
loss, which is the one failure mode this whole component exists to prevent.

The classification mirrors `icarus.extraction.ExtractionFailed`'s, because the
two halves of one pipeline disagreeing about what "transient" means is its own
bug: transport errors, timeouts, HTTP status errors and a 200 whose body is not
what was asked for are all weather. Only a job whose PAYLOAD is wrong — empty
text, a kwarg this worker does not accept — is deterministic, because a fifth
attempt finds it just as wrong.

Fail-open is the default in the retry direction: an exception this module does
not recognise is retried. arq's `max_tries` (5) is the ceiling that keeps that
honest.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("cognitive-worker.transient")

# 1 min, doubling, capped at 15 — the same shape as the sweeper's own backoff
# (`session_store.RETRY_BACKOFF_BASE`), scaled down because arq allows 5 tries
# and the sweeper's cadence is 15 minutes.
RETRY_DELAY_BASE = 60
RETRY_DELAY_CAP = 900

# Wrong-payload failures. A retry cannot fix any of them.
_DETERMINISTIC = (ValueError, TypeError, KeyError, AttributeError, IndexError)


def _status_code(exc) -> int | None:
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None)
    # qdrant_client's UnexpectedResponse carries it directly.
    if code is None:
        code = getattr(exc, "status_code", None)
    return int(code) if isinstance(code, int) else None


def is_transient(exc: BaseException) -> bool:
    """True if another attempt could plausibly succeed."""
    if isinstance(exc, _DETERMINISTIC):
        return False
    return True


def is_configuration_error(exc: BaseException) -> bool:
    """A 4xx that is not 429 — a rotated key, a renamed model, a bad request.

    Not a separate retry decision (it still retries, fail-open), just the one
    thing an operator needs told: this will not fix itself.
    """
    code = _status_code(exc)
    return code is not None and 400 <= code < 500 and code != 429


def retry_delay(job_try: int) -> int:
    """Seconds to wait before attempt `job_try + 1`."""
    exponent = max(0, int(job_try) - 1)
    # Cap the exponent before the multiplication, not after: the same lesson
    # ADR-0003 records for the SQL backoff, where an uncapped POWER overflowed
    # the interval type before LEAST could save it.
    exponent = min(exponent, 8)
    return min(RETRY_DELAY_BASE * (2 ** exponent), RETRY_DELAY_CAP)


def retry_or_raise(exc: BaseException, *, job_try: int):
    """Convert a transient failure into an arq `Retry`; re-raise anything else.

    Imported lazily so this module stays importable without arq — the tests
    load it by path, and `is_transient` is a pure function.
    """
    if not is_transient(exc):
        raise exc

    from arq.worker import Retry

    delay = retry_delay(job_try)
    if is_configuration_error(exc):
        logger.error("ingestion failed with a configuration error (%s) — retrying in %ss, "
                     "but this will not fix itself", exc, delay)
    else:
        logger.warning("ingestion failed transiently (%s) — retrying in %ss (attempt %s)",
                       exc, delay, job_try)
    raise Retry(defer=delay) from exc
