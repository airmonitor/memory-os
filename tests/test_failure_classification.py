import io
import pytest
from icarus import extraction


def raising(exc):
    def _open(req, timeout=None):
        raise exc
    return _open


def test_a_timeout_is_transient():
    with pytest.raises(extraction.ExtractionFailed) as e:
        extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                   max_tokens=10, timeout=1, opener=raising(TimeoutError("slow")))
    assert e.value.transient is True


def test_a_missing_key_is_transient_because_it_is_configuration_not_content():
    with pytest.raises(extraction.ExtractionFailed) as e:
        extraction.extract_entries("t", base_url="http://x/v1", api_key="", model="m",
                                   max_tokens=10, timeout=1)
    assert e.value.transient is True


def test_unparseable_model_output_is_deterministic():
    # The gateway answered like a gateway; the MODEL's content is the unusable part.
    def opener(req, timeout=None):
        return io.BytesIO(b'{"choices": [{"message": {"content": "not json"}}]}')
    with pytest.raises(extraction.ExtractionFailed) as e:
        extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                   max_tokens=10, timeout=1, opener=opener)
    assert e.value.transient is False


def test_a_body_that_is_not_a_chat_completion_is_transient():
    # A misrouting proxy returns 200 with an HTML error page. That is an outage
    # wearing a 200, and counting it toward retirement is how an outage becomes
    # permanent memory loss.
    def opener(req, timeout=None):
        return io.BytesIO(b"<html><body>502 upstream</body></html>")
    with pytest.raises(extraction.ExtractionFailed) as e:
        extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                   max_tokens=10, timeout=1, opener=opener)
    assert e.value.transient is True
