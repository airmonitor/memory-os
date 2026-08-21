import io
import json
import pytest
from icarus import extraction


def fake_opener(payload, *, capture=None):
    def _open(req, timeout=None):
        if capture is not None:
            capture["timeout"] = timeout
            capture["url"] = req.full_url
            capture["body"] = json.loads(req.data.decode())
        return io.BytesIO(json.dumps(
            {"choices": [{"message": {"content": payload}}]}).encode())
    return _open


def test_entries_are_parsed_from_a_fenced_json_array():
    raw = '```json\n[{"type": "decision", "summary": "s", "content": "c", ' \
          '"training_value": "high"}]\n```'
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k",
                                     model="m", max_tokens=100, timeout=5,
                                     opener=fake_opener(raw))
    assert out == [{"type": "decision", "summary": "s", "content": "c",
                    "training_value": "high"}]


def test_the_configured_timeout_reaches_the_http_call():
    capture = {}
    extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                               max_tokens=100, timeout=42,
                               opener=fake_opener("[]", capture=capture))
    assert capture["timeout"] == 42
    assert capture["url"] == "http://x/v1/chat/completions"


def test_malformed_output_yields_no_entries_and_does_not_raise():
    assert extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                      max_tokens=10, timeout=5,
                                      opener=fake_opener("not json at all")) == []


def test_entries_missing_required_fields_are_dropped():
    raw = json.dumps([{"type": "decision"}, {"type": "note", "summary": "s", "content": "c"}])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert [e["summary"] for e in out] == ["s"]


def test_no_api_key_means_no_call():
    def explode(*a, **k):
        raise AssertionError("must not call the gateway without a key")
    assert extraction.extract_entries("t", base_url="http://x/v1", api_key="",
                                      model="m", max_tokens=10, timeout=5,
                                      opener=explode) == []
