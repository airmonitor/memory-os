import io
import json
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


# A summary/content pair long enough to survive _validate_entries' minimum
# lengths (summary >= 10 chars, content >= 60 chars after stripping).
SUMMARY = "a sufficiently long summary line"
CONTENT = ("## Context\nSomething happened during the session that is worth "
           "recording.\n## Outcome\nIt was resolved.")


def _entry(**overrides):
    entry = {"type": "decision", "summary": SUMMARY, "content": CONTENT,
             "training_value": "high"}
    entry.update(overrides)
    return entry


def test_entries_are_parsed_from_a_fenced_json_array():
    raw = "```json\n" + json.dumps([_entry()]) + "\n```"
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k",
                                     model="m", max_tokens=100, timeout=5,
                                     opener=fake_opener(raw))
    assert out == [_entry()]


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
    raw = json.dumps([{"type": "decision"}, _entry(summary=SUMMARY, content=CONTENT)])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert [e["summary"] for e in out] == [SUMMARY]


def test_no_api_key_means_no_call():
    def explode(*a, **k):
        raise AssertionError("must not call the gateway without a key")
    assert extraction.extract_entries("t", base_url="http://x/v1", api_key="",
                                      model="m", max_tokens=10, timeout=5,
                                      opener=explode) == []


# ── Restored guarantees (fix round 1) ──────────────────────────────────────

def test_an_entries_wrapper_is_unwrapped():
    raw = json.dumps({"entries": [_entry()]})
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert out == [_entry()]


def test_a_results_wrapper_is_unwrapped():
    raw = json.dumps({"results": [_entry()]})
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert out == [_entry()]


def test_a_bare_single_object_is_unwrapped():
    raw = json.dumps(_entry())
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert out == [_entry()]


def test_a_too_short_summary_is_dropped():
    raw = json.dumps([_entry(summary="short")])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert out == []


def test_too_short_content_is_dropped():
    raw = json.dumps([_entry(content="too short")])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert out == []


def test_an_overlong_summary_and_content_are_truncated():
    raw = json.dumps([_entry(summary="s" * 200, content="c" * 3000)])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert len(out) == 1
    assert out[0]["summary"] == "s" * 80
    assert out[0]["content"] == "c" * 2000


def test_an_unknown_type_comes_back_as_note_with_the_entry_intact():
    raw = json.dumps([_entry(type="mystery")])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert out == [_entry(type="note")]


def test_a_missing_training_value_defaults_to_normal():
    entry = _entry()
    del entry["training_value"]
    raw = json.dumps([entry])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert out == [_entry(training_value="normal")]
