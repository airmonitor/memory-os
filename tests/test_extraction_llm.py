import io
import json

import pytest

from icarus import extraction
from icarus.extraction import ExtractionFailed


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


def test_malformed_output_raises_instead_of_looking_like_an_empty_session():
    """This test used to assert `== []`, and that assertion WAS the Critical bug:
    on the sweeper path an empty list marks the slice extracted and published,
    advancing the watermark past a conversation nobody ever read."""
    with pytest.raises(ExtractionFailed):
        extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                   max_tokens=10, timeout=5,
                                   opener=fake_opener("not json at all"))


def test_entries_missing_required_fields_are_dropped():
    raw = json.dumps([{"type": "decision"}, _entry(summary=SUMMARY, content=CONTENT)])
    out = extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                     max_tokens=10, timeout=5, opener=fake_opener(raw))
    assert [e["summary"] for e in out] == [SUMMARY]


def test_no_api_key_means_no_call_and_no_silent_empty_result():
    """No key still means no gateway call — but it must not read as "nothing
    worth keeping" either, or a misconfigured host burns its entire backlog
    three slices per sweep while every counter reads healthy."""
    def explode(*a, **k):
        raise AssertionError("must not call the gateway without a key")
    with pytest.raises(ExtractionFailed):
        extraction.extract_entries("t", base_url="http://x/v1", api_key="",
                                   model="m", max_tokens=10, timeout=5,
                                   opener=explode)


# ── The empty-vs-failed distinction (fix round 2) ──────────────────────────

def test_a_genuine_empty_array_is_still_an_empty_list():
    """The one case that must NOT raise: the model read the transcript and said
    there is nothing here. Everything else raising is only safe if this does not."""
    assert extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                      max_tokens=10, timeout=5,
                                      opener=fake_opener("[]")) == []


def test_a_transport_failure_raises_extraction_failed():
    def timing_out(req, timeout=None):
        raise TimeoutError("the read operation timed out")
    with pytest.raises(ExtractionFailed) as exc:
        extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                   max_tokens=10, timeout=5, opener=timing_out)
    # The cause is chained so mark_failed's stored error is diagnostic.
    assert isinstance(exc.value.__cause__, TimeoutError)


def test_a_response_that_is_not_a_chat_completion_raises():
    def wrong_shape(req, timeout=None):
        return io.BytesIO(json.dumps({"error": "model not found"}).encode())
    with pytest.raises(ExtractionFailed):
        extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                   max_tokens=10, timeout=5, opener=wrong_shape)


def test_entries_dropped_by_validation_are_not_a_failure():
    """The model was asked and it answered; the answer was junk. That is
    "nothing worth keeping", not "the call did not happen"."""
    raw = json.dumps([{"type": "decision", "summary": "x", "content": "y"}])
    assert extraction.extract_entries("t", base_url="http://x/v1", api_key="k", model="m",
                                      max_tokens=10, timeout=5,
                                      opener=fake_opener(raw)) == []


def test_parse_json_robust_stays_lenient_for_the_plugin_surface():
    assert extraction.parse_json_robust("not json at all") == []


# ── Transcript budget (fix round 2) ────────────────────────────────────────

def test_an_overlong_transcript_keeps_its_tail_and_says_what_it_dropped():
    """A 35-message Slack thread runs 15-20k chars; `transcript[:8000]` kept the
    greeting and threw away the outcome, while scoring ran on the whole slice."""
    text = "HEAD-GREETING" + ("x" * 20000) + "TAIL-OUTCOME"
    out = extraction.clamp_transcript(text)
    assert out.endswith("TAIL-OUTCOME")
    assert "HEAD-GREETING" not in out
    dropped = len(text) - extraction.TRANSCRIPT_MAX_CHARS
    assert f"{dropped} earlier characters elided" in out


def test_a_transcript_within_budget_is_untouched():
    assert extraction.clamp_transcript("short") == "short"


def test_the_clamped_transcript_is_what_reaches_the_request_body():
    capture = {}
    text = "HEAD-GREETING" + ("x" * 20000) + "TAIL-OUTCOME"
    extraction.extract_entries(text, base_url="http://x/v1", api_key="k", model="m",
                               max_tokens=10, timeout=5,
                               opener=fake_opener("[]", capture=capture))
    sent = capture["body"]["messages"][1]["content"]
    assert sent.endswith("TAIL-OUTCOME")
    assert "elided" in sent


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
