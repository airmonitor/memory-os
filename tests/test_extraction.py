from icarus import extraction
from icarus.hermes_state import Message


def M(id, role, content="", tool_calls="", tool_name=None):
    return Message(id, role, content, tool_calls, tool_name, None, 0)


def test_tool_call_rows_become_markers_not_holes():
    t = extraction.build_transcript([
        M(1, "user", "read the config"),
        M(2, "assistant", "", tool_calls='[{"function": {"name": "read_file"}}]'),
        M(3, "tool", "line one\nline two", tool_name="read_file"),
        M(4, "assistant", "it says two lines"),
    ])
    assert "read the config" in t
    assert "[tool: read_file]" in t
    assert "line one" in t
    assert "it says two lines" in t


def test_context_messages_are_marked_and_precede_the_slice():
    t = extraction.build_transcript([M(9, "user", "new question")],
                                    context=[M(8, "assistant", "earlier answer")])
    assert t.index("earlier answer") < t.index("new question")
    assert "CONTEXT" in t


def test_exchanges_pair_user_with_the_following_assistant_text():
    ex = extraction.messages_to_exchanges([
        M(1, "user", "q1"), M(2, "assistant", "a1"),
        M(3, "tool", "ignored by pairing"), M(4, "assistant", "a1 continued"),
        M(5, "user", "q2"), M(6, "assistant", "a2"),
    ])
    assert [e["user"] for e in ex] == ["q1", "q2"]
    assert ex[0]["assistant"] == "a1\na1 continued"


def test_score_rises_with_substance_and_stays_low_for_chatter():
    chatter = [{"user": "hi", "assistant": "hello"}]
    assert extraction.score_exchanges(chatter)["total"] < 0.2

    real = [{"user": "u" * 60, "assistant": "we decided to use X. Result: it works. " + "d" * 200}
            for _ in range(5)]
    assert extraction.score_exchanges(real, recall_usage=0.5, linked_entries=2)["total"] >= 0.2


def test_scoring_never_divides_by_zero_on_an_empty_slice():
    assert extraction.score_exchanges([])["total"] == 0.0
