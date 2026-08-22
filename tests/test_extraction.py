from icarus import extraction, state
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


def test_shared_regex_objects_are_identical():
    """Prevent DECISION_RE and OUTCOME_RE from drifting between modules."""
    assert state.DECISION_RE is extraction.DECISION_RE
    assert state.OUTCOME_RE is extraction.OUTCOME_RE


def test_agent_initiated_turn_starts_anonymous_exchange():
    """Assistant content before the first user message is preserved in an anonymous exchange."""
    ex = extraction.messages_to_exchanges([
        M(1, "assistant", "here's a thought"),
        M(2, "user", "thanks"),
        M(3, "assistant", "you're welcome"),
    ])
    assert len(ex) == 2
    assert ex[0]["user"] == ""
    assert ex[0]["assistant"] == "here's a thought"
    assert ex[1]["user"] == "thanks"
    assert ex[1]["assistant"] == "you're welcome"


def test_polish_decision_language_scores_like_english():
    """DECISION_RE carries the heaviest weight of the five and was English-only.

    Measured on the reference host 2026-08-22: it matched on none of the 26
    slices the sweeper had consumed, so 3 of 10 weight points were a fixed tax
    rather than a signal (issue #20)."""
    pl = [{"user": "u" * 60,
           "assistant": "Zdecydowaliśmy się na wariant B, ponieważ " + "d" * 200}]
    en = [{"user": "u" * 60,
           "assistant": "We decided on variant B, because " + "d" * 200}]
    assert extraction.score_exchanges(pl)["decision"] == 1.0
    assert extraction.score_exchanges(pl)["decision"] == extraction.score_exchanges(en)["decision"]


def test_bare_bo_does_not_fire_the_outcome_regex():
    """`bo` is the commonest Polish causal conjunction; matching it would make
    OUTCOME_RE fire on nearly every transcript. `ponieważ` carries the same
    meaning without being a two-letter substring of ordinary prose."""
    assert not extraction.OUTCOME_RE.search("nie wiem bo tak")
    assert extraction.OUTCOME_RE.search("nie wiem, ponieważ tak")


def test_a_component_the_caller_cannot_measure_abstains_instead_of_scoring_zero():
    """None is not 0.0. Zero means "measured, and it was zero"; None means the
    caller cannot see it, and such a component must leave the DENOMINATOR too --
    otherwise dropping it is indistinguishable from scoring it a failure."""
    ex = [{"user": "u" * 60, "assistant": "Zdecydowaliśmy. Result: ok. " + "d" * 200}]
    abstained = extraction.score_exchanges(ex)
    measured_zero = extraction.score_exchanges(ex, recall_usage=0.0, linked_entries=0)
    assert "recall_usage" not in abstained and "linked_entries" not in abstained
    assert abstained["total"] > measured_zero["total"]
    # 6 weight points instead of 10, same numerator.
    assert abstained["total"] == round(measured_zero["total"] * 10 / 6, 2)


def test_an_agentic_session_can_reach_the_threshold():
    """THE SHAPE THAT WAS UNSCOREABLE. An agent doing autonomous work produces
    one user instruction, dozens of tool calls with empty content, and one
    substantive answer -- so `depth` is pinned at 1/5 by construction. Measured
    on the reference host, seven such sessions scored an identical 0.0733 and
    were consumed for zero memories: 320 of 548 messages, 58% of the corpus."""
    agentic = ([{"user": "Przeanalizuj to zadanie. " + "u" * 240, "assistant": ""}]
               + [{"user": "", "assistant": ""} for _ in range(25)]
               + [{"user": "", "assistant": "Zdecydowałem: wariant B, ponieważ " + "d" * 900}])
    assert extraction.score_exchanges(agentic)["total"] >= 0.2


def test_two_message_chatter_still_fails_after_the_rescale():
    """The rescale must not buy the threshold's job. Measured across all 29
    sessions in the reference host's state.db, every session of four messages
    or fewer stays at 0.07 or below."""
    for text in ("cześć", "dzięki", "ok, naprawiłem"):
        assert extraction.score_exchanges([{"user": text, "assistant": text}])["total"] < 0.2
