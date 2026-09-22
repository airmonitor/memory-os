"""The full reflection must keep the structure of a fenced JSON reply (local Qwen models fence it)."""
import importlib.util
from pathlib import Path

import pytest

_MODULE = Path(__file__).resolve().parent.parent / "docker" / "worker" / "json_reply.py"
_spec = importlib.util.spec_from_file_location("worker_json_reply", _MODULE)
json_reply = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(json_reply)

OBJ = '{"patterns": [], "connections": [], "insights": ["x"], "actions": []}'


@pytest.mark.parametrize("reply", [
    OBJ,                                   # gpt-5.6-luna: bare object
    "\n\n```json\n" + OBJ + "\n```",      # lm-studio-qwen3.6 / qwen3.8-splash, measured 2026-09-22
    "```\n" + OBJ + "\n```\n",            # fence without a language tag
    "Here is the analysis:\n" + OBJ,       # prose before the object
])
def test_reply_shapes_parse_to_the_same_object(reply):
    assert json_reply.parse_json_reply(reply)["insights"] == ["x"]


def test_no_json_raises_value_error():
    with pytest.raises(ValueError):
        json_reply.parse_json_reply("I could not analyse these memories.")


def test_json_decode_error_is_a_value_error():
    # reflection.py catches ValueError; json.JSONDecodeError subclasses it, so both paths land there.
    with pytest.raises(ValueError):
        json_reply.parse_json_reply("```json\n{broken\n```")
