import os
import pytest


@pytest.fixture
def fabric(tmp_path, monkeypatch):
    """icarus.state reads FABRIC_DIR at import time, so the module has to be
    reloaded under the patched environment — and reloaded BACK afterwards, or
    every later test in the process inherits this tmp_path and
    `state.exchanges` points at a different module object than the one under
    test."""
    import importlib
    from icarus import state
    monkeypatch.setenv("FABRIC_DIR", str(tmp_path / "fabric"))
    importlib.reload(state)
    yield state
    monkeypatch.undo()
    importlib.reload(state)


def test_same_suffix_overwrites_instead_of_multiplying(fabric):
    a = fabric.write_entry("decision", "body", "a summary", suffix="deadbeef")
    b = fabric.write_entry("decision", "body v2", "a summary", suffix="deadbeef")
    assert a == b
    assert len(list((fabric.FABRIC_DIR).glob("*.md"))) == 1
    assert "body v2" in open(a).read()


def test_no_suffix_still_produces_unique_names(fabric):
    a = fabric.write_entry("note", "b", "s")
    b = fabric.write_entry("note", "b", "s")
    assert a != b


def test_no_partial_file_is_left_behind(fabric):
    fabric.write_entry("note", "b", "s", suffix="cafe")
    assert not list(fabric.FABRIC_DIR.glob("*.tmp"))
