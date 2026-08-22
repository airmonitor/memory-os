import inspect
import pytest


def test_register_lineage_is_keyword_only():
    from scripts.context_enhancer import register_lineage
    kinds = {p.kind for p in inspect.signature(register_lineage).parameters.values()}
    assert kinds == {inspect.Parameter.KEYWORD_ONLY}


def test_on_session_end_does_not_extract(monkeypatch):
    from icarus import hooks, state
    state.exchanges.clear()          # module-level list: leaks between tests
    called = []
    monkeypatch.setattr(hooks, "extract_entries", lambda *a, **k: called.append(1) or [])
    monkeypatch.setattr(state, "write_entry", lambda *a, **k: called.append(1))
    state.exchanges.extend([{"user": "u" * 60, "assistant": "decided. Result: works. " + "d" * 300}] * 6)
    hooks.on_session_end(session_id="s", platform="slack")
    assert called == []


def test_search_qdrant_registers_lineage(monkeypatch):
    from icarus import hooks
    seen = {}
    monkeypatch.setitem(__import__("sys").modules, "scripts.context_enhancer",
                        _fake_enhancer(seen))
    hooks._search_qdrant("what did we decide about X", top_k=2)
    # state.session_id is set by on_session_start; in a bare test process it is
    # "" and the hook substitutes "unknown". Either is acceptable, empty is not.
    assert seen["session_id"]
    assert seen["retrieved_chunk_ids"] == ["c1"]


def _fake_enhancer(seen):
    import types
    mod = types.ModuleType("scripts.context_enhancer")
    mod.embed_query = lambda q: [0.0]
    mod.embed_query_sparse = lambda q: ([0], [0.0])
    mod.search_with_fallback = lambda **kw: ([{"id": "c1", "score": 0.9}], "dense", 1.0, 0.0)

    def register_lineage(**kwargs):
        seen.update(kwargs)
        return "lineage-1"
    mod.register_lineage = register_lineage
    return mod
