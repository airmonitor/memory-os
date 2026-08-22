import ast
import pathlib
import pytest

# Resolved from THIS FILE, never from the working directory. `Path("scripts")`
# is relative to wherever pytest was invoked, so running the suite from
# anywhere but the repo root collected ZERO cases here — the parametrisation
# emptied out, and a permanent regression gate for the incident that cost
# memoryos-reflection-trigger 32 runs vanished with no failure and no skip.
# An empty parametrize list is silent by construction; that is the whole
# reason this is anchored.
_REPO = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS = sorted((_REPO / "scripts").glob("*.py"))
VENDORED = {"psycopg", "qdrant_client", "arq", "redis"}

# Anything imported inside one of these never runs at module-import time, so
# it cannot reproduce the sys.path bug: a function/async-function/class body
# only executes when called/instantiated, and a `try:` block (the common
# optional-dependency `try/except ImportError` guard) is deliberately allowed
# here too. The rule this test encodes is precise: nothing vendored may be
# imported AT MODULE LEVEL above memos_config — not "nowhere above it in
# source order".
DEFERRED_CONTEXTS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Try)


def _parent_map(tree):
    parents = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _is_deferred(node, parents):
    current = parents.get(node)
    while current is not None:
        if isinstance(current, DEFERRED_CONTEXTS):
            return True
        current = parents.get(current)
    return False


def _vendored_above_memos_config(tree, filename):
    """Module-level vendored imports that sit above memos_config.

    Returns None if the file never imports memos_config (caller should skip),
    else a list of failure strings (empty means the file is clean).
    """
    parents = _parent_map(tree)
    memos_line = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "memos_config":
            memos_line = node.lineno if memos_line is None else min(memos_line, node.lineno)
    if memos_line is None:
        return None

    failures = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        if _is_deferred(node, parents):
            continue
        names = []
        if isinstance(node, ast.Import):
            names = [a.name.split(".")[0] for a in node.names]
        elif node.module:
            names = [node.module.split(".")[0]]
        for name in names:
            if name in VENDORED and node.lineno <= memos_line:
                failures.append(
                    f"{filename}:{node.lineno} imports {name} above memos_config "
                    f"(line {memos_line})")
    return failures


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda p: p.name)
def test_memos_config_is_imported_before_anything_vendored(path):
    """vendor/ reaches sys.path through memos_config's import side effect.

    Anything imported at MODULE LEVEL above it raises ModuleNotFoundError in
    the deployed pod — which is what cost memoryos-reflection-trigger its
    first 32 runs. A deferred import (inside a function, a class body, or a
    try/except guard) never runs at import time, so it cannot reproduce that
    failure and is allowed regardless of its source position.
    """
    tree = ast.parse(path.read_text())
    failures = _vendored_above_memos_config(tree, path.name)
    if failures is None:
        pytest.skip(f"{path.name} does not use memos_config")
    assert failures == []


def test_the_rule_collected_something_to_check():
    """The gate above is a parametrisation, and an EMPTY parametrisation
    passes in silence — no failure, no skip, no case ids. That is exactly how
    it disappeared: `SCRIPTS` globbed a path relative to the working
    directory, so `pytest` run from anywhere but the repo root checked nothing
    at all while reporting green. Anchoring `SCRIPTS` to `__file__` is the
    fix; this test is what makes a future un-anchoring (or a moved/renamed
    `scripts/`) fail loudly instead of evaporating.
    """
    assert SCRIPTS, "no scripts/*.py collected — the import-order gate is not running"
    names = {p.name for p in SCRIPTS}
    assert {"session_sweeper.py", "session_store.py", "db.py"} <= names


def test_a_deferred_vendored_import_does_not_fail_the_rule(tmp_path):
    """Positive case for the module-level scoping above, on a throwaway
    fixture rather than whichever real script happens to have this shape
    today — depending on someone else's file layout is the same fragility
    one level up.

    `import arq` here sits above `from memos_config import config` by source
    position, but it is inside a function body and therefore never executes
    at module-import time, so the rule must not fail it.
    """
    fixture = tmp_path / "fixture_deferred_import.py"
    fixture.write_text(
        "def lazy_worker():\n"
        "    from arq import create_pool\n"
        "    return create_pool\n"
        "\n"
        "from memos_config import config\n"
    )
    tree = ast.parse(fixture.read_text())
    failures = _vendored_above_memos_config(tree, fixture.name)
    assert failures == []


def test_the_lineage_hash_width_agrees_across_writers():
    """AST-based lookup, not string-split: `hooks.split("register_lineage")[-1]`
    would silently mis-scope if a later occurrence of that name (e.g. a new
    docstring mention) landed below the call this test cares about — the
    same truncating-lookup shape this repo already has a lesson about (an
    assertion that got its expectation through the same truncating lookup as
    the code under test, and passed while the files it compared differed).
    """
    hooks_tree = ast.parse((_REPO / "icarus" / "hooks.py").read_text())
    enhancer = (_REPO / "scripts" / "context_enhancer.py").read_text()

    hash_kw = None
    for node in ast.walk(hooks_tree):
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "register_lineage"):
            for kw in node.keywords:
                if kw.arg == "generation_context_hash":
                    hash_kw = kw.value
    assert hash_kw is not None, "no register_lineage(...) call found in icarus/hooks.py"
    assert isinstance(hash_kw, ast.Subscript), (
        "expected icarus/hooks.py's generation_context_hash= to be a "
        "hexdigest()[:N] slice")
    sl = hash_kw.slice
    width = sl.upper.value if isinstance(sl, ast.Slice) and isinstance(sl.upper, ast.Constant) else None
    assert width == 32, (
        f"icarus/hooks.py's register_lineage call uses a {width}-hex hash; "
        "expected 32 to match scripts/context_enhancer.py's writer")

    assert enhancer.count("hexdigest()[:32]") >= 1
