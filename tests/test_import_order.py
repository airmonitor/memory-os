import ast
import pathlib
import pytest

SCRIPTS = sorted(pathlib.Path("scripts").glob("*.py"))


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda p: p.name)
def test_memos_config_is_imported_before_anything_vendored(path):
    """vendor/ reaches sys.path through memos_config's import side effect.

    Anything imported above it raises ModuleNotFoundError in the deployed pod —
    which is what cost memoryos-reflection-trigger its first 32 runs.
    """
    VENDORED = {"psycopg", "qdrant_client", "arq", "redis"}
    tree = ast.parse(path.read_text())
    memos_line = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "memos_config":
            memos_line = node.lineno if memos_line is None else min(memos_line, node.lineno)
    if memos_line is None:
        pytest.skip(f"{path.name} does not use memos_config")
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name.split(".")[0] for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            names = [node.module.split(".")[0]]
        for name in names:
            if name in VENDORED:
                assert node.lineno > memos_line, (
                    f"{path.name}:{node.lineno} imports {name} above memos_config "
                    f"(line {memos_line})")


def test_the_lineage_hash_width_agrees_across_writers():
    hooks = pathlib.Path("icarus/hooks.py").read_text()
    enhancer = pathlib.Path("scripts/context_enhancer.py").read_text()
    assert "hexdigest()[:16]" not in hooks.split("register_lineage")[-1]
    assert enhancer.count("hexdigest()[:32]") >= 1
