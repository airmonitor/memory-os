"""Debug entry point — `python -m memos_config` prints resolved config."""

import json
from types import SimpleNamespace

from .loader import config


def _to_dict(node):
    if isinstance(node, SimpleNamespace):
        return {k: _to_dict(v) for k, v in vars(node).items()}
    if isinstance(node, list):
        return [_to_dict(x) for x in node]
    return node


if __name__ == "__main__":
    print(json.dumps(_to_dict(config), indent=2, sort_keys=True, default=str))
