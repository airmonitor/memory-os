"""YAML config loader with ${VAR} interpolation and deep-merge override.

Loads `config/services.yaml` from the repo root (or `$CONFIG_PATH`), interpolates
`${ENV_VAR}` and `${ENV_VAR:default}` placeholders from `os.environ`, and deep-
merges `config/services.local.yaml` on top if present.

Exposes a module-level `config` singleton (loaded on import) plus `load_config()`
for explicit path use and `reload_config()` for tests.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

try:
    from dotenv import load_dotenv as _load_dotenv
except ImportError:  # python-dotenv optional; fall back to manual .env parsing
    _load_dotenv = None

_PLACEHOLDER = re.compile(r"\$\{([^}]+)\}")
_config: SimpleNamespace | None = None
_env_loaded = False


def _load_env_once(repo_root: Path) -> None:
    """Load .env from repo root into os.environ if not already loaded.

    Idempotent across reload_config() calls. Existing env vars take precedence
    (load_dotenv default: override=False).
    """
    global _env_loaded
    if _env_loaded:
        return
    env_path = repo_root / ".env"
    if env_path.exists():
        if _load_dotenv is not None:
            _load_dotenv(env_path, override=False)
        else:
            # Minimal fallback parser (no quoting tricks, no export keyword)
            for raw in env_path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k, v)
    _env_loaded = True


def _resolve_default_path() -> Path:
    """Find services.yaml. Order: $CONFIG_PATH > repo-relative > cwd-relative."""
    env_path = os.environ.get("CONFIG_PATH")
    if env_path:
        return Path(env_path)
    # Walk up from this file looking for config/services.yaml
    here = Path(__file__).resolve().parent
    for candidate in (here.parent, *here.parents):
        p = candidate / "config" / "services.yaml"
        if p.exists():
            return p
    # Fallback to cwd-relative
    return Path("config/services.yaml")


def _coerce_scalar(value: str) -> Any:
    """Promote interpolated strings to int/float/bool when they round-trip cleanly.

    Avoids requiring every consumer to coerce ports, dimensions, timeouts etc.
    after env interpolation strips YAML's native typing.
    """
    if value == "":
        return ""
    lowered = value.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if lowered in ("null", "none"):
        return None
    # int
    try:
        if value.lstrip("-").isdigit():
            return int(value)
    except (ValueError, AttributeError):
        pass
    # float
    try:
        if any(c in value for c in (".", "e", "E")):
            return float(value)
    except (ValueError, AttributeError):
        pass
    return value


def _interpolate(node: Any) -> Any:
    """Recursively replace ${VAR} and ${VAR:default} from os.environ."""
    if isinstance(node, dict):
        return {k: _interpolate(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_interpolate(v) for v in node]
    if isinstance(node, str) and "${" in node:
        def repl(m: re.Match[str]) -> str:
            spec = m.group(1)
            if ":" in spec:
                name, default = spec.split(":", 1)
            else:
                name, default = spec, ""
            return os.environ.get(name.strip(), default)
        replaced = _PLACEHOLDER.sub(repl, node)
        # If the original was exactly one placeholder, coerce the result.
        # Otherwise (composite string) keep as str.
        if _PLACEHOLDER.fullmatch(node):
            return _coerce_scalar(replaced)
        return replaced
    return node


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge `override` into `base`. Dict values merge, others replace."""
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _to_namespace(d: Any) -> Any:
    """Recursively convert dicts to SimpleNamespace for attribute access."""
    if isinstance(d, dict):
        return SimpleNamespace(**{k: _to_namespace(v) for k, v in d.items()})
    if isinstance(d, list):
        return [_to_namespace(x) for x in d]
    return d


def load_config(path: str | Path | None = None) -> SimpleNamespace:
    """Load (or return cached) config. Pass `path` to override default lookup."""
    global _config
    if _config is not None and path is None:
        return _config

    base_path = Path(path) if path else _resolve_default_path()
    if not base_path.exists():
        raise FileNotFoundError(
            f"services.yaml not found at {base_path}. "
            "Set $CONFIG_PATH or place config/services.yaml in repo root."
        )

    # Auto-load .env from repo root (parent of config/) before interpolation
    _load_env_once(base_path.parent.parent)

    data = yaml.safe_load(base_path.read_text(encoding="utf-8")) or {}

    local = base_path.parent / "services.local.yaml"
    if local.exists():
        local_data = yaml.safe_load(local.read_text(encoding="utf-8")) or {}
        data = _deep_merge(data, local_data)

    data = _interpolate(data)
    ns = _to_namespace(data)
    _config = ns
    return ns


def reload_config(path: str | Path | None = None) -> SimpleNamespace:
    """Bust the cache and reload. Useful for tests."""
    global _config, _env_loaded
    _config = None
    _env_loaded = False
    return load_config(path)


config = load_config()
