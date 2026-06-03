"""Memory OS service configuration loader.

Single source of truth for hosts, ports, model names. Reads `config/services.yaml`
with `${VAR}` environment interpolation. Local overrides in
`config/services.local.yaml` (deep-merged on top).

Usage:
    from memos_config import config
    config.postgres.host          # -> "192.168.1.134"
    config.litellm.models.chat.name  # -> "lm-studio-qwen3.6"
"""

from .loader import config, load_config, reload_config

__all__ = ["config", "load_config", "reload_config"]
