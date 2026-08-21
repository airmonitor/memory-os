import os
import yaml


def test_every_sweeper_knob_is_a_config_key_with_a_default():
    raw = open("config/services.yaml").read()
    doc = yaml.safe_load(raw)
    block = doc["session_extraction"]
    for key in ("idle_minutes", "min_messages", "context_overlap", "max_lag_hours",
                "max_per_run", "quality_threshold"):
        assert key in block, key
        assert str(block[key]).startswith("${"), f"{key} must be ${{VAR:default}}"
    assert "${SESSION_IDLE_MINUTES:90}" in raw
    assert "${EXTRACTION_TIMEOUT:100}" in raw
