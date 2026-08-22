import os
import pathlib

import yaml

# Resolved from THIS FILE, never from the working directory -- see
# tests/test_import_order.py's identical comment for the incident this class
# of bug caused. Running the suite from anywhere but the repo root made this
# test raise FileNotFoundError instead of skipping or passing, which is at
# least loud, but the anchor is one line and removes the CWD dependency.
_REPO = pathlib.Path(__file__).resolve().parent.parent


def test_every_sweeper_knob_is_a_config_key_with_a_default():
    raw = (_REPO / "config" / "services.yaml").read_text()
    doc = yaml.safe_load(raw)
    block = doc["session_extraction"]
    for key in ("idle_minutes", "min_messages", "context_overlap", "max_lag_hours",
                "max_per_run", "quality_threshold"):
        assert key in block, key
        assert str(block[key]).startswith("${"), f"{key} must be ${{VAR:default}}"
    assert "${SESSION_IDLE_MINUTES:90}" in raw
    assert "${EXTRACTION_TIMEOUT:100}" in raw
