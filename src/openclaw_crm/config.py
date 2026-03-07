from __future__ import annotations

import os
from pathlib import Path
from typing import Any


_config_cache: dict[str, Any] | None = None


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    global _config_cache
    if _config_cache is not None:
        return _config_cache

    if path is None:
        path = os.environ.get("OPENCLAW_CRM_CONFIG", "crm.yaml")

    p = Path(path)
    if not p.exists():
        sid = os.environ.get("CRM_SPREADSHEET_ID", "")
        if sid:
            _config_cache = {"google": {"crm_spreadsheet_id": sid}}
            return _config_cache
        return {}

    try:
        import yaml
        with open(p) as f:
            _config_cache = yaml.safe_load(f) or {}
    except ImportError:
        import json
        with open(p) as f:
            _config_cache = json.load(f)

    return _config_cache


def get_spreadsheet_id() -> str:
    sid = os.environ.get("CRM_SPREADSHEET_ID")
    if sid:
        return sid
    cfg = load_config()
    return cfg.get("google", {}).get("crm_spreadsheet_id", "")


def clear_config_cache() -> None:
    global _config_cache
    _config_cache = None
