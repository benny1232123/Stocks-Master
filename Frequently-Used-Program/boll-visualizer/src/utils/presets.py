from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
from typing import Any

from utils.config import STOCK_DATA_DIR

PRESET_DIR = STOCK_DATA_DIR / "presets"
PRESET_FILE = PRESET_DIR / "ui_parameter_presets.json"

DATE_KEYS = {"start_date", "end_date"}
ALLOWED_KEYS = {
    "analysis_mode",
    "codes_text",
    "start_date",
    "end_date",
    "window",
    "k",
    "near_ratio",
    "adjust",
    "force_refresh",
    "price_upper_limit",
    "debt_asset_ratio_limit",
    "exclude_gem_sci",
    "max_workers",
    "max_retries",
    "retry_backoff_seconds",
    "request_interval_seconds",
    "boll_max_workers",
    "market_fast_mode",
    "market_fast_days",
}


def _ensure_parent_dir() -> None:
    PRESET_DIR.mkdir(parents=True, exist_ok=True)


def _read_raw_payload() -> dict[str, dict[str, Any]]:
    if not PRESET_FILE.exists():
        return {}

    try:
        payload = json.loads(PRESET_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

    if not isinstance(payload, dict):
        return {}

    cleaned: dict[str, dict[str, Any]] = {}
    for name, values in payload.items():
        if not isinstance(name, str) or not isinstance(values, dict):
            continue
        normalized = {k: values.get(k) for k in ALLOWED_KEYS if k in values}
        cleaned[name.strip()] = normalized
    return {k: v for k, v in cleaned.items() if k}


def _write_raw_payload(payload: dict[str, dict[str, Any]]) -> None:
    _ensure_parent_dir()
    PRESET_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _json_safe_value(key: str, value: Any) -> Any:
    if key in DATE_KEYS and isinstance(value, (date, datetime)):
        return value.strftime("%Y-%m-%d")

    if isinstance(value, (str, bool, int, float)) or value is None:
        return value

    if isinstance(value, (date, datetime)):
        return value.strftime("%Y-%m-%d")

    return str(value)


def normalize_preset_values(values: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in ALLOWED_KEYS:
        if key not in values:
            continue
        out[key] = _json_safe_value(key, values.get(key))
    return out


def load_parameter_presets() -> dict[str, dict[str, Any]]:
    return _read_raw_payload()


def upsert_parameter_preset(name: str, values: dict[str, Any]) -> str:
    trimmed_name = str(name).strip()
    if not trimmed_name:
        raise ValueError("预设名称不能为空")

    payload = _read_raw_payload()
    payload[trimmed_name] = normalize_preset_values(values)
    _write_raw_payload(payload)
    return trimmed_name


def delete_parameter_preset(name: str) -> bool:
    trimmed_name = str(name).strip()
    if not trimmed_name:
        return False

    payload = _read_raw_payload()
    if trimmed_name not in payload:
        return False

    payload.pop(trimmed_name, None)
    _write_raw_payload(payload)
    return True


def parse_date_value(raw_value: Any, fallback: date) -> date:
    if isinstance(raw_value, date):
        return raw_value
    if raw_value is None:
        return fallback
    try:
        return datetime.strptime(str(raw_value), "%Y-%m-%d").date()
    except Exception:
        return fallback
