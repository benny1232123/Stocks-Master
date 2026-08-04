from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import threading
from typing import Any, Callable
import uuid

import pandas as pd

from core.boll_strategy import analyze_stocks
from core.data_fetcher import fetch_all_a_share_codes
from core.full_flow_strategy import analyze_stocks_full_flow
from utils.config import CSV_ENCODING, DEFAULT_ADJUST, STOCK_DATA_DIR

ProgressCallback = Callable[[str, int, int, str], None]
TaskWorker = Callable[[ProgressCallback], tuple[pd.DataFrame, dict[str, int | float]]]

TASK_DIR = STOCK_DATA_DIR / "tasks"
TASK_RESULTS_DIR = TASK_DIR / "results"
TASK_HISTORY_FILE = TASK_DIR / "history.jsonl"
MAX_PROGRESS_EVENTS = 5000
MAX_HISTORY_EVENTS = 800

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="boll-task")
_task_lock = threading.Lock()
_active_tasks: dict[str, dict[str, Any]] = {}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_task_dirs() -> None:
    TASK_RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, bool, int, float)) or value is None:
        return value
    if isinstance(value, (date, datetime)):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def _persist_history(task: dict[str, Any]) -> None:
    _ensure_task_dirs()
    record = task.copy()
    events = record.get("progress_events")
    if isinstance(events, list) and len(events) > MAX_HISTORY_EVENTS:
        record["progress_events"] = events[-MAX_HISTORY_EVENTS:]
    record = _json_safe(record)
    with TASK_HISTORY_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")


def _read_history() -> list[dict[str, Any]]:
    if not TASK_HISTORY_FILE.exists():
        return []

    records: list[dict[str, Any]] = []
    with TASK_HISTORY_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except Exception:
                continue
            if isinstance(item, dict):
                records.append(item)
    return records


def _upsert_active_task(task_id: str, **kwargs: Any) -> None:
    with _task_lock:
        if task_id not in _active_tasks:
            return
        _active_tasks[task_id].update(kwargs)


def _record_progress(task_id: str, stage: str, done: int, total: int, message: str) -> None:
    safe_total = total if total > 0 else 1
    percent = max(0.0, min(1.0, done / safe_total))
    event = {
        "time": _now_text(),
        "stage": str(stage),
        "done": int(done),
        "total": int(total),
        "message": str(message),
    }

    with _task_lock:
        task = _active_tasks.get(task_id)
        if task is None:
            return

        events = task.get("progress_events")
        if not isinstance(events, list):
            events = []

        if not events or any(events[-1].get(key) != event.get(key) for key in ["stage", "done", "total", "message"]):
            events.append(event)
            if len(events) > MAX_PROGRESS_EVENTS:
                events = events[-MAX_PROGRESS_EVENTS:]

        task.update(
            {
                "progress_stage": str(stage),
                "progress_done": int(done),
                "progress_total": int(total),
                "progress_message": str(message),
                "progress_percent": round(percent, 4),
                "updated_at": _now_text(),
                "progress_events": events,
            }
        )


def _run_task(task_id: str, worker: TaskWorker) -> None:
    _upsert_active_task(task_id, status="running", started_at=_now_text())
    _record_progress(task_id, "init", 0, 1, "任务启动，准备执行")

    def progress_callback(stage: str, done: int, total: int, message: str) -> None:
        _record_progress(task_id, stage, done, total, message)

    try:
        result_df, flow_stats = worker(progress_callback)
        _ensure_task_dirs()
        result_path = TASK_RESULTS_DIR / f"{task_id}.csv"
        result_df.to_csv(result_path, index=False, encoding=CSV_ENCODING)
        _record_progress(task_id, "done", 1, 1, f"任务完成，结果已保存：{result_path.name}")

        with _task_lock:
            task = _active_tasks.get(task_id, {}).copy()
            task.update(
                {
                    "status": "success",
                    "ended_at": _now_text(),
                    "result_csv": str(result_path),
                    "flow_stats": _json_safe(flow_stats),
                    "error": "",
                    "progress_percent": 1.0,
                }
            )
            _active_tasks[task_id] = task
        _persist_history(task)
    except Exception as error:
        _record_progress(task_id, "failed", 1, 1, f"任务失败：{error}")
        with _task_lock:
            task = _active_tasks.get(task_id, {}).copy()
            task.update(
                {
                    "status": "failed",
                    "ended_at": _now_text(),
                    "error": str(error),
                }
            )
            _active_tasks[task_id] = task
        _persist_history(task)


def submit_task(
    *,
    title: str,
    mode: str,
    scope: str,
    params: dict[str, Any],
    worker: TaskWorker,
) -> str:
    task_id = uuid.uuid4().hex[:12]
    task = {
        "task_id": task_id,
        "title": str(title),
        "mode": str(mode),
        "scope": str(scope),
        "status": "pending",
        "created_at": _now_text(),
        "updated_at": _now_text(),
        "started_at": "",
        "ended_at": "",
        "progress_stage": "init",
        "progress_done": 0,
        "progress_total": 0,
        "progress_message": "任务已提交，等待执行",
        "progress_percent": 0.0,
        "progress_events": [
            {
                "time": _now_text(),
                "stage": "init",
                "done": 0,
                "total": 0,
                "message": "任务已提交，等待执行",
            }
        ],
        "params": _json_safe(params),
        "result_csv": "",
        "flow_stats": {},
        "error": "",
    }

    with _task_lock:
        _active_tasks[task_id] = task

    _executor.submit(_run_task, task_id, worker)
    return task_id


def _sort_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(tasks, key=lambda item: str(item.get("created_at", "")), reverse=True)


def list_tasks(limit: int = 30) -> list[dict[str, Any]]:
    with _task_lock:
        active_items = [task.copy() for task in _active_tasks.values()]

    history_items = _read_history()

    combined: dict[str, dict[str, Any]] = {}
    for item in history_items:
        task_id = str(item.get("task_id", "")).strip()
        if task_id:
            combined[task_id] = item

    for item in active_items:
        task_id = str(item.get("task_id", "")).strip()
        if task_id:
            combined[task_id] = item

    items = _sort_tasks(list(combined.values()))
    return items[: max(1, int(limit))]


def get_task(task_id: str) -> dict[str, Any] | None:
    target = str(task_id).strip()
    if not target:
        return None

    with _task_lock:
        if target in _active_tasks:
            return _active_tasks[target].copy()

    for item in _sort_tasks(_read_history()):
        if str(item.get("task_id", "")) == target:
            return item
    return None


def load_task_result_dataframe(task_id: str) -> pd.DataFrame:
    task = get_task(task_id)
    if not task:
        return pd.DataFrame()

    result_csv = str(task.get("result_csv", "")).strip()
    if not result_csv:
        return pd.DataFrame()

    path = Path(result_csv)
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def submit_market_analysis_task(
    *,
    mode: str,
    start_date: date,
    end_date: date,
    window: int,
    k: float,
    near_ratio: float,
    adjust: str = DEFAULT_ADJUST,
    force_refresh: bool = False,
    price_upper_limit: float = 35.0,
    debt_asset_ratio_limit: float = 70.0,
    exclude_gem_sci: bool = True,
    max_workers: int = 4,
    max_retries: int = 2,
    retry_backoff_seconds: float = 0.5,
    request_interval_seconds: float = 0.0,
    boll_max_workers: int = 8,
    market_fast_mode: bool = True,
    market_fast_days: int = 180,
) -> str:
    task_mode = str(mode).strip().lower()
    if task_mode not in {"full", "boll_only"}:
        raise ValueError("mode 仅支持 full 或 boll_only")

    params = {
        "mode": task_mode,
        "start_date": start_date,
        "end_date": end_date,
        "window": int(window),
        "k": float(k),
        "near_ratio": float(near_ratio),
        "adjust": str(adjust),
        "force_refresh": bool(force_refresh),
        "price_upper_limit": float(price_upper_limit),
        "debt_asset_ratio_limit": float(debt_asset_ratio_limit),
        "exclude_gem_sci": bool(exclude_gem_sci),
        "max_workers": int(max_workers),
        "max_retries": int(max_retries),
        "retry_backoff_seconds": float(retry_backoff_seconds),
        "request_interval_seconds": float(request_interval_seconds),
        "boll_max_workers": int(boll_max_workers),
        "market_fast_mode": bool(market_fast_mode),
        "market_fast_days": int(market_fast_days),
    }

    title = "全市场全流程分析" if task_mode == "full" else "全市场仅Boll分析"

    def worker(progress_callback: ProgressCallback) -> tuple[pd.DataFrame, dict[str, int | float]]:
        progress_callback("init", 0, 1, "正在获取全市场代码")
        codes = fetch_all_a_share_codes(force_refresh=bool(force_refresh))
        if not codes:
            raise RuntimeError("未获取到全市场代码，请检查网络后重试")

        progress_callback("init", 1, 1, f"已获取全市场代码：{len(codes)}")

        if task_mode == "full":
            effective_workers = int(max_workers)
            if bool(market_fast_mode):
                effective_workers = max(effective_workers, 8)

            result_df, _data_map, flow_stats = analyze_stocks_full_flow(
                codes=codes,
                start_date=start_date,
                end_date=end_date,
                window=int(window),
                k=float(k),
                near_ratio=float(near_ratio),
                adjust=str(adjust),
                price_upper_limit=float(price_upper_limit),
                debt_asset_ratio_limit=float(debt_asset_ratio_limit),
                exclude_gem_sci=bool(exclude_gem_sci),
                force_refresh=bool(force_refresh),
                max_workers=effective_workers,
                max_retries=int(max_retries),
                retry_backoff_seconds=float(retry_backoff_seconds),
                request_interval_seconds=float(request_interval_seconds),
                fast_mode=bool(market_fast_mode),
                progress_callback=progress_callback,
            )
            return result_df, flow_stats

        analysis_start_date = start_date
        if bool(market_fast_mode):
            fast_start_date = end_date - timedelta(days=max(1, int(market_fast_days) - 1))
            analysis_start_date = max(start_date, fast_start_date)

        result_df, _data_map = analyze_stocks(
            codes=codes,
            start_date=analysis_start_date,
            end_date=end_date,
            window=int(window),
            k=float(k),
            near_ratio=float(near_ratio),
            adjust=str(adjust),
            code_name_map={},
            force_refresh=bool(force_refresh),
            max_workers=int(max(1, boll_max_workers)),
            retain_all_charts=False,
            progress_callback=progress_callback,
        )
        flow_stats = {
            "输入代码数": len(codes),
            "Boll命中": int(result_df["命中策略"].sum())
            if (not result_df.empty and "命中策略" in result_df.columns)
            else 0,
        }
        return result_df, flow_stats

    return submit_task(
        title=title,
        mode=task_mode,
        scope="market",
        params=params,
        worker=worker,
    )
