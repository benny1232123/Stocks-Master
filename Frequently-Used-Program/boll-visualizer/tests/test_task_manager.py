from pathlib import Path
import importlib
import sys
import time

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

manager = importlib.import_module("core.task_manager")


def test_submit_task_and_load_result(monkeypatch, tmp_path) -> None:
    task_dir = tmp_path / "tasks"
    task_results_dir = task_dir / "results"
    task_history_file = task_dir / "history.jsonl"

    monkeypatch.setattr(manager, "TASK_DIR", task_dir)
    monkeypatch.setattr(manager, "TASK_RESULTS_DIR", task_results_dir)
    monkeypatch.setattr(manager, "TASK_HISTORY_FILE", task_history_file)

    with manager._task_lock:
        manager._active_tasks.clear()

    def _worker(progress_callback):
        progress_callback("init", 1, 2, "开始")
        progress_callback("done", 2, 2, "完成")
        return pd.DataFrame([{"股票代码": "600000", "命中策略": True}]), {"输入代码数": 1, "Boll命中": 1}

    task_id = manager.submit_task(
        title="测试任务",
        mode="full",
        scope="market",
        params={"window": 20},
        worker=_worker,
    )

    end_time = time.time() + 8
    task = None
    while time.time() < end_time:
        task = manager.get_task(task_id)
        if task and str(task.get("status", "")) in {"success", "failed"}:
            break
        time.sleep(0.05)

    assert task is not None
    assert str(task.get("status", "")) == "success"
    events = task.get("progress_events", [])
    assert isinstance(events, list)
    assert len(events) >= 3
    assert any("开始" in str(item.get("message", "")) for item in events)
    assert any("完成" in str(item.get("message", "")) for item in events)

    loaded_df = manager.load_task_result_dataframe(task_id)
    assert not loaded_df.empty
    assert str(loaded_df.iloc[0]["股票代码"]) == "600000"

    task_list = manager.list_tasks(limit=10)
    assert any(str(item.get("task_id", "")) == task_id for item in task_list)
