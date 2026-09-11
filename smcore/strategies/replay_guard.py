"""回放模式守卫 —— 让策略在历史重放时诚实标注"哪些输入不是 point-in-time"。

背景：EM 资金流排行 / 盈利预测 / 同花顺异动等接口只能返回"当前时刻"的数据，
历史重放（replay_history.py / run_strategy_for_date.py）调用它们等于把未来信息
注入历史信号。这些输入无法从数据源层面修复，因此退而求其次：

1. 回放驱动脚本统一设置环境变量 REPLAY_MODE=1 / REPLAY_SIGNAL_DATE=YYYYMMDD；
2. 策略在读到该标记时：
   - 跳过纯实时的加成因子（如异动催化）；
   - 在产出 CSV 旁写 `<同名>.meta.json` 侧标，声明 universe_pit=false 及原因；
3. 下游（walk_forward / 任何引用重放产物的人）可以据此拒绝采信重放指标。
"""
from __future__ import annotations

import json
import os
from pathlib import Path


def is_replay_mode() -> bool:
    """是否处于历史重放模式（由回放驱动脚本设置环境变量）。"""
    return os.environ.get("REPLAY_MODE", "").strip() == "1"


def replay_signal_date() -> str | None:
    """回放的信号日（YYYYMMDD），非回放返回 None。"""
    if not is_replay_mode():
        return None
    return os.environ.get("REPLAY_SIGNAL_DATE", "").strip() or None


def stamp_replay_meta(out_path: Path | str, *, universe_pit: bool, reasons: list[str]) -> Path | None:
    """在策略产出 CSV 旁写 `<同名>.meta.json`，声明回放模式下各输入的 PIT 状态。

    只在回放模式下写；非回放返回 None。写失败不影响主产物（print 告警）。
    """
    if not is_replay_mode():
        return None
    p = Path(out_path)
    meta_path = p.with_name(p.name + ".meta.json")
    meta = {
        "signal_date": replay_signal_date(),
        "replay_mode": True,
        "universe_pit": universe_pit,
        "reasons": reasons,
    }
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        print(f"[replay_guard] WARN: 回放侧标写入失败 {meta_path}: {exc}")
        return None
    print(
        f"[replay_guard] ⚠️ 回放模式侧标已写入 {meta_path.name} "
        f"(universe_pit={universe_pit})；重放产物不可作为策略有效性证据"
    )
    return meta_path
