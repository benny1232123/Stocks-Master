#!/usr/bin/env python3
"""在指定信号日跑单个策略。

背景：theme / cctv / relativity / momentum 四个策略模块里的「今天」是
`datetime.now()` / `date.today()` 硬编码的，本地想 backfill 历史某天时
输出的 CSV 会落到「今天」目录下，没法回填。boll 已经原生支持 `today` 参数，
所以不动它。

本脚本：
  1) 读 `SIGNAL_DATE`（YYYYMMDD）环境变量；未设则报「需指定信号日」直接退出。
  2) 用 freezegun 之外最稳的方式：构造继承自 `datetime` 的 `FrozenDateTime` 子类
     和继承自 `date` 的 `FrozenDate` 子类，**只覆盖 `.now()` / `.today()`**。
  3) 策略模块的写法有三种：
       a) `import datetime` 然后用 `datetime.datetime.now()` / `datetime.date.today()`
          → 替换 `datetime` 模块的 `.datetime` / `.date` 类属性即可
       b) `from datetime import datetime, timedelta`
          → 直接 setattr 到该模块，把 `datetime` 局部引用换成 FrozenDateTime
       c) `from datetime import datetime` 然后用 `datetime.now()` / `datetime.now().strftime(...)`
          → 同 (b)
     本脚本同时做 (a) 和 (b) 两种 patch，覆盖所有现存策略。

用法：
  SIGNAL_DATE=20260714 python scripts/run_strategy_for_date.py theme
  SIGNAL_DATE=20260714 python scripts/run_strategy_for_date.py cctv
  SIGNAL_DATE=20260714 python scripts/run_strategy_for_date.py relativity
  SIGNAL_DATE=20260714 python scripts/run_strategy_for_date.py momentum
"""
from __future__ import annotations

import datetime as _dt
import importlib
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


SUPPORTED = ("theme", "cctv", "relativity", "momentum")


def _make_frozen(yyyymmdd: str):
    """构造两个 frozen 子类，类方法 .now() / .today() 返回指定日期。"""
    y = int(yyyymmdd[0:4])
    m = int(yyyymmdd[4:6])
    d = int(yyyymmdd[6:8])
    fixed_dt = _dt.datetime(y, m, d, 9, 30, 0)  # 早盘开始
    fixed_d = _dt.date(y, m, d)

    class _FrozenDate(_dt.date):
        @classmethod
        def today(cls):
            return fixed_d

    class _FrozenDateTime(_dt.datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_dt
            return fixed_dt.replace(tzinfo=tz)

    return _FrozenDate, _FrozenDateTime, fixed_d, fixed_dt


def _patch_module(mod, frozen_dt_cls, frozen_date_cls) -> None:
    """把模块里 from datetime import ... 拿到的引用全部换成 frozen 版本。"""
    # 1) 覆盖 `from datetime import datetime, timedelta` 的局部引用
    if getattr(mod, "datetime", None) is _dt.datetime:
        setattr(mod, "datetime", frozen_dt_cls)
    if getattr(mod, "date", None) is _dt.date:
        setattr(mod, "date", frozen_date_cls)
    if getattr(mod, "timedelta", None) is _dt.timedelta:
        # 保留原 timedelta，不动
        pass
    # 2) 覆盖 `import datetime` 模块的 .datetime / .date 类属性
    #    注意：mod.datetime 可能是 (1) 里的局部类引用（被 frozen 替了），
    #         也可能是真正的 datetime 模块 import 进来的。
    mod_datetime = getattr(mod, "datetime", None)
    if mod_datetime is _dt or mod_datetime is _dt.datetime:
        # mod 里 `import datetime` 但还没被 (1) 碰过
        setattr(mod, "datetime", _dt)  # 先确保是真正的模块
    # 强行覆盖 _dt 的两个类属性，影响所有用 datetime.datetime.X / datetime.date.X 的代码
    _dt.datetime = frozen_dt_cls  # type: ignore[misc]
    _dt.date = frozen_date_cls     # type: ignore[misc]


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in SUPPORTED:
        print(f"用法: SIGNAL_DATE=YYYYMMDD python {Path(__file__).name} "
              f"{{{','.join(SUPPORTED)}}}", file=sys.stderr)
        return 2

    sig = os.environ.get("SIGNAL_DATE", "").strip()
    if not sig or len(sig) != 8 or not sig.isdigit():
        print("请先设置环境变量 SIGNAL_DATE=YYYYMMDD", file=sys.stderr)
        return 2

    strat = sys.argv[1]
    FrozenDate, FrozenDateTime, fixed_d, fixed_dt = _make_frozen(sig)

    # 1) 先 import 策略模块（执行其顶层 import 链，固化 from datetime import X 引用）
    mod = importlib.import_module(f"smcore.strategies.{strat}")
    # 2) 修模块 + 全局
    _patch_module(mod, FrozenDateTime, FrozenDate)

    print(f"[run_strategy_for_date] {strat} @ 信号日 {sig}（已 patch datetime/date）")
    print(f"  today = {fixed_d}, now = {fixed_dt}")

    run_fn = getattr(mod, f"run_{strat}", None)
    if run_fn is None:
        print(f"smcore.strategies.{strat} 缺少 run_{strat}()", file=sys.stderr)
        return 3

    # 策略模块内部的 argparse 会直接用 sys.argv；把策略名从 sys.argv 里清掉，
    # 否则 run_cctv()/run_momentum() 等会把 'cctv'/'momentum' 当成未识别参数。
    original_argv = sys.argv
    sys.argv = [sys.argv[0]]
    try:
        rc = run_fn()
    finally:
        sys.argv = original_argv
    return int(rc) if rc is not None else 0


if __name__ == "__main__":
    sys.exit(main())
