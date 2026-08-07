"""CLI: 对指定信号日的 Daily-Action-List 做 Brinson 绩效归因。

用法:
  python scripts/run_attribution.py 20260719
  python scripts/run_attribution.py 20260719 --horizon 10 --benchmark market
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.attribution import format_attribution, run_attribution


def main() -> int:
    ap = argparse.ArgumentParser(description="Brinson 绩效归因（组合优化层/选股贡献分解）")
    ap.add_argument("signal_date", help="信号日 YYYYMMDD，对应 stock_data/Daily-Action-List-{date}.csv")
    ap.add_argument("--horizon", type=int, default=10, help="前向收益窗口(交易日)，默认 10")
    ap.add_argument("--benchmark", choices=["equal", "market"], default="equal",
                    help="equal=等权同名单(看优化层贡献)；market=沪深300(看选股贡献)")
    args = ap.parse_args()

    res = run_attribution(args.signal_date, horizon=args.horizon, benchmark=args.benchmark)
    if res is None:
        print(f"无可用数据做归因（缺 Daily-Action-List-{args.signal_date}.csv 或收益缺失）", file=sys.stderr)
        return 2
    print(format_attribution(res, args.signal_date, args.horizon, args.benchmark))
    return 0


if __name__ == "__main__":
    sys.exit(main())
