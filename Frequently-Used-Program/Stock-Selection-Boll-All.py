from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import baostock as bs
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VISUALIZER_SRC = PROJECT_ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"
STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"

if str(VISUALIZER_SRC) not in sys.path:
    sys.path.insert(0, str(VISUALIZER_SRC))

from core.full_flow_strategy import analyze_stocks_full_flow


PRICE_UPPER_LIMIT = 300.0
DEBT_ASSET_RATIO_LIMIT = 70.0
WINDOW = 20
K = 1.645
NEAR_RATIO = 1.015
ADJUST = "qfq"
DAYS_BACK = 180
EXCLUDE_GEM_SCI = False


def _result_set_to_df(result_set) -> pd.DataFrame:
    rows: list[list[str]] = []
    while result_set.next():
        rows.append(result_set.get_row_data())
    return pd.DataFrame(rows, columns=result_set.fields)


def _fetch_all_a_share_codes(max_lookback_days: int = 10) -> list[str]:
    login_result = bs.login()
    if login_result.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {login_result.error_msg}")

    try:
        raw_df = pd.DataFrame()
        for offset in range(max_lookback_days + 1):
            day = (date.today() - timedelta(days=offset)).strftime("%Y-%m-%d")
            result_set = bs.query_all_stock(day=day)
            if result_set.error_code != "0":
                continue
            temp_df = _result_set_to_df(result_set)
            if not temp_df.empty:
                raw_df = temp_df
                break

        if raw_df.empty or "code" not in raw_df.columns:
            return []

        code_series = raw_df["code"].astype(str)
        code_series = code_series[code_series.str.match(r"^(sh|sz)\.\d{6}$", na=False)]
        code_series = code_series.str[-6:]

        sh_mask = code_series.str.match(r"^(600|601|603|605|688)\d{3}$", na=False)
        sz_mask = code_series.str.match(r"^(000|001|002|003|300|301)\d{3}$", na=False)
        code_series = code_series[sh_mask | sz_mask]

        return sorted(code_series.dropna().unique().tolist())
    finally:
        bs.logout()


def main() -> None:
    STOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)

    end_date = date.today()
    start_date = end_date - timedelta(days=DAYS_BACK)

    codes = _fetch_all_a_share_codes()
    if not codes:
        raise RuntimeError("未获取到A股代码，请检查网络或稍后重试")

    print(f"全市场代码数: {len(codes)}")
    print(f"分析区间: {start_date} ~ {end_date}")

    result_df, _data_map, flow_stats = analyze_stocks_full_flow(
        codes=codes,
        start_date=start_date,
        end_date=end_date,
        window=WINDOW,
        k=K,
        near_ratio=NEAR_RATIO,
        adjust=ADJUST,
        price_upper_limit=PRICE_UPPER_LIMIT,
        debt_asset_ratio_limit=DEBT_ASSET_RATIO_LIMIT,
        exclude_gem_sci=EXCLUDE_GEM_SCI,
    )

    today_text = end_date.strftime("%Y%m%d")
    full_path = STOCK_DATA_DIR / f"Stock-Selection-Boll-All-{today_text}.csv"
    hit_path = STOCK_DATA_DIR / f"Stock-Selection-Boll-All-Hits-{today_text}.csv"

    result_df.to_csv(full_path, index=False, encoding="utf-8-sig")

    hit_df = result_df[result_df["命中策略"] == True].copy() if not result_df.empty else pd.DataFrame()
    hit_df.to_csv(hit_path, index=False, encoding="utf-8-sig")

    print("\n流程统计:")
    print(
        " -> ".join(
            [
                f"输入{flow_stats.get('输入代码数', 0)}",
                f"板块后{flow_stats.get('板块过滤后', 0)}",
                f"资金流{flow_stats.get('资金流通过', 0)}",
                f"基本面{flow_stats.get('基本面通过', 0)}",
                f"前置汇合{flow_stats.get('前置汇合通过', 0)}",
                f"股东{flow_stats.get('股东通过', 0)}",
                f"Boll命中{flow_stats.get('Boll命中', 0)}",
            ]
        )
    )

    print(f"\n全量结果已保存: {full_path}")
    print(f"命中结果已保存: {hit_path}")


if __name__ == "__main__":
    main()
