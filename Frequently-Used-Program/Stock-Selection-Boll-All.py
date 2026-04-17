from __future__ import annotations
# pyright: reportMissingImports=false

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VISUALIZER_SRC = PROJECT_ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"
STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"

if not VISUALIZER_SRC.exists():
    raise FileNotFoundError(f"未找到可视化策略源码目录: {VISUALIZER_SRC}")

if str(VISUALIZER_SRC) not in sys.path:
    sys.path.insert(0, str(VISUALIZER_SRC))

from core.data_fetcher import fetch_all_a_share_codes
from core.full_flow_strategy import analyze_stocks_full_flow
from strategy_common import load_checkpoint_df, merge_result_rows, normalize_code_series, save_checkpoint_df


PRICE_UPPER_LIMIT = 30.0
DEBT_ASSET_RATIO_LIMIT = 70.0
WINDOW = 20
K = 1.645
NEAR_RATIO = 1.015
ADJUST = "qfq"
DAYS_BACK = 180
EXCLUDE_GEM_SCI = False
DEFAULT_CHUNK_SIZE = 400
DEFAULT_MAX_WORKERS = 4
DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 0.5
DEFAULT_REQUEST_INTERVAL_SECONDS = 0.0
DEFAULT_FAST_MODE = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="全市场 Selection Boll 批量分析")
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="忽略本地缓存并强制重新抓取数据，同时覆盖当日结果文件。",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="启用断点续跑，自动从当日检查点继续执行。",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="分块处理大小，默认 400。",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="全流程并发评估线程数，默认 4。",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="网络请求失败重试次数，默认 2。",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=DEFAULT_RETRY_BACKOFF_SECONDS,
        help="重试基准退避秒数，默认 0.5。",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=DEFAULT_REQUEST_INTERVAL_SECONDS,
        help="请求限流间隔秒数，默认 0（不限流）。",
    )
    parser.add_argument(
        "--slow-accurate",
        action="store_true",
        help="关闭极速模式，完整执行慢接口（耗时会显著增加）。",
    )
    return parser.parse_args()


def _on_progress(stage: str, done: int, total: int, message: str) -> None:
    safe_total = total if total > 0 else 1
    if stage == "evaluate" and done not in {safe_total} and done % 200 != 0:
        return
    print(f"[{stage}] {done}/{safe_total} {message}")


def _bool_col(result_df: pd.DataFrame, col_name: str) -> pd.Series:
    if col_name in result_df.columns:
        return result_df[col_name].astype(bool)
    return pd.Series([False] * len(result_df), index=result_df.index)


def _build_flow_stats_from_result(result_df: pd.DataFrame, input_count: int) -> dict[str, int | float]:
    if result_df.empty:
        return {
            "输入代码数": input_count,
            "板块过滤后": 0,
            "资金流通过": 0,
            "基本面通过": 0,
            "前置汇合通过": 0,
            "股东通过": 0,
            "Boll命中": 0,
            "3日资金命中": 0,
            "5日资金命中": 0,
            "10日资金命中": 0,
            "平均评分": 0.0,
            "A档数量": 0,
        }

    flow_text = result_df.get("资金流说明", pd.Series(["" for _ in range(len(result_df))])).astype(str)
    basic_pass = (
        _bool_col(result_df, "资产负债率通过")
        & _bool_col(result_df, "净利润通过")
        & _bool_col(result_df, "现金流通过")
        & _bool_col(result_df, "盈利预期通过")
    )

    stats: dict[str, int | float] = {
        "输入代码数": int(input_count),
        "板块过滤后": int(len(result_df)),
        "资金流通过": int(_bool_col(result_df, "资金流通过").sum()),
        "基本面通过": int(basic_pass.sum()),
        "前置汇合通过": int(_bool_col(result_df, "前置汇合通过").sum()),
        "股东通过": int(_bool_col(result_df, "重要股东通过").sum()),
        "Boll命中": int(_bool_col(result_df, "命中策略").sum()),
        "3日资金命中": int(flow_text.str.contains("3日排行", na=False).sum()),
        "5日资金命中": int(flow_text.str.contains("5日排行", na=False).sum()),
        "10日资金命中": int(flow_text.str.contains("10日排行", na=False).sum()),
        "平均评分": round(float(result_df.get("综合评分", pd.Series([0])).mean()), 2),
        "A档数量": int((result_df.get("评分等级", pd.Series([""])) == "A").sum()),
    }
    return stats


def main() -> None:
    args = parse_args()

    STOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)

    end_date = date.today()
    start_date = end_date - timedelta(days=DAYS_BACK)
    today_text = end_date.strftime("%Y%m%d")
    chunk_size = max(50, int(args.chunk_size))
    max_workers = max(1, int(args.max_workers))
    max_retries = max(0, int(args.max_retries))
    retry_backoff = max(0.0, float(args.retry_backoff))
    request_interval = max(0.0, float(args.request_interval))
    fast_mode = DEFAULT_FAST_MODE and (not bool(args.slow_accurate))

    full_path = STOCK_DATA_DIR / f"Stock-Selection-Boll-All-{today_text}.csv"
    hit_path = STOCK_DATA_DIR / f"Stock-Selection-Boll-All-Hits-{today_text}.csv"
    checkpoint_dir = STOCK_DATA_DIR / "checkpoints"
    checkpoint_path = checkpoint_dir / f"Stock-Selection-Boll-All-Checkpoint-{today_text}.csv"

    if args.force_refresh and checkpoint_path.exists():
        checkpoint_path.unlink(missing_ok=True)

    if full_path.exists() and hit_path.exists() and not args.force_refresh:
        print("检测到今日结果文件已存在，直接复用。")
        print("如需强制重跑，请使用 --force-refresh。")
        print(f"全量结果已保存: {full_path}")
        print(f"命中结果已保存: {hit_path}")
        return

    codes = fetch_all_a_share_codes(force_refresh=bool(args.force_refresh))
    if not codes:
        raise RuntimeError("未获取到A股代码，请检查网络或稍后重试")

    print(f"全市场代码数: {len(codes)}")
    print(f"分析区间: {start_date} ~ {end_date}")
    if args.force_refresh:
        print("已启用强制刷新：忽略本地缓存并重新抓取数据。")
    if args.resume:
        print(f"已启用断点续跑，分块大小: {chunk_size}")
    print(
        f"并发参数: workers={max_workers}, retries={max_retries}, "
        f"backoff={retry_backoff}, interval={request_interval}, fast_mode={fast_mode}"
    )

    checkpoint_df = load_checkpoint_df(checkpoint_path) if args.resume else pd.DataFrame()
    done_codes = set(normalize_code_series(checkpoint_df["股票代码"]).tolist()) if not checkpoint_df.empty else set()
    pending_codes = [code for code in codes if code not in done_codes]

    if args.resume and done_codes:
        print(f"检查点已完成: {len(done_codes)}，待处理: {len(pending_codes)}")

    result_df = checkpoint_df.copy() if args.resume else pd.DataFrame()
    total_chunks = (len(pending_codes) + chunk_size - 1) // chunk_size if pending_codes else 0

    for chunk_index in range(total_chunks):
        chunk_start = chunk_index * chunk_size
        chunk_codes = pending_codes[chunk_start : chunk_start + chunk_size]
        print(f"\n处理分块 {chunk_index + 1}/{total_chunks}，股票数: {len(chunk_codes)}")

        chunk_df, _data_map, _chunk_stats = analyze_stocks_full_flow(
            codes=chunk_codes,
            start_date=start_date,
            end_date=end_date,
            window=WINDOW,
            k=K,
            near_ratio=NEAR_RATIO,
            adjust=ADJUST,
            price_upper_limit=PRICE_UPPER_LIMIT,
            debt_asset_ratio_limit=DEBT_ASSET_RATIO_LIMIT,
            exclude_gem_sci=EXCLUDE_GEM_SCI,
            force_refresh=bool(args.force_refresh),
            max_workers=max_workers,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff,
            request_interval_seconds=request_interval,
            fast_mode=bool(fast_mode),
            progress_callback=_on_progress,
        )

        result_df = merge_result_rows(
            result_df,
            chunk_df,
            code_col="股票代码",
            sort_cols=["命中策略", "综合评分", "股票代码"],
        )
        if args.resume:
            save_checkpoint_df(checkpoint_path, result_df)
            print(f"检查点已更新: {checkpoint_path}")

    if result_df.empty and not pending_codes:
        result_df = checkpoint_df.copy()

    if result_df.empty and pending_codes:
        raise RuntimeError("本次执行未获取到有效结果，请检查网络或数据源")

    flow_stats = _build_flow_stats_from_result(result_df, input_count=len(codes))

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
                f"平均评分{flow_stats.get('平均评分', 0)}",
            ]
        )
    )

    print(f"\n全量结果已保存: {full_path}")
    print(f"命中结果已保存: {hit_path}")
    if args.resume:
        print(f"检查点文件: {checkpoint_path}")


if __name__ == "__main__":
    main()
