from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path
import re
import shutil

import pandas as pd
import streamlit as st
from pandas.errors import EmptyDataError

from backtest_signal_picks import _extract_signal_date_from_file, _fetch_hist, _load_signal_file, _fetch_index_close_series


APP_FILE = Path(__file__).resolve()
REPO_ROOT = APP_FILE.parents[1]
STOCK_DATA_DIR = REPO_ROOT / "stock_data"
UI_UPLOAD_DIR = STOCK_DATA_DIR / "ui_uploads"


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _extract_yyyymmdd_from_name(path: Path) -> str:
    m = re.search(r"(\d{8})", path.name)
    return m.group(1) if m else ""


def _in_date_range(date_text: str, start_text: str, end_text: str) -> bool:
    if not date_text:
        return False
    if start_text and date_text < start_text:
        return False
    if end_text and date_text > end_text:
        return False
    return True


def _find_signal_files(strategy_key: str, include_archive: bool, start_text: str, end_text: str) -> list[Path]:
    if strategy_key == "boll":
        patterns = ["stock_data/Stock-Selection-Boll-*.csv"]
        if include_archive:
            patterns.append("stock_data/archive/*/boll/Stock-Selection-Boll-*.csv")
    elif strategy_key == "relativity":
        patterns = ["stock_data/Stock-Selection-Relativity-*.csv"]
        if include_archive:
            patterns.append("stock_data/archive/*/theme/Stock-Selection-Relativity-*.csv")
    elif strategy_key == "theme":
        patterns = ["stock_data/Stock-Selection-Ashare-Theme-Turnover-*.csv"]
        if include_archive:
            patterns.append("stock_data/archive/*/theme/Stock-Selection-Ashare-Theme-Turnover-*.csv")
    else:
        patterns = ["stock_data/Stock-Selection-*.csv"]
        if include_archive:
            patterns.extend(
                [
                    "stock_data/archive/*/boll/Stock-Selection-*.csv",
                    "stock_data/archive/*/theme/Stock-Selection-*.csv",
                ]
            )

    files: list[Path] = []
    for p in patterns:
        files.extend(REPO_ROOT.glob(p))

    unique_files = sorted(set(files), key=lambda x: x.name)
    selected = []
    for f in unique_files:
        ds = _extract_yyyymmdd_from_name(f)
        if _in_date_range(ds, start_text, end_text):
            selected.append(f)
    return selected


def _run_python_script(script_path: Path, args: list[str]) -> tuple[int, str]:
    cmd = [sys.executable, str(script_path), *args]
    completed = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
    output = (completed.stdout or "") + ("\n" + completed.stderr if completed.stderr else "")
    return int(completed.returncode), output.strip()


def _show_dataframe_download(df: pd.DataFrame, label: str, filename: str) -> None:
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8-sig"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


def _safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except EmptyDataError:
        return pd.DataFrame()


def _build_signal_file_diagnostics(file_path: Path, top_n: int, hold_days: int) -> dict[str, object]:
    signal_date_text = _extract_signal_date_from_file(file_path)
    diag: dict[str, object] = {
        "文件": str(file_path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "信号日期": signal_date_text or "N/A",
        "候选数": 0,
        "可回测数": 0,
        "主要原因": "",
    }

    if not signal_date_text:
        diag["主要原因"] = "文件名未识别到日期"
        return diag

    try:
        signal_df = _load_signal_file(file_path, top_n=max(int(top_n), 1))
    except Exception as error:
        diag["主要原因"] = f"读取失败: {error}"
        return diag

    diag["候选数"] = int(len(signal_df))
    if signal_df.empty:
        diag["主要原因"] = "无有效股票代码"
        return diag

    signal_dt = pd.to_datetime(signal_date_text, format="%Y%m%d", errors="coerce")
    if pd.isna(signal_dt):
        diag["主要原因"] = "日期解析失败"
        return diag

    holding_days = max(int(hold_days), 1)
    end_dt = signal_dt + pd.Timedelta(days=holding_days * 3 + 15)
    valid_count = 0
    failure_reasons: list[str] = []

    for row in signal_df.itertuples(index=False):
        hist = _fetch_hist(str(row.code), signal_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))
        if hist.empty:
            failure_reasons.append(f"{row.code}: 无价格数据")
            continue

        after_signal = hist[hist["date"] > signal_dt].reset_index(drop=True)
        if len(after_signal) < holding_days:
            failure_reasons.append(f"{row.code}: 未来交易日不足({len(after_signal)}/{holding_days})")
            continue

        valid_count += 1

    diag["可回测数"] = int(valid_count)
    if valid_count == 0:
        diag["主要原因"] = failure_reasons[0] if failure_reasons else "没有可完成持有期的样本"
    elif failure_reasons:
        diag["主要原因"] = f"部分股票跳过: {failure_reasons[0]}"
    else:
        diag["主要原因"] = "全部可回测"

    return diag


def _build_backtested_stock_views(trades_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if trades_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    required_cols = ["信号日期", "策略", "股票代码", "股票名称", "净收益率(%)", "区间最大回撤(%)"]
    missing_cols = [c for c in required_cols if c not in trades_df.columns]
    if missing_cols:
        return pd.DataFrame(), pd.DataFrame()

    detail = trades_df[required_cols].copy()
    detail["净收益率(%)"] = pd.to_numeric(detail["净收益率(%)"], errors="coerce").fillna(0.0).round(3)
    detail["区间最大回撤(%)"] = pd.to_numeric(detail["区间最大回撤(%)"], errors="coerce").fillna(0.0).round(3)
    detail = detail.sort_values(["信号日期", "策略", "净收益率(%)"], ascending=[True, True, False]).reset_index(drop=True)

    def _codes_text(group: pd.DataFrame) -> str:
        pairs = [f"{r['股票代码']}({r['股票名称']})" for _, r in group[["股票代码", "股票名称"]].drop_duplicates().iterrows()]
        if len(pairs) <= 12:
            return ", ".join(pairs)
        return ", ".join(pairs[:12]) + f" ... 共{len(pairs)}只"

    daily = (
        detail.groupby(["信号日期", "策略"], as_index=False)
        .agg(
            回测股票数=("股票代码", "count"),
            平均净收益率=("净收益率(%)", "mean"),
            平均区间回撤=("区间最大回撤(%)", "mean"),
        )
        .sort_values(["信号日期", "策略"])
        .reset_index(drop=True)
    )
    daily["平均净收益率"] = daily["平均净收益率"].round(3)
    daily["平均区间回撤"] = daily["平均区间回撤"].round(3)

    code_text_df = (
        detail.groupby(["信号日期", "策略"])[["股票代码", "股票名称"]]
        .apply(lambda g: _codes_text(g.reset_index(drop=True)))
        .reset_index(name="当日回测股票")
    )
    daily = daily.merge(code_text_df, on=["信号日期", "策略"], how="left")
    return daily, detail


def _parse_yyyymmdd(value: object) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    dt = pd.NaT
    if len(digits) == 8:
        dt = pd.to_datetime(digits, format="%Y%m%d", errors="coerce")
    if pd.isna(dt):
        dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y%m%d")


def _to_datetime_from_signal_date_series(series: pd.Series) -> pd.Series:
    values = series.astype(str).str.strip()
    digits = values.str.replace(r"\D", "", regex=True)
    parsed_yyyymmdd = pd.to_datetime(digits.where(digits.str.len() == 8), format="%Y%m%d", errors="coerce")
    parsed_generic = pd.to_datetime(values, errors="coerce")
    return parsed_yyyymmdd.where(parsed_yyyymmdd.notna(), parsed_generic)


def _build_benchmark_compare_df(portfolio_daily_df: pd.DataFrame, target_total_return_pct: float) -> tuple[pd.DataFrame, dict[str, float]]:
    if portfolio_daily_df.empty or "信号日期" not in portfolio_daily_df.columns:
        return pd.DataFrame(), {}

    work = portfolio_daily_df.copy()
    work["_date"] = _to_datetime_from_signal_date_series(work["信号日期"])
    work = work.dropna(subset=["_date"]).sort_values("_date").reset_index(drop=True)
    if work.empty:
        return pd.DataFrame(), {}

    if "组合净收益率(%)" in work.columns:
        daily_ret = pd.to_numeric(work["组合净收益率(%)"], errors="coerce").fillna(0.0)
        work["组合累计收益率(%)"] = ((1.0 + daily_ret / 100.0).cumprod() - 1.0) * 100.0
    elif "组合资金(元)" in work.columns and len(work) > 0 and float(work["组合资金(元)"].iloc[0]) > 0:
        base = float(work["组合资金(元)"].iloc[0])
        work["组合累计收益率(%)"] = (pd.to_numeric(work["组合资金(元)"], errors="coerce") / base - 1.0) * 100.0
    else:
        return pd.DataFrame(), {}

    start_text = _parse_yyyymmdd(work["_date"].iloc[0])
    end_text = _parse_yyyymmdd(work["_date"].iloc[-1])
    if not start_text or not end_text:
        return pd.DataFrame(), {}

    sh_df = _fetch_index_close_series("sh.000001", start_text, end_text)
    hs300_df = _fetch_index_close_series("sh.000300", start_text, end_text)

    out = pd.DataFrame()
    out["_date"] = pd.to_datetime(work["_date"], errors="coerce")
    out["信号日期"] = out["_date"].dt.strftime("%Y%m%d")
    out["组合累计收益率(%)"] = pd.to_numeric(work["组合累计收益率(%)"], errors="coerce").fillna(0.0)

    target_daily = (1.0 + float(target_total_return_pct) / 100.0) ** (1.0 / max(len(out), 1)) - 1.0
    out["目标累计收益率(%)"] = (((1.0 + target_daily) ** (pd.RangeIndex(start=1, stop=len(out) + 1))) - 1.0) * 100.0

    def _index_change_metrics(index_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        if index_df.empty:
            nan_series = pd.Series([float("nan")] * len(out))
            return nan_series, nan_series

        tmp = index_df.copy().sort_values("date").reset_index(drop=True)
        tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
        tmp = tmp.dropna(subset=["close", "date"])
        if tmp.empty:
            nan_series = pd.Series([float("nan")] * len(out))
            return nan_series, nan_series

        base_df = out[["_date"]].sort_values("_date").reset_index(drop=True)
        idx_df = tmp[["date", "close"]].sort_values("date").reset_index(drop=True)
        merged = pd.merge_asof(base_df, idx_df, left_on="_date", right_on="date", direction="backward")

        closes = pd.to_numeric(merged["close"], errors="coerce")
        valid = closes.dropna()
        if valid.empty:
            nan_series = pd.Series([float("nan")] * len(out))
            return nan_series, nan_series

        base = float(valid.iloc[0])
        if base <= 0:
            nan_series = pd.Series([float("nan")] * len(out))
            return nan_series, nan_series

        cumulative = (closes / base - 1.0) * 100.0
        daily = closes.pct_change() * 100.0
        return cumulative, daily

    sh_cum, sh_daily = _index_change_metrics(sh_df)
    hs300_cum, hs300_daily = _index_change_metrics(hs300_df)
    out["上证指数累计变化(%)"] = sh_cum
    out["沪深300指数累计变化(%)"] = hs300_cum
    out["上证指数涨跌幅(%)"] = sh_daily
    out["沪深300指数涨跌幅(%)"] = hs300_daily
    out = out.drop(columns=["_date"], errors="ignore")

    summary = {
        "actual": float(out["组合累计收益率(%)"].iloc[-1]) if not out.empty else 0.0,
        "target": float(out["目标累计收益率(%)"].iloc[-1]) if not out.empty else float(target_total_return_pct),
        "sh_daily": float(out["上证指数涨跌幅(%)"].iloc[-1]) if (not out.empty and pd.notna(out["上证指数涨跌幅(%)"].iloc[-1])) else float("nan"),
        "hs300_daily": float(out["沪深300指数涨跌幅(%)"].iloc[-1]) if (not out.empty and pd.notna(out["沪深300指数涨跌幅(%)"].iloc[-1])) else float("nan"),
        "sh_cum": float(out["上证指数累计变化(%)"].iloc[-1]) if (not out.empty and pd.notna(out["上证指数累计变化(%)"].iloc[-1])) else float("nan"),
        "hs300_cum": float(out["沪深300指数累计变化(%)"].iloc[-1]) if (not out.empty and pd.notna(out["沪深300指数累计变化(%)"].iloc[-1])) else float("nan"),
    }
    return out, summary


def _render_tradebook_format_hint() -> None:
    st.markdown("必填字段格式（真实成交）")
    st.code(
        """日期,股票代码,买卖,成交价,数量,手续费
2026-04-01,600519,买,1500.00,100,5.00
2026-04-10,600519,卖,1560.00,100,5.00""",
        language="text",
    )
    st.caption("买卖字段支持：买/卖 或 buy/sell；日期建议 YYYY-MM-DD。")


def _render_tradebook_tab() -> None:
    st.subheader("真实成交回测")
    st.caption("不需要上传文件，直接在页面录入或粘贴交易数据。")
    _render_tradebook_format_hint()

    default_df = pd.DataFrame(
        [
            {"日期": "2026-04-01", "股票代码": "600519", "买卖": "买", "成交价": 1500.00, "数量": 100, "手续费": 5.00},
            {"日期": "2026-04-10", "股票代码": "600519", "买卖": "卖", "成交价": 1560.00, "数量": 100, "手续费": 5.00},
        ]
    )
    trade_df = st.data_editor(
        default_df,
        num_rows="dynamic",
        use_container_width=True,
        key="tradebook_editor",
    )

    output_prefix = st.text_input(
        "输出前缀（可选）",
        value="",
        placeholder="例如 stock_data/Trade-Backtest-UI",
        help="留空会自动按日期写入 stock_data。",
    ).strip()

    run_btn = st.button("运行真实成交回测", type="primary", use_container_width=True)
    if not run_btn:
        return

    run_id = f"tradebook_{_ts()}"
    run_dir = UI_UPLOAD_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    required_cols = ["日期", "股票代码", "买卖", "成交价", "数量", "手续费"]
    if any(c not in trade_df.columns for c in required_cols):
        st.error("表格字段不完整，请保留：日期、股票代码、买卖、成交价、数量、手续费")
        return

    cleaned = trade_df[required_cols].copy()
    cleaned["日期"] = cleaned["日期"].astype(str).str.strip()
    cleaned["股票代码"] = cleaned["股票代码"].astype(str).str.strip()
    cleaned["买卖"] = cleaned["买卖"].astype(str).str.strip()
    cleaned["成交价"] = pd.to_numeric(cleaned["成交价"], errors="coerce")
    cleaned["数量"] = pd.to_numeric(cleaned["数量"], errors="coerce")
    cleaned["手续费"] = pd.to_numeric(cleaned["手续费"], errors="coerce").fillna(0.0)
    cleaned = cleaned.dropna(subset=["成交价", "数量"])
    cleaned = cleaned[(cleaned["日期"] != "") & (cleaned["股票代码"] != "") & (cleaned["买卖"] != "")]

    if cleaned.empty:
        st.warning("请至少填写一条有效交易记录。")
        return

    trades_csv = run_dir / "tradebook_input.csv"
    cleaned.to_csv(trades_csv, index=False, encoding="utf-8-sig")

    args: list[str] = ["--trades-csv", str(trades_csv.relative_to(REPO_ROOT)).replace("\\", "/")]

    if output_prefix:
        args.extend(["--output-prefix", output_prefix])
        out_prefix = output_prefix
    else:
        out_prefix = f"stock_data/Trade-Backtest-UI-{datetime.now().strftime('%Y%m%d')}"
        args.extend(["--output-prefix", out_prefix])

    script = APP_FILE.parent / "backtest_tradebook.py"
    with st.spinner("正在运行真实成交回测..."):
        code, output = _run_python_script(script, args)

    with st.expander("运行日志", expanded=True):
        st.text(output or "无输出")

    if code != 0:
        st.error("真实成交回测失败，请检查日志和输入列名。")
        return

    st.success("真实成交回测完成。")

    summary_path = REPO_ROOT / f"{out_prefix}-summary.csv"
    detail_path = REPO_ROOT / f"{out_prefix}-closed-trades.csv"
    curve_path = REPO_ROOT / f"{out_prefix}-equity-curve.csv"

    if summary_path.exists():
        summary_df = _safe_read_csv(summary_path)
        if not summary_df.empty:
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            _show_dataframe_download(summary_df, "下载 summary.csv", summary_path.name)

    if detail_path.exists():
        detail_df = _safe_read_csv(detail_path)
        if not detail_df.empty:
            st.dataframe(detail_df.head(300), use_container_width=True, hide_index=True)
            _show_dataframe_download(detail_df, "下载 closed-trades.csv", detail_path.name)
        else:
            st.info("closed-trades.csv 为空：当前回测没有可配对成交。")

    if curve_path.exists():
        curve_df = _safe_read_csv(curve_path)
        if not curve_df.empty:
            st.line_chart(curve_df, x="日期", y="累计收益(元)")
            _show_dataframe_download(curve_df, "下载 equity-curve.csv", curve_path.name)


def _render_signal_tab() -> None:
    st.subheader("信号样本回测")
    st.caption("自动读取历史信号文件（不需要上传，按交易日回测，周末和节假日自动跳过）。")
    st.markdown("所需字段格式（信号文件）")
    st.code(
        """股票代码,股票名称,...
600519,贵州茅台,...
000001,平安银行,...""",
        language="text",
    )
    st.caption("文件名需包含日期 YYYYMMDD，例如 Stock-Selection-Boll-20260412.csv。系统只会读取交易日文件，不会把周末/节假日当成信号日。")
    st.caption("题材策略默认仅回测命中 CCTV 的股票（优先按 CCTV 股票池过滤，缺失时按题材命中标记回退）。")

    st.markdown("参数设置")
    with st.expander("基础参数", expanded=True):
        col_sel_1, col_sel_2, col_sel_3 = st.columns(3)
        strategy_key = col_sel_1.selectbox(
            "信号来源",
            options=["boll", "relativity", "theme", "all"],
            format_func=lambda x: {
                "boll": "Boll",
                "relativity": "Relativity",
                "theme": "Theme-Turnover",
                "all": "全部",
            }[x],
            index=3,
        )
        include_archive = col_sel_2.checkbox("包含 archive 历史", value=True)
        end_date = datetime.now().date()
        start_date = col_sel_3.date_input("起始日期", value=end_date.replace(day=1))
        end_date_input = st.date_input("结束日期", value=end_date)

        col1, col2 = st.columns(2)
        top_n = int(col1.number_input("每个信号日前 N 只", min_value=1, max_value=100, value=10, step=1))
        hold_days = int(col2.number_input("持有交易日", min_value=1, max_value=30, value=5, step=1))

        col3, col4, col5 = st.columns(3)
        buy_slip_bps = float(col3.number_input("买入滑点 bps", min_value=0.0, max_value=100.0, value=5.0, step=1.0))
        sell_slip_bps = float(col4.number_input("卖出滑点 bps", min_value=0.0, max_value=100.0, value=5.0, step=1.0))
        sell_stamp_tax_rate = float(
            col5.number_input("卖出印花税率", min_value=0.0, max_value=0.01, value=0.001, step=0.0001, format="%.4f")
        )

        col6, col7 = st.columns(2)
        buy_fee_rate = float(
            col6.number_input("买入佣金率", min_value=0.0, max_value=0.01, value=0.0003, step=0.0001, format="%.4f")
        )
        sell_fee_rate = float(
            col7.number_input("卖出佣金率", min_value=0.0, max_value=0.01, value=0.0003, step=0.0001, format="%.4f")
        )
        relativity_min_down_ratio_pct = float(
            st.number_input("Relativity 抗跌满足率下限(%)", min_value=0.0, max_value=100.0, value=70.0, step=1.0)
        )
        allow_cache_fallback = st.checkbox(
            "akshare失败时允许本地缓存兜底",
            value=True,
            help="关闭后将严格只用在线数据，若接口返回空则该样本会被跳过。",
        )

    with st.expander("配比参数", expanded=True):
        st.markdown("每日策略配比（按日复利回测）")
        c1, c2, c3, c4, c5 = st.columns(5)
        ratio_boll = float(c1.number_input("Boll(%)", min_value=0.0, max_value=100.0, value=40.0, step=1.0))
        ratio_theme = float(c2.number_input("Theme(%)", min_value=0.0, max_value=100.0, value=25.0, step=1.0))
        ratio_relativity = float(c3.number_input("Relativity(%)", min_value=0.0, max_value=100.0, value=20.0, step=1.0))
        ratio_cctv = float(c4.number_input("CCTV(%)", min_value=0.0, max_value=100.0, value=10.0, step=1.0))
        ratio_cash = float(c5.number_input("现金(%)", min_value=0.0, max_value=100.0, value=5.0, step=1.0))
        auto_full_single = st.checkbox("单策略来源自动满仓(100%)", value=True)
        auto_market_ratios = st.checkbox("按每日市场分析自动配比", value=True)
        initial_capital = float(
            st.number_input("组合初始资金(元)", min_value=1000.0, max_value=1000000000.0, value=100000.0, step=10000.0)
        )
        target_total_return_pct = float(
            st.number_input("目标累计收益率(%)", min_value=-100.0, max_value=10000.0, value=5.0, step=1.0)
        )

        ratio_csv = st.file_uploader(
            "按日期配比CSV（可选，会覆盖当日默认配比）",
            type=["csv"],
            accept_multiple_files=False,
            key="daily_ratio_csv",
        )
        st.caption("CSV列示例：信号日期,boll,theme,relativity,cctv,cash；日期支持 YYYYMMDD 或 YYYY-MM-DD。")
        st.caption("配比优先级：配比CSV > 市场自动配比 > 手工输入配比。")

    if auto_full_single and strategy_key in {"boll", "relativity", "theme"}:
        ratio_boll = 100.0 if strategy_key == "boll" else 0.0
        ratio_theme = 100.0 if strategy_key == "theme" else 0.0
        ratio_relativity = 100.0 if strategy_key == "relativity" else 0.0
        ratio_cctv = 0.0
        ratio_cash = 0.0
        st.info("已启用单策略自动满仓：当前来源会按100%权重回测。")

    ratio_total = ratio_boll + ratio_theme + ratio_relativity + ratio_cctv + ratio_cash
    st.caption(f"当前配比合计 {ratio_total:.1f}%（脚本会自动归一化到 100%）。")
    ratio_text = (
        f"boll={ratio_boll},theme={ratio_theme},relativity={ratio_relativity},"
        f"cctv={ratio_cctv},cash={ratio_cash}"
    )

    output_prefix = st.text_input(
        "输出前缀（可选）",
        value="",
        placeholder="例如 stock_data/Signal-Backtest-UI",
        help="留空会自动按日期写入 stock_data。",
        key="signal_output_prefix",
    ).strip()

    run_btn = st.button("运行信号样本回测", type="primary", use_container_width=True)
    start_text = start_date.strftime("%Y%m%d")
    end_text = end_date_input.strftime("%Y%m%d")

    if start_date > end_date_input:
        st.warning("起始日期不能晚于结束日期。")
        return

    signal_files = _find_signal_files(strategy_key, include_archive, start_text=start_text, end_text=end_text)

    st.caption(f"自动匹配到 {len(signal_files)} 个信号文件")
    st.info(f"当前持有期 {hold_days} 个交易日，回测脚本会自动跳过持有期不足的文件。")

    show_diagnostics = st.checkbox("显示文件诊断（较慢）", value=False)
    if show_diagnostics and signal_files:
        diag_rows = [_build_signal_file_diagnostics(path, top_n=top_n, hold_days=hold_days) for path in signal_files]
        preview = pd.DataFrame(diag_rows)
        st.dataframe(preview, use_container_width=True, hide_index=True)

    if not run_btn:
        return

    if not signal_files:
        st.warning("当前筛选条件下未找到信号文件，请放宽日期范围或切换来源。")
        return

    run_id = f"signal_{_ts()}"
    run_dir = UI_UPLOAD_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    ratio_csv_rel = ""
    if ratio_csv is not None:
        ratio_csv_path = run_dir / "daily-ratios.csv"
        ratio_csv_path.write_bytes(ratio_csv.getvalue())
        ratio_csv_rel = ratio_csv_path.relative_to(REPO_ROOT).as_posix()

    for idx, src in enumerate(signal_files, start=1):
        dst = run_dir / f"{idx:04d}_{src.name}"
        shutil.copy2(src, dst)

    signals_glob = f"{run_dir.relative_to(REPO_ROOT).as_posix()}/*.csv"

    if output_prefix:
        out_prefix = output_prefix
    else:
        out_prefix = f"stock_data/Signal-Backtest-UI-{datetime.now().strftime('%Y%m%d')}"

    script = APP_FILE.parent / "backtest_signal_picks.py"
    args = [
        "--signals-glob",
        signals_glob,
        "--top-n",
        str(top_n),
        "--hold-days",
        str(hold_days),
        "--buy-slip-bps",
        str(buy_slip_bps),
        "--sell-slip-bps",
        str(sell_slip_bps),
        "--buy-fee-rate",
        str(buy_fee_rate),
        "--sell-fee-rate",
        str(sell_fee_rate),
        "--sell-stamp-tax-rate",
        str(sell_stamp_tax_rate),
        "--relativity-min-down-ratio-pct",
        str(relativity_min_down_ratio_pct),
        "--daily-strategy-ratios",
        ratio_text,
        "--initial-capital",
        str(initial_capital),
        "--output-prefix",
        out_prefix,
    ]
    if ratio_csv_rel:
        args.extend(["--daily-ratios-csv", ratio_csv_rel])
    if auto_market_ratios:
        args.append("--auto-market-ratios")
    if not allow_cache_fallback:
        args.append("--no-cache-fallback")

    with st.spinner("正在运行信号样本回测..."):
        code, output = _run_python_script(script, args)

    with st.expander("运行日志", expanded=(code != 0)):
        st.text(output or "无输出")

    if code != 0:
        st.error("信号样本回测失败，请检查日志。")
        return

    st.success("信号样本回测完成。")

    summary_path = REPO_ROOT / f"{out_prefix}-summary.csv"
    trades_path = REPO_ROOT / f"{out_prefix}-trades.csv"
    daily_path = REPO_ROOT / f"{out_prefix}-daily.csv"
    portfolio_summary_path = REPO_ROOT / f"{out_prefix}-portfolio-summary.csv"
    portfolio_daily_path = REPO_ROOT / f"{out_prefix}-portfolio-daily.csv"

    summary_df = _safe_read_csv(summary_path) if summary_path.exists() else pd.DataFrame()
    daily_df = _safe_read_csv(daily_path) if daily_path.exists() else pd.DataFrame()
    trades_df = _safe_read_csv(trades_path) if trades_path.exists() else pd.DataFrame()
    portfolio_summary_df = _safe_read_csv(portfolio_summary_path) if portfolio_summary_path.exists() else pd.DataFrame()
    portfolio_daily_df = _safe_read_csv(portfolio_daily_path) if portfolio_daily_path.exists() else pd.DataFrame()
    daily_stock_df, stock_detail_df = _build_backtested_stock_views(trades_df)

    if trades_df.empty and signal_files:
        st.warning(
            "本次回测结果为 0。常见原因是信号日期太近，按当前持有天数无法凑够未来交易日（例如持有5天但最新信号只有1-2个交易日数据）。"
        )
        sample_paths = signal_files[: min(len(signal_files), 8)]
        diag_rows = [_build_signal_file_diagnostics(path, top_n=top_n, hold_days=hold_days) for path in sample_paths]
        diag_df = pd.DataFrame(diag_rows)
        if not diag_df.empty:
            st.caption("快速诊断（前8个文件）")
            st.dataframe(diag_df, use_container_width=True, hide_index=True)
            st.caption("建议：降低持有交易日，或把结束日期提前到更早的信号日。")

    result_tab_1, result_tab_2, result_tab_3, result_tab_4 = st.tabs(["结果总览", "每日配比", "回测股票", "下载结果"])

    with result_tab_1:
        col_a, col_b, col_c, col_d = st.columns(4)
        trade_count = int(len(trades_df)) if not trades_df.empty else 0
        signal_days = int(trades_df["信号日期"].nunique()) if (not trades_df.empty and "信号日期" in trades_df.columns) else 0
        final_capital = "-"
        total_ret = "-"
        if not portfolio_summary_df.empty:
            if "期末资金(元)" in portfolio_summary_df.columns:
                final_capital = f"{float(portfolio_summary_df.iloc[0]['期末资金(元)']):,.2f}"
            if "组合累计收益率(%)" in portfolio_summary_df.columns:
                total_ret = f"{float(portfolio_summary_df.iloc[0]['组合累计收益率(%)']):.3f}%"

        col_a.metric("回测样本数", trade_count)
        col_b.metric("覆盖信号日", signal_days)
        col_c.metric("期末资金", final_capital)
        col_d.metric("组合累计收益", total_ret)

        benchmark_df, benchmark_summary = _build_benchmark_compare_df(
            portfolio_daily_df=portfolio_daily_df,
            target_total_return_pct=target_total_return_pct,
        )
        if benchmark_summary:
            st.markdown("目标与指数对标")
            b1, b2, b3, b4 = st.columns(4)
            b1.metric("组合累计收益率", f"{benchmark_summary.get('actual', 0.0):.3f}%")
            b2.metric("目标累计收益率", f"{benchmark_summary.get('target', 0.0):.3f}%")
            sh_text = "N/A" if pd.isna(benchmark_summary.get("sh_daily", float("nan"))) else f"{benchmark_summary.get('sh_daily', 0.0):+.3f}%"
            hs300_text = "N/A" if pd.isna(benchmark_summary.get("hs300_daily", float("nan"))) else f"{benchmark_summary.get('hs300_daily', 0.0):+.3f}%"
            b3.metric("上证指数涨跌幅", sh_text)
            b4.metric("沪深300指数涨跌幅", hs300_text)

            excess_target = f"{benchmark_summary.get('actual', 0.0) - benchmark_summary.get('target', 0.0):+.3f}%"
            if pd.isna(benchmark_summary.get("sh_cum", float("nan"))):
                excess_sh = "N/A"
            else:
                excess_sh = f"{benchmark_summary.get('actual', 0.0) - benchmark_summary.get('sh_cum', 0.0):+.3f}%"

            if pd.isna(benchmark_summary.get("hs300_cum", float("nan"))):
                excess_hs300 = "N/A"
            else:
                excess_hs300 = f"{benchmark_summary.get('actual', 0.0) - benchmark_summary.get('hs300_cum', 0.0):+.3f}%"

            st.caption(
                f"超额收益: 对目标 {excess_target} | 对上证 {excess_sh} | 对沪深300 {excess_hs300}"
            )

        if not benchmark_df.empty:
            chart_cols = ["组合累计收益率(%)", "目标累计收益率(%)"]
            if "上证指数累计变化(%)" in benchmark_df.columns and benchmark_df["上证指数累计变化(%)"].notna().any():
                chart_cols.append("上证指数累计变化(%)")
            if "沪深300指数累计变化(%)" in benchmark_df.columns and benchmark_df["沪深300指数累计变化(%)"].notna().any():
                chart_cols.append("沪深300指数累计变化(%)")
            st.line_chart(benchmark_df, x="信号日期", y=chart_cols)
        if not portfolio_summary_df.empty:
            st.dataframe(portfolio_summary_df, use_container_width=True, hide_index=True)
        if not summary_df.empty:
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

    with result_tab_2:
        if portfolio_daily_df.empty:
            st.info("暂无每日配比数据。")
        else:
            ratio_cols = [c for c in ["boll权重(%)", "theme权重(%)", "relativity权重(%)", "cctv权重(%)", "cash权重(%)"] if c in portfolio_daily_df.columns]
            show_cols = ["信号日期", "配比来源", "市场状态", "配比原因", *ratio_cols, "组合净收益率(%)", "组合资金(元)", "组合回撤(%)"]
            show_cols = [c for c in show_cols if c in portfolio_daily_df.columns]
            st.dataframe(portfolio_daily_df[show_cols], use_container_width=True, hide_index=True)
            if ratio_cols:
                st.line_chart(portfolio_daily_df, x="信号日期", y=ratio_cols)

    with result_tab_3:
        if trades_df.empty:
            st.info("trades.csv 为空：当前条件下没有形成可回测样本（例如信号文件为空或持有期不足）。")
        else:
            st.markdown("每日回测股票清单")
            if not daily_stock_df.empty:
                st.dataframe(daily_stock_df, use_container_width=True, hide_index=True)
            st.markdown("回测股票明细")
            if not stock_detail_df.empty:
                st.dataframe(stock_detail_df, use_container_width=True, hide_index=True)

    with result_tab_4:
        if not summary_df.empty:
            _show_dataframe_download(summary_df, "下载 summary.csv", summary_path.name)
        if not daily_df.empty:
            _show_dataframe_download(daily_df, "下载 daily.csv", daily_path.name)
        if not trades_df.empty:
            _show_dataframe_download(trades_df, "下载 trades.csv", trades_path.name)
        if not portfolio_summary_df.empty:
            _show_dataframe_download(portfolio_summary_df, "下载 portfolio-summary.csv", portfolio_summary_path.name)
        if not portfolio_daily_df.empty:
            _show_dataframe_download(portfolio_daily_df, "下载 portfolio-daily.csv", portfolio_daily_path.name)


def main() -> None:
    st.set_page_config(page_title="Stocks-Master 回测中心", layout="wide")
    st.title("Stocks-Master 回测中心")
    st.caption("页面录入/自动读取历史数据 -> 填参数 -> 一键回测 -> 下载结果")

    tab1, tab2 = st.tabs(["真实成交回测", "信号样本回测"])
    with tab1:
        _render_tradebook_tab()
    with tab2:
        _render_signal_tab()


if __name__ == "__main__":
    main()
