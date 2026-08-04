from __future__ import annotations

import argparse
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
STOCK_DATA_DIR = ROOT_DIR / "stock_data"


@dataclass
class OpenLot:
    code: str
    buy_date: str
    buy_price: float
    quantity: float
    fee: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="根据买卖成交记录回测策略有效性")
    parser.add_argument("--trades-csv", default="", help="交易流水CSV（建议包含买入/卖出）")
    parser.add_argument("--buy-csv", default="", help="买入记录CSV（可选）")
    parser.add_argument("--sell-csv", default="", help="卖出记录CSV（可选）")
    parser.add_argument("--output-prefix", default="", help="输出文件前缀，默认 stock_data/Trade-Backtest-YYYYMMDD")
    return parser.parse_args()


def _resolve_path(path_text: str) -> Path:
    p = Path(path_text)
    if not p.is_absolute():
        p = (ROOT_DIR / p).resolve()
    return p


def _pick_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str:
    lower_map = {str(col).strip().lower(): col for col in df.columns}
    for c in candidates:
        key = c.strip().lower()
        if key in lower_map:
            return str(lower_map[key])
    if required:
        raise ValueError(f"缺少列，候选: {candidates}")
    return ""


def _normalize_code(value: object) -> str:
    text = str(value or "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text.strip()


def _normalize_side(value: object) -> str:
    txt = str(value or "").strip().lower()
    if txt in {"buy", "b", "long", "买", "买入"}:
        return "BUY"
    if txt in {"sell", "s", "short", "卖", "卖出"}:
        return "SELL"
    return ""


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def load_trades_single_file(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    if df.empty:
        return pd.DataFrame(columns=["date", "code", "side", "price", "quantity", "fee"])

    date_col = _pick_column(df, ["date", "trade_date", "日期", "成交日期", "交易日期"])
    code_col = _pick_column(df, ["code", "股票代码", "symbol", "证券代码"])
    side_col = _pick_column(df, ["side", "方向", "action", "买卖", "交易方向"])
    price_col = _pick_column(df, ["price", "成交价", "成交均价", "均价", "trade_price"])
    qty_col = _pick_column(df, ["quantity", "数量", "成交数量", "成交股数", "volume"], required=False)
    fee_col = _pick_column(df, ["fee", "手续费", "佣金", "费用", "cost"], required=False)

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    out["code"] = df[code_col].apply(_normalize_code)
    out["side"] = df[side_col].apply(_normalize_side)
    out["price"] = pd.to_numeric(df[price_col], errors="coerce")
    out["quantity"] = pd.to_numeric(df[qty_col], errors="coerce") if qty_col else 1.0
    out["fee"] = pd.to_numeric(df[fee_col], errors="coerce") if fee_col else 0.0

    out["quantity"] = out["quantity"].fillna(1.0)
    out["fee"] = out["fee"].fillna(0.0)

    out = out.dropna(subset=["date", "code", "side", "price"]).copy()
    out = out[out["side"].isin(["BUY", "SELL"])].copy()
    out = out[out["quantity"] > 0].copy()
    return out.sort_values(["date", "code"]).reset_index(drop=True)


def load_trades_two_files(buy_path: Path, sell_path: Path) -> pd.DataFrame:
    buy_df = pd.read_csv(buy_path, encoding="utf-8-sig")
    sell_df = pd.read_csv(sell_path, encoding="utf-8-sig")

    def _normalize(df: pd.DataFrame, side_text: str) -> pd.DataFrame:
        date_col = _pick_column(df, ["date", "trade_date", "日期", "成交日期", "交易日期"])
        code_col = _pick_column(df, ["code", "股票代码", "symbol", "证券代码"])
        price_col = _pick_column(df, ["price", "成交价", "成交均价", "均价", "trade_price"])
        qty_col = _pick_column(df, ["quantity", "数量", "成交数量", "成交股数", "volume"], required=False)
        fee_col = _pick_column(df, ["fee", "手续费", "佣金", "费用", "cost"], required=False)

        out = pd.DataFrame()
        out["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
        out["code"] = df[code_col].apply(_normalize_code)
        out["side"] = side_text
        out["price"] = pd.to_numeric(df[price_col], errors="coerce")
        out["quantity"] = pd.to_numeric(df[qty_col], errors="coerce") if qty_col else 1.0
        out["fee"] = pd.to_numeric(df[fee_col], errors="coerce") if fee_col else 0.0
        out["quantity"] = out["quantity"].fillna(1.0)
        out["fee"] = out["fee"].fillna(0.0)
        out = out.dropna(subset=["date", "code", "price"]).copy()
        out = out[out["quantity"] > 0].copy()
        return out

    merged = pd.concat([_normalize(buy_df, "BUY"), _normalize(sell_df, "SELL")], ignore_index=True)
    return merged.sort_values(["date", "code", "side"]).reset_index(drop=True)


def match_trades_fifo(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame(
            columns=[
                "股票代码",
                "买入日期",
                "卖出日期",
                "买入价",
                "卖出价",
                "数量",
                "持有天数",
                "单笔收益率(%)",
                "单笔收益(元)",
                "买入手续费",
                "卖出手续费",
            ]
        )

    lots: dict[str, deque[OpenLot]] = defaultdict(deque)
    closed_rows: list[dict[str, object]] = []

    for row in trades_df.itertuples(index=False):
        trade_date = str(row.date)
        code = str(row.code)
        side = str(row.side)
        price = float(row.price)
        qty = float(row.quantity)
        fee = float(row.fee)

        if side == "BUY":
            lots[code].append(OpenLot(code=code, buy_date=trade_date, buy_price=price, quantity=qty, fee=fee))
            continue

        remaining = qty
        while remaining > 0 and lots[code]:
            lot = lots[code][0]
            matched_qty = min(remaining, lot.quantity)

            buy_dt = pd.to_datetime(lot.buy_date, errors="coerce")
            sell_dt = pd.to_datetime(trade_date, errors="coerce")
            hold_days = (sell_dt - buy_dt).days if (pd.notna(buy_dt) and pd.notna(sell_dt)) else 0

            buy_amount = lot.buy_price * matched_qty
            sell_amount = price * matched_qty
            buy_fee_alloc = lot.fee * (matched_qty / lot.quantity) if lot.quantity > 0 else 0.0
            sell_fee_alloc = fee * (matched_qty / qty) if qty > 0 else 0.0
            pnl = (sell_amount - buy_amount) - buy_fee_alloc - sell_fee_alloc
            ret = (pnl / buy_amount * 100.0) if buy_amount > 0 else 0.0

            closed_rows.append(
                {
                    "股票代码": code,
                    "买入日期": lot.buy_date,
                    "卖出日期": trade_date,
                    "买入价": round(lot.buy_price, 4),
                    "卖出价": round(price, 4),
                    "数量": round(matched_qty, 4),
                    "持有天数": int(max(hold_days, 0)),
                    "单笔收益率(%)": round(ret, 3),
                    "单笔收益(元)": round(pnl, 2),
                    "买入手续费": round(buy_fee_alloc, 2),
                    "卖出手续费": round(sell_fee_alloc, 2),
                }
            )

            remaining -= matched_qty
            lot.quantity -= matched_qty
            if lot.quantity <= 1e-9:
                lots[code].popleft()

    return pd.DataFrame(closed_rows)


def summarize_backtest(closed_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if closed_df.empty:
        summary = pd.DataFrame(
            [
                {
                    "交易笔数": 0,
                    "胜率(%)": 0.0,
                    "平均单笔收益率(%)": 0.0,
                    "中位单笔收益率(%)": 0.0,
                    "总收益(元)": 0.0,
                    "盈亏比": 0.0,
                    "平均持有天数": 0.0,
                    "最大回撤(元)": 0.0,
                }
            ]
        )
        return summary, pd.DataFrame(columns=["日期", "累计收益(元)", "回撤(元)"])

    returns = pd.to_numeric(closed_df["单笔收益率(%)"], errors="coerce").fillna(0.0)
    pnls = pd.to_numeric(closed_df["单笔收益(元)"], errors="coerce").fillna(0.0)
    hold_days = pd.to_numeric(closed_df["持有天数"], errors="coerce").fillna(0.0)

    win_mask = pnls > 0
    win_rate = float(win_mask.mean() * 100.0)
    avg_ret = float(returns.mean())
    median_ret = float(returns.median())
    total_pnl = float(pnls.sum())

    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = float(-pnls[pnls < 0].sum())
    profit_factor = (gross_profit / gross_loss) if gross_loss > 1e-12 else float("inf")

    curve = closed_df.copy()
    curve["日期"] = pd.to_datetime(curve["卖出日期"], errors="coerce")
    curve = curve.dropna(subset=["日期"]).sort_values("日期")
    curve_daily = curve.groupby("日期", as_index=False)["单笔收益(元)"].sum()
    curve_daily["累计收益(元)"] = curve_daily["单笔收益(元)"].cumsum()
    curve_daily["历史峰值(元)"] = curve_daily["累计收益(元)"].cummax()
    curve_daily["回撤(元)"] = curve_daily["累计收益(元)"] - curve_daily["历史峰值(元)"]
    max_drawdown = float(curve_daily["回撤(元)"].min()) if not curve_daily.empty else 0.0

    summary = pd.DataFrame(
        [
            {
                "交易笔数": int(len(closed_df)),
                "胜率(%)": round(win_rate, 2),
                "平均单笔收益率(%)": round(avg_ret, 3),
                "中位单笔收益率(%)": round(median_ret, 3),
                "总收益(元)": round(total_pnl, 2),
                "盈亏比": ("INF" if profit_factor == float("inf") else round(profit_factor, 3)),
                "平均持有天数": round(float(hold_days.mean()), 2),
                "最大回撤(元)": round(max_drawdown, 2),
            }
        ]
    )
    return summary, curve_daily[["日期", "累计收益(元)", "回撤(元)"]]


def default_output_prefix() -> Path:
    today_text = pd.Timestamp.now().strftime("%Y%m%d")
    return STOCK_DATA_DIR / f"Trade-Backtest-{today_text}"


def main() -> int:
    args = parse_args()

    if not args.trades_csv and not (args.buy_csv and args.sell_csv):
        raise SystemExit("请提供 --trades-csv，或同时提供 --buy-csv 与 --sell-csv")

    if args.trades_csv:
        trades_path = _resolve_path(args.trades_csv)
        if not trades_path.exists():
            raise SystemExit(f"未找到交易流水文件: {trades_path}")
        trades_df = load_trades_single_file(trades_path)
    else:
        buy_path = _resolve_path(args.buy_csv)
        sell_path = _resolve_path(args.sell_csv)
        if not buy_path.exists():
            raise SystemExit(f"未找到买入文件: {buy_path}")
        if not sell_path.exists():
            raise SystemExit(f"未找到卖出文件: {sell_path}")
        trades_df = load_trades_two_files(buy_path, sell_path)

    if trades_df.empty:
        raise SystemExit("未读取到有效交易记录，请检查CSV列名和内容")

    closed_df = match_trades_fifo(trades_df)
    summary_df, curve_df = summarize_backtest(closed_df)

    out_prefix = _resolve_path(args.output_prefix) if args.output_prefix.strip() else default_output_prefix()
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    raw_path = Path(str(out_prefix) + "-raw-trades.csv")
    detail_path = Path(str(out_prefix) + "-closed-trades.csv")
    summary_path = Path(str(out_prefix) + "-summary.csv")
    curve_path = Path(str(out_prefix) + "-equity-curve.csv")

    trades_df.to_csv(raw_path, index=False, encoding="utf-8-sig")
    closed_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    curve_df.to_csv(curve_path, index=False, encoding="utf-8-sig")

    print("回测完成。")
    print(summary_df.to_string(index=False))
    print(f"已保存: {raw_path}")
    print(f"已保存: {detail_path}")
    print(f"已保存: {summary_path}")
    print(f"已保存: {curve_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
