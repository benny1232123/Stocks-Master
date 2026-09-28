#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""融资融券余额面板（新数据维度，2026-09-28）。

数据源：akshare `stock_margin_detail_sse` / `stock_margin_detail_szse`（交易所信用
账户明细，按日切片，覆盖两融标的 ~4100 只）。逐日落盘缓存
stock_data/margin_cache/{date}.csv；`load_panel(dates)` 合并为 (日期 × 代码) 的
融资余额面板，**整体滞后 1 档**（T 日明细 T+1 早发布，信号日 T 只能用 T-1——因果纪律）。

非两融标的无数据 → NaN（因子端按缺失中性处理）。
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR

CACHE_DIR = STOCK_DATA_DIR / "margin_cache"
LAG = 1  # 发布滞后（交易日档）


def _norm_code(x) -> str:
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = s.split(".")[0]
    return s.zfill(6) if s.isdigit() else ""


def fetch_day(date_yyyymmdd: str) -> pd.Series | None:
    """单日全市场融资余额 {code6: balance}；带磁盘缓存；失败返回 None。"""
    cf = CACHE_DIR / f"{date_yyyymmdd}.csv"
    if cf.exists():
        try:
            d = pd.read_csv(cf, dtype={"code": str})
            if len(d):
                return pd.Series(d["balance"].values, index=d["code"].values)
        except Exception:
            pass
    import akshare as ak
    parts = []
    for fn, code_col, bal_col in (("stock_margin_detail_sse", "标的证券代码", "融资余额"),
                                  ("stock_margin_detail_szse", "证券代码", "融资余额")):
        try:
            d = getattr(ak, fn)(date=date_yyyymmdd)
        except Exception:
            continue
        if d is None or d.empty or code_col not in d.columns:
            continue
        d = d[[code_col, bal_col]].copy()
        d["code"] = d[code_col].map(_norm_code)
        d["balance"] = pd.to_numeric(d[bal_col], errors="coerce")
        d = d.dropna()
        d = d[d["code"] != ""]
        parts.append(d.set_index("code")["balance"])
    if not parts:
        return None
    s = pd.concat(parts)
    s = s[~s.index.duplicated(keep="last")].astype(float)
    s.name = "balance"
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        s.to_frame().to_csv(cf, encoding="utf-8-sig")
    except Exception:
        pass
    return s


def load_panel(dates, codes=None, lag: int = LAG) -> pd.DataFrame | None:
    """融资余额面板，按 grid 交易日对齐并整体滞后 `lag` 档。

    dates: 因子网格的交易日索引（DatetimeIndex）。
    codes: 可选列裁剪。返回 DataFrame(index=dates, columns=codes)，值=融资余额（元）；
    单日获取失败 → 该行为 NaN（因子端中性）。
    """
    dates = pd.DatetimeIndex(dates)
    str_dates = [d.strftime("%Y%m%d") for d in dates]
    rows, got = [], 0
    for sd in str_dates:
        s = fetch_day(sd)
        rows.append(s if s is not None else pd.Series(dtype=float))
        got += 1 if s is not None else 0
    if got == 0:
        return None
    panel = pd.DataFrame(rows, index=dates)
    panel = panel.shift(lag)  # T+1 发布 → 信号日只能用 T-1
    if codes is not None:
        cols = [c for c in panel.columns if c in set(map(str, codes))]
        panel = panel.reindex(columns=cols)
    return panel if not panel.empty else None
