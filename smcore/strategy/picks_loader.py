"""各策略当日结果 CSV 的加载与回退逻辑。

从 fusion.py 抽出「按日期找策略 CSV（限制回退窗口）+ 泛型解析」。
所有策略统一产出「综合分」契约的 Stock-Selection-<Label>-*.csv，由 _load_scored_picks
按 factor_types.STRATEGY_ORDER 泛型装载；缺失一律 fail-soft 返回空 dict。
（历史上 boll/relativity/theme/cctv/momentum 各有专属格式与专属 loader，现已移除。）
"""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code
from smcore.utils.format import to_float

from .name_lookup import _normalize_name


def _extract_date_from_filename(path: Path) -> Optional[str]:
    """从文件名末尾提取 YYYYMMDD，例如 Stock-Selection-PVCorr20-20260704.csv。"""
    suffix = path.stem.rsplit("-", 1)[-1]
    if len(suffix) == 8 and suffix.isdigit():
        return suffix
    return None


def _find_strategy_csv(
    pattern: str,
    date_yyyymmdd: str,
    *,
    max_stale_days: int = 3,
) -> Optional[tuple[Path, str]]:
    """按日期找策略结果 CSV，仅在 max_stale_days 内回退到最近一期。

    此前各策略独立回退到“各自最新文件”，可能把不同日期的信号混在一起，
    导致操作清单基于过期/错日数据。现在统一限制回退窗口并返回实际日期。
    """
    preferred = STOCK_DATA_DIR / f"{pattern}-{date_yyyymmdd}.csv"
    if preferred.exists():
        return preferred, date_yyyymmdd

    requested = datetime.strptime(date_yyyymmdd, "%Y%m%d").date()
    best: Optional[tuple[Path, str]] = None

    def _consider(paths: list[Path]) -> None:
        nonlocal best
        for path in paths:
            actual_date = _extract_date_from_filename(path)
            if not actual_date:
                continue
            actual = datetime.strptime(actual_date, "%Y%m%d").date()
            stale_days = (requested - actual).days
            if stale_days < 0 or stale_days > max_stale_days:
                continue
            if best is None or actual_date > best[1]:
                best = (path, actual_date)

    _consider(sorted(STOCK_DATA_DIR.glob(f"{pattern}-*.csv"), key=lambda p: p.name, reverse=True))

    archive = STOCK_DATA_DIR / "archive"
    if archive.exists():
        _consider(sorted(archive.rglob(f"{pattern}-*.csv"), key=lambda p: p.name, reverse=True))

    return best


def _load_scored_picks(
    factor: str, date_yyyymmdd: str, *, max_stale_days: int = 3
) -> tuple[dict, Optional[str]]:
    """读取「综合分」型策略的选股结果，返回 ({code: {...}}, 实际数据日期)。

    消费 `Stock-Selection-<factor>-*.csv`（列：股票代码/股票名称/综合分）。
    当前菜单全部策略（A 批 12 个因子池价格因子）均产出此契约的 CSV，
    由 fusion 按 factor_types.STRATEGY_ORDER 泛型装载。
    缺失或空表时返回空 dict（fail-soft），对融合链路是 no-op。
    """
    found = _find_strategy_csv(f"Stock-Selection-{factor}", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": _normalize_name(row.get("股票名称", "")),
                "score": to_float(row.get("综合分")),
            }
    return picks, actual_date
