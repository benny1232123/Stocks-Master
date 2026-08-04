"""一次性构建「股票代码 → 所属行业」映射并缓存为 stock_data/sector_map.json。

数据源：baostock query_stock_industry（证监会行业分类，本地可达、稳定）。
映射相对静态（行业分类变化慢），构建一次提交仓库即可；云端 CI 直接读缓存 JSON，
无需在线抓取（规避海外东财 push2 不可达、akshare spot 偶发 ConnectionError 的问题）。

用法：
    python scripts/build_sector_map.py          # 构建/补全全市场映射
    python scripts/build_sector_map.py --force   # 忽略已有缓存，全量重建

设计为可断点续跑：若 sector_map.json 已存在则只补全缺失代码，方便中断后继续。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# 允许以 `python -m scripts.build_sector_map` 或仓库根目录直接运行
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from smcore.config.defaults import PROJECT_ROOT  # noqa: E402
from smcore.utils.code import format_stock_code  # noqa: E402

SECTOR_MAP_PATH = PROJECT_ROOT / "stock_data" / "sector_map.json"


def _iter(rs):
    """安全地遍历 baostock ResultData（next 返回 False 即结束）。"""
    while rs.next():
        yield rs.get_row_data()


def _load_existing() -> dict:
    if SECTOR_MAP_PATH.exists():
        try:
            return json.loads(SECTOR_MAP_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _strip_code_prefix(industry: str) -> str:
    """证监会行业形如 'J66货币金融服务' → '货币金融服务'（去掉大类字母+数字前缀，便于展示）。"""
    if not industry:
        return "未知"
    s = industry.strip()
    # 去掉开头的字母+数字（如 J66 / C27）
    i = 0
    while i < len(s) and not ("\u4e00" <= s[i] <= "\u9fff"):
        i += 1
    name = s[i:].strip()
    return name or "未知"


def build(force: bool = False) -> dict:
    import baostock as bs

    existing = {} if force else _load_existing()
    out = dict(existing)

    lg = bs.login()
    if lg.error_code != "0":
        print(f"[sector_map] baostock 登录失败: {lg.error_msg}", file=sys.stderr)
        return out

    try:
        # 1) 取全市场股票列表（query_stock_basic 仅支持 code/code_name 两个参数，
        #    不接收 type；需自行按代码规则过滤 A 股：沪市 60/688、深市 000/001/002/300/301 等）
        rs = bs.query_stock_basic(code="", code_name="")
        codes: list[str] = []
        for row in _iter(rs):
            code = row[0]
            if not code or not (code.startswith("sh.") or code.startswith("sz.")):
                continue
            num = code.split(".", 1)[1]
            if len(num) != 6:
                continue
            # 排除指数（上证 000/999、深证 399、板块 880 等）：仅保留 A 股代码前缀
            if num[:3] in ("000", "999", "399", "880"):
                continue
            if num[0] not in ("6", "0", "3", "2", "8"):
                continue
            codes.append(code)
        print(f"[sector_map] 共 {len(codes)} 只 A 股待查行业（已缓存 {len(existing)}）")

        done = 0
        skipped = 0
        for code in codes:
            c6 = format_stock_code(code)
            if not c6:
                continue
            if c6 in out and not force:
                skipped += 1
                continue
            try:
                ir = bs.query_stock_industry(code=code)
                ind = "未知"
                if ir.error_code == "0":
                    while ir.next():
                        d = ir.get_row_data()
                        # d: [updateDate, code, industry, industryClassification]
                        if len(d) >= 3 and d[2]:
                            ind = _strip_code_prefix(d[2])
                            break
                out[c6] = ind
            except Exception:
                out.setdefault(c6, "未知")
            done += 1
            if done % 200 == 0:
                # 每 200 只落盘一次，断点续跑友好
                SECTOR_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
                SECTOR_MAP_PATH.write_text(
                    json.dumps(out, ensure_ascii=False, indent=0), encoding="utf-8"
                )
                print(f"[sector_map] 进度 {done}/{len(codes)} 已缓存 {len(out)}")
    finally:
        bs.logout()

    SECTOR_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    SECTOR_MAP_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=0), encoding="utf-8")
    print(f"[sector_map] 完成：缓存 {len(out)} 只行业映射到 {SECTOR_MAP_PATH}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="忽略已有缓存全量重建")
    args = ap.parse_args()
    t0 = time.time()
    out = build(force=args.force)
    import collections

    cnt = collections.Counter(out.values())
    print(f"[sector_map] 行业数 {len(cnt)}；用时 {time.time()-t0:.0f}s")
    print("[sector_map] Top10:", cnt.most_common(10))


if __name__ == "__main__":
    main()
