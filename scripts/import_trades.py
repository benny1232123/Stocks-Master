"""批量导入实盘交易 / 持仓快照到 Supabase `trades` 表。

用途
----
持仓日报（`notify_holdings_analysis.py` / CI `daily-holdings.yml`）的持仓
**唯一来源**是 trade 记录（Supabase `trades` 表）。系统产出的
`Daily-Action-List-*.csv` / `Stock-Selection-*.csv` / `k_data` 是策略数据，
与个人持仓无关，不能当作持仓来源。

两种模式
--------
1. **完整模式（默认）**：需要 date/code/side/price/qty，适合有完整成交记录。
2. **快照模式 `--snapshot`**：只知道「现在持有多少股」时用。
   - 只需 `code` + `qty`（name 可选）。
   - `date` 留空 → 默认今天；`price` 留空 → 自动取最新收盘价占位。
   - ⚠️ 成本价是**占位值**，会让「持仓盈亏」页不准；
     但**每日个股分析报告不受影响**（该报告只按代码出技术面/基本面，不含成本与盈亏）。
   - 每条会自动在 notes 里打上占位标记，方便以后校正。

实现要点
--------
- **不依赖 `supabase` 包**：直接用 PostgREST（`requests`），本地 venv 缺包也能跑。
- 凭证从仓库 `.env`（SUPABASE_URL / SUPABASE_KEY）或环境变量读取。
- `--dry-run` 只校验不写入；`--replace` 先清空表再导入（默认追加）。

用法
----
    # ===== 快照模式（忘了买入时间，只知道持仓数量）=====
    python scripts/import_trades.py --snapshot --init     # 生成极简模板
    python scripts/import_trades.py --snapshot --dry-run  # 校验 + 预览自动补的价格
    python scripts/import_trades.py --snapshot            # 正式导入

    # ===== 完整模式（有成交记录）=====
    python scripts/import_trades.py --init                # 生成模板
    python scripts/import_trades.py --dry-run
    python scripts/import_trades.py
    python scripts/import_trades.py --replace             # 先清空再导入

CSV 列
------
完整模式：date,code,name,side,price,qty,fee,notes
    date  YYYY-MM-DD（必需）｜ code 6位（必需）｜ side buy/sell（必需）
    price >0（必需）｜ qty >0（必需）｜ name/fee/notes 可选

快照模式：code,qty,name,date,price,notes
    code 6位（必需）｜ qty >0（必需）｜ 其余可选（留空自动补）

退出码：0 = 成功；1 = 校验/写入失败。
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import date as _date
from pathlib import Path

import requests

# 允许以 `python scripts/import_trades.py` 直接运行：把项目根目录加入 sys.path，
# 否则 smcore 不可用（脚本的 sys.path[0] 是 scripts/）。
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from smcore.utils.code import format_stock_code
except Exception:  # pragma: no cover - 兜底：无 smcore 时也能跑
    def format_stock_code(code: str) -> str:
        digits = "".join(ch for ch in str(code) if ch.isdigit())
        return digits.zfill(6) if digits else ""

ENV_FILE = PROJECT_ROOT / ".env"

DEFAULT_CSV = PROJECT_ROOT / "stock_data" / "trades_import.csv"
SNAPSHOT_CSV = PROJECT_ROOT / "stock_data" / "holdings_snapshot.csv"

CSV_HEADER = ["date", "code", "name", "side", "price", "qty", "fee", "notes"]
TEMPLATE_ROWS = [
    ["2026-08-01", "600519", "贵州茅台", "buy", "1500.00", "100", "5.0", "示例：建仓"],
    ["2026-08-05", "000001", "平安银行", "buy", "11.50", "1000", "5.0", "示例：加仓"],
]

SNAPSHOT_HEADER = ["code", "qty", "name", "date", "price", "notes"]
SNAPSHOT_TEMPLATE_ROWS = [
    ["600519", "100", "贵州茅台", "", "", ""],
    ["000001", "1000", "平安银行", "", "", ""],
]

PLACEHOLDER_NOTE = "【快照导入·成本价/日期占位】"


# --------------------------------------------------------------------------
# .env
# --------------------------------------------------------------------------
def load_env(path: Path) -> dict[str, str]:
    """读取 .env（不覆盖已存在的环境变量）。"""
    out: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            out[key.strip()] = value.strip().strip('"').strip("'")
    return out


# --------------------------------------------------------------------------
# 价格占位：复用 smcore.holdings_snapshot（与 /admin 页面同一套逻辑）
# --------------------------------------------------------------------------
def resolve_placeholder_price(code: str) -> tuple[float | None, str]:
    """返回 (价格, 来源说明)。先本地 k_data（离线），再实时行情。"""
    from smcore.holdings_snapshot import resolve_placeholder_price as _resolve

    return _resolve(code)


# --------------------------------------------------------------------------
# CSV 读取 / 校验
# --------------------------------------------------------------------------
def normalize_side(raw: str) -> str | None:
    s = (raw or "").strip().lower()
    if s in ("buy", "b", "买入", "买"):
        return "BUY"
    if s in ("sell", "s", "卖出", "卖"):
        return "SELL"
    return None


def _read_rows(path: Path) -> tuple[list[dict], list[str]]:
    """读 CSV 为 (行dict列表, 表头)。"""
    if not path.exists():
        raise FileNotFoundError(f"找不到 CSV: {path}（可先跑 --init 生成模板）")
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        if not sample.strip():
            raise ValueError("CSV 是空文件")
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(fh, dialect=dialect)
        rows = list(reader)
        fields = [f.strip() for f in (reader.fieldnames or [])]
    if not rows:
        raise ValueError("CSV 只有表头，没有数据行")
    return rows, fields


def read_csv(path: Path, snapshot: bool = False, date_override: str = "") -> list[dict]:
    """读取并校验 CSV，返回可直接写入 DB 的行字典列表。"""
    raw_rows, fields = _read_rows(path)

    if snapshot:
        required = ["code", "qty"]
    else:
        required = ["date", "code", "side", "price", "qty"]

    missing = [c for c in required if c not in fields]
    if missing:
        expect = ",".join(SNAPSHOT_HEADER if snapshot else CSV_HEADER)
        raise ValueError(f"CSV 缺少必需列: {', '.join(missing)}（表头应为 {expect}）")

    today = (date_override or "").strip() or _date.today().isoformat()
    rows: list[dict] = []

    for idx, raw in enumerate(raw_rows, start=2):  # 行号从 2 开始（表头是 1）
        code_raw = (raw.get("code") or "").strip()
        qty_raw = (raw.get("qty") or "").strip()

        if not code_raw and not qty_raw:
            continue  # 跳过纯空行

        code = format_stock_code(code_raw)
        if not code:
            raise ValueError(f"第 {idx} 行：股票代码无效 -> {code_raw!r}")

        try:
            qty = float(qty_raw)
        except (TypeError, ValueError):
            raise ValueError(f"第 {idx} 行：qty 必须是数字 -> {qty_raw!r}")
        if qty <= 0:
            raise ValueError(f"第 {idx} 行：qty 必须 > 0 -> {qty}")

        notes = (raw.get("notes") or "").strip()
        name = (raw.get("name") or "").strip() or code

        if snapshot:
            side = "BUY"
            trade_date = (raw.get("date") or "").strip() or today
            price_raw = (raw.get("price") or "").strip()
            if price_raw:
                try:
                    price = float(price_raw)
                except (TypeError, ValueError):
                    raise ValueError(f"第 {idx} 行：price 必须是数字 -> {price_raw!r}")
                if price <= 0:
                    raise ValueError(f"第 {idx} 行：price 必须 > 0 -> {price}")
                price_src = "用户提供"
            else:
                price, price_src = resolve_placeholder_price(code)
                if price is None:
                    raise ValueError(
                        f"第 {idx} 行：无法自动获取 {code} 的价格"
                        f"（本地无 k_data 且实时行情不可用），请在 price 列手填成本价"
                    )
                notes = f"{notes} {PLACEHOLDER_NOTE}成本价按{price_src}占位".strip()
            fee = 0.0
        else:
            trade_date = (raw.get("date") or "").strip()
            if not trade_date:
                raise ValueError(f"第 {idx} 行：date 不能为空")
            side = normalize_side((raw.get("side") or "").strip())
            if side is None:
                raise ValueError(
                    f"第 {idx} 行：side 只能是 buy/sell -> {(raw.get('side') or '')!r}"
                )
            try:
                price = float((raw.get("price") or "").strip())
            except (TypeError, ValueError):
                raise ValueError(f"第 {idx} 行：price 必须是数字 -> {(raw.get('price') or '')!r}")
            if price <= 0:
                raise ValueError(f"第 {idx} 行：price 必须 > 0 -> {price}")
            try:
                fee = float((raw.get("fee") or "0").strip() or 0)
            except (TypeError, ValueError):
                fee = 0.0
            price_src = "用户提供"

        rows.append(
            {
                "trade_date": trade_date,
                "code": code,
                "name": name,
                "side": side,
                "price": price,
                "quantity": qty,
                "fee": fee,
                "notes": notes,
                "_price_src": price_src,  # 仅用于本地预览，写库前剔除
            }
        )

    return rows


def write_template(path: Path, snapshot: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = SNAPSHOT_HEADER if snapshot else CSV_HEADER
    sample = SNAPSHOT_TEMPLATE_ROWS if snapshot else TEMPLATE_ROWS
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(sample)
    print(f"[OK] 模板已生成: {path}")
    if snapshot:
        print("     只需填 code + qty（name 可选）；date / price 留空会自动补")
    print("     填好后运行：python scripts/import_trades.py"
          + (" --snapshot" if snapshot else "") + " --dry-run")


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="批量导入交易/持仓到 Supabase trades 表")
    parser.add_argument("--file", default=None, help="CSV 路径（默认按模式选择）")
    parser.add_argument("--snapshot", action="store_true", help="持仓快照模式：只需 code+qty")
    parser.add_argument("--init", action="store_true", help="生成模板 CSV 后退出")
    parser.add_argument("--dry-run", action="store_true", help="只校验，不写入 Supabase")
    parser.add_argument("--replace", action="store_true", help="导入前先清空 trades 表")
    parser.add_argument("--date", default="", help="日期默认值（快照模式用于覆盖「今天」）")
    args = parser.parse_args()

    default_csv = SNAPSHOT_CSV if args.snapshot else DEFAULT_CSV
    csv_path = Path(args.file) if args.file else default_csv

    if args.init:
        write_template(csv_path, snapshot=args.snapshot)
        return 0

    # 1) 校验 CSV
    try:
        rows = read_csv(csv_path, snapshot=args.snapshot, date_override=args.date)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[FATAL] {exc}")
        return 1

    mode = "持仓快照" if args.snapshot else "完整交易"
    print(f"[OK] CSV 校验通过（{mode}模式）: {len(rows)} 条")
    buys = sum(1 for r in rows if r["side"] == "BUY")
    print(f"     买入 {buys} 条 / 卖出 {len(rows) - buys} 条")
    for r in rows[:10]:
        src = r.get("_price_src", "")
        src_hint = f"  <- {src}" if src and src != "用户提供" else ""
        print(f"     {r['trade_date']} {r['code']} {r['name']} {r['side']} "
              f"{r['price']} x {int(r['quantity'])}{src_hint}")
    if len(rows) > 10:
        print(f"     ... 还有 {len(rows) - 10} 条")

    if args.snapshot:
        print("\n[提示] 快照模式的成本价是占位值 → 「持仓盈亏」页会不准；")
        print("       但每日个股分析报告不受影响（它只按代码出技术面/基本面）。")
        print("       以后查到真实成本价，可改 CSV 用 --replace 重新导入。")

    if args.dry_run:
        print("\n[DRY-RUN] 未写入 Supabase。确认无误后去掉 --dry-run 正式导入。")
        return 0

    # 2) 读取凭证
    env = load_env(ENV_FILE)
    url = (env.get("SUPABASE_URL") or os.getenv("SUPABASE_URL", "")).rstrip("/")
    key = env.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY", "")
    if not url or not key:
        print("[FATAL] 缺少 SUPABASE_URL / SUPABASE_KEY（检查 .env 或环境变量）")
        return 1

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    # 3) 探测表是否存在
    probe = requests.get(f"{url}/rest/v1/trades?select=id&limit=1", headers=headers, timeout=20)
    if probe.status_code != 200:
        print(f"[FATAL] trades 表不可访问 (status={probe.status_code}): {probe.text[:300]}")
        print("        请先在 Supabase SQL Editor 执行建表 SQL：")
        print("        python -c \"from smcore.storage.trades_repo import SUPABASE_SCHEMA_SQL; print(SUPABASE_SCHEMA_SQL)\"")
        return 1
    print("\n[OK] Supabase trades 表可访问")

    # 4) 可选：清空
    if args.replace:
        del_resp = requests.delete(f"{url}/rest/v1/trades?id=not.is.null", headers=headers, timeout=30)
        if del_resp.status_code not in (200, 204):
            print(f"[FATAL] 清空 trades 表失败 (status={del_resp.status_code}): {del_resp.text[:300]}")
            return 1
        print("[OK] 已清空 trades 表（--replace）")

    # 5) 批量写入（剔除本地预览字段）
    payload = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    batch_size = 200
    inserted = 0
    for i in range(0, len(payload), batch_size):
        batch = payload[i : i + batch_size]
        resp = requests.post(f"{url}/rest/v1/trades", headers=headers, json=batch, timeout=60)
        if resp.status_code not in (200, 201, 204):
            print(f"[FATAL] 写入失败 (status={resp.status_code}): {resp.text[:300]}")
            print(f"       已成功写入 {inserted} 条，请检查后重跑（可用 --replace 重来）")
            return 1
        inserted += len(batch)

    print(f"[OK] 已写入 {inserted} 条到 Supabase trades 表")

    # 6) 回读确认
    cnt_headers = dict(headers)
    cnt_headers["Prefer"] = "count=exact"
    cnt_headers["Range"] = "0-0"
    chk = requests.get(f"{url}/rest/v1/trades?select=*", headers=cnt_headers, timeout=20)
    print(f"[OK] 回读确认: trades 表现有 {chk.headers.get('Content-Range', '(unknown)')}")
    print("\n下一步：CI 下次运行（工作日 18:30）即可读到持仓并推送真实个股分析。")
    print("       本地验证：python scripts/notify_holdings_analysis.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
