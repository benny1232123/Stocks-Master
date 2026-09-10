"""审计代码库里的「静默异常」——``except ...: pass/continue`` 这类吞掉错误的位置。

为什么需要
──────────
``except Exception: pass`` 在数据源探测等场景是合理的 fail-soft，但在**决策路径**上
是事故温床：2026-09-09 实测发现两类真实故障都被静默吞掉——

1. ``list_kline_codes`` 读坏 parquet 直接跳过 → 股票池悄悄缩水，无报错；
2. regime 快照写入失败 → 快照停留在 6 周前，前端长期展示陈旧 regime 与等权权重。

本脚本不改代码，只输出清单，按「是否在决策路径」给出优先级，供逐个决策。

用法::

    python scripts/audit_silent_except.py                 # 全部
    python scripts/audit_silent_except.py --dir smcore/data
    python scripts/audit_silent_except.py --top 30
"""
from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# 这些目录/文件名片段属于第三方或产物，跳过
SKIP_DIRS = {"__pycache__", ".venv", "node_modules", ".git", "archive", "plots"}
SKIP_FILE_PARTS = {"_pb2", "conftest"}

# 决策路径关键字：命中则判定为高风险（静默失败会改变策略输出但不报错）
DECISION_HINTS = (
    "kline", "regime", "fusion", "weight", "risk", "selection",
    "pick", "signal", "score", "backtest", "snapshot", "guard",
)


def _is_silent(node: ast.ExceptHandler) -> bool:
    """handler 体里只有 pass / continue / 只含常量表达式，视为静默。"""
    body = [s for s in node.body if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant) and isinstance(s.value.value, str))]
    if not body:
        return True  # 只有 docstring 也算静默
    if len(body) == 1 and isinstance(body[0], (ast.Pass, ast.Continue)):
        return True
    return False


def audit(root: Path, target: Path | None = None) -> list[dict]:
    base = target or (root / "smcore")
    if base.is_file():
        files = [base]
    else:
        files = sorted(base.rglob("*.py"))

    out: list[dict] = []
    for f in files:
        if any(p in SKIP_DIRS for p in f.parts):
            continue
        if any(p in f.name for p in SKIP_FILE_PARTS):
            continue
        try:
            tree = ast.parse(f.read_text(encoding="utf-8"), filename=str(f))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            if not _is_silent(node):
                continue
            rel = f.relative_to(root).as_posix()
            out.append({
                "file": rel,
                "line": node.lineno,
                "handler": ast.unparse(node.type) if node.type else "bare",
                "decision": any(h in rel.lower() for h in DECISION_HINTS),
            })
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="审计静默异常")
    ap.add_argument("--dir", default=None, help="审计目录或文件（默认 smcore/）")
    ap.add_argument("--top", type=int, default=0, help="只显示前 N 条")
    ap.add_argument("--decision-only", action="store_true", help="只显示决策路径上的")
    args = ap.parse_args()

    target = Path(args.dir) if args.dir else None
    if target and not target.is_absolute():
        target = ROOT / target
    rows = audit(ROOT, target)

    if args.decision_only:
        rows = [r for r in rows if r["decision"]]

    hi = [r for r in rows if r["decision"]]
    lo = [r for r in rows if not r["decision"]]
    print(f"静默异常合计 {len(rows)} 处 —— 决策路径 {len(hi)} 处 / 其它 {len(lo)} 处\n")

    show = hi + lo
    if args.top:
        show = show[: args.top]

    print(f"{'风险':<4} {'位置':<62} {'捕获类型'}")
    print("-" * 100)
    for r in show:
        mark = "高" if r["decision"] else "低"
        loc = f"{r['file']}:{r['line']}"
        print(f"{mark:<4} {loc:<62} {r['handler']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
