"""前端因子注册表 ↔ 后端单一真源 一致性守卫。

背景（2026-09-17）
------------------
`frontend/src/lib/factorTypes.js` 是后端 `smcore/strategy/factor_types.py` 的
**展示副本**（文件头也写着「两处必须一致」）。但策略菜单从 5 → 8 → 14 扩了两代，
前端副本一直没跟上：`quality/value/size` 与 6 个价格原子因子的来源策略在前端
全部落到 `_DEFAULT_TYPE = '其他'`，于是网站上这些票的因子标签一律渲染成灰色兜底。

这个漂移**没有任何测试发现**，因为它是「静默降级」而不是报错。同理，
`styles.css` 的 `.top-tag--<id>` 与 `App.jsx` 的 `STRAT_LABEL` 也各是一份手工副本
（`momentum` 的 top-tag 样式就丢失了很久）。

本模块把「两处必须一致」从注释里的口头约定，变成可执行断言。刻意用正则解析前端
源码（不引入 node 运行时依赖）；解析失败会显式报错，绝不会「静默通过」。
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy import factor_types as ft  # noqa: E402

FRONTEND = ROOT / "frontend" / "src"
FACTOR_TYPES_JS = FRONTEND / "lib" / "factorTypes.js"
STYLES_CSS = FRONTEND / "styles.css"
APP_JSX = FRONTEND / "App.jsx"


def _read(path: Path) -> str:
    assert path.exists(), f"缺少前端源文件：{path}"
    return path.read_text(encoding="utf-8")


def _js_object_entries(text: str, const_name: str, entry_re: str) -> dict[str, str]:
    """解析 `export const X = { k: 'v', ... }` 形式的顶层字面量。"""
    m = re.search(
        rf"export const {const_name}\s*=\s*\{{(.*?)\n\}}", text, re.S
    )
    assert m, f"未能在前端源码中定位 {const_name} 字面量（格式可能已变，请更新本测试）"
    entries = re.findall(entry_re, m.group(1), re.M)
    assert entries, f"{const_name} 解析结果为空（正则与源码格式不匹配）"
    return dict(entries)


def _parse_strategy_factor_type() -> dict[str, str]:
    text = _read(FACTOR_TYPES_JS)
    return _js_object_entries(
        text, "STRATEGY_FACTOR_TYPE", r"^\s*([A-Za-z0-9_]+)\s*:\s*'([^']*)'\s*,"
    )


def _parse_factor_type_order() -> list[str]:
    text = _read(FACTOR_TYPES_JS)
    m = re.search(r"export const FACTOR_TYPE_ORDER\s*=\s*\[(.*?)\]", text, re.S)
    assert m, "未能定位 FACTOR_TYPE_ORDER 数组"
    order = re.findall(r"'([^']*)'", m.group(1))
    assert order, "FACTOR_TYPE_ORDER 解析结果为空"
    return order


def _parse_factor_type_colors() -> set[str]:
    text = _read(FACTOR_TYPES_JS)
    m = re.search(r"export const FACTOR_TYPE_COLORS\s*=\s*\{(.*?)\n\}", text, re.S)
    assert m, "未能定位 FACTOR_TYPE_COLORS 字面量"
    keys = re.findall(r"^\s*'([^']+)'\s*:\s*\{", m.group(1), re.M)
    assert keys, "FACTOR_TYPE_COLORS 解析结果为空"
    return set(keys)


def _parse_strat_label() -> set[str]:
    text = _read(APP_JSX)
    m = re.search(r"const STRAT_LABEL\s*=\s*\{(.*?)\n\}", text, re.S)
    assert m, "未能定位 App.jsx 的 STRAT_LABEL 字面量"
    keys = re.findall(r"^\s*([A-Za-z0-9_]+)\s*:\s*'", m.group(1), re.M)
    assert keys, "STRAT_LABEL 解析结果为空"
    return set(keys)


def _parse_css_tag_classes() -> set[str]:
    css = _read(STYLES_CSS)
    return set(re.findall(r"\.top-tag--([A-Za-z0-9_]+)\s*\{", css))


# ── 后端自身一致性（前端副本比对的前提）────────────────────────────────

def test_backend_registry_self_consistent():
    ids = set(ft.STRATEGY_LABEL)
    assert ids == set(ft.STRATEGY_ORDER), "STRATEGY_LABEL 与 STRATEGY_ORDER 的 id 集合不一致"
    assert ids == set(ft.STRATEGY_FACTOR_TYPE), "STRATEGY_FACTOR_TYPE 缺策略 id"
    for sid, label in ft.STRATEGY_LABEL.items():
        assert label.lower() == sid, (
            f"STRATEGY_LABEL[{sid!r}]={label!r} 的 lower() 必须等于 id（否则归因失效）"
        )


# ── 前端副本必须与后端逐项一致 ────────────────────────────────────────

def test_frontend_strategy_factor_type_matches_backend():
    fe = _parse_strategy_factor_type()
    be = ft.STRATEGY_FACTOR_TYPE
    missing = sorted(set(be) - set(fe))
    extra = sorted(set(fe) - set(be))
    assert not missing, (
        f"前端 factorTypes.js 缺少策略（会静默渲染成「其他」）：{missing}。"
        "请同步 frontend/src/lib/factorTypes.js 的 STRATEGY_FACTOR_TYPE。"
    )
    assert not extra, f"前端登记了后端不存在的策略：{extra}"
    drifted = {k: (fe[k], be[k]) for k in be if fe.get(k) != be[k]}
    assert not drifted, (
        f"因子类型串不一致（前端→后端）：{drifted}。"
        "因子类型串是前后端共用的归并键，不一致会导致分类与统计口径分叉。"
    )


def test_frontend_factor_type_order_matches_backend():
    assert _parse_factor_type_order() == list(ft.FACTOR_TYPE_ORDER), (
        "前端 FACTOR_TYPE_ORDER 与后端 display 顺序不一致（会影响分布图/归并桶的展示次序）"
    )


def test_frontend_colors_cover_every_factor_type():
    colors = _parse_factor_type_colors()
    missing = [t for t in ft.FACTOR_TYPE_ORDER if t not in colors]
    assert not missing, f"FACTOR_TYPE_COLORS 缺配色（会落灰色兜底）：{missing}"


def test_css_has_top_tag_class_for_every_strategy():
    """`.top-tag--<id>` 缺失 = 策略标签失去配色（静默降级）。

    `momentum` 的样式此前就长期缺失，正是本用例要防的那类问题。
    """
    classes = _parse_css_tag_classes()
    missing = sorted(s for s in ft.STRATEGY_ORDER if s not in classes)
    assert not missing, (
        f"styles.css 缺少 .top-tag--<id> 规则：{missing}。"
        "新增策略时需补一条配色规则（见 styles.css「策略标签」段落）。"
    )


def test_app_jsx_strat_label_covers_every_strategy():
    labels = _parse_strat_label()
    missing = sorted(s for s in ft.STRATEGY_ORDER if s not in labels)
    assert not missing, (
        f"App.jsx 的 STRAT_LABEL 缺少策略：{missing}。"
        "调用方虽有 `|| s` 文本兜底，但会显示成原始英文 id，且 top-tag 配色无从判定。"
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
