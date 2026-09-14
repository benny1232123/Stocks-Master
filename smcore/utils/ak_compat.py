"""akshare 可选依赖兼容层。

akshare 在整条链路里只承担「兜底」与「离线批处理」角色（新闻抓取、宏观指标、
候选池快照、指数兜底）——云端精简模式（RENDER_LITE=1）已把对应端点全部 503，
且这些用途在 Render 上各有替代（本地 CSV 缓存 / dashboard 快照 / 已关闭），
因此云端不安装 akshare（见 requirements-cloud.txt）。

问题：项目里多处直接 `import akshare as ak`。依赖被拆走后这些位置会抛 ImportError
让端点 500，而不是走原本的 fail-soft 回退。

本模块提供统一入口 :func:`get_ak`：

* akshare 可用 → 返回真实模块，行为完全不变；
* akshare 缺失 → 返回 :data:`MISSING`（占位对象）。它的**任何属性访问都会返回一个
  「调用即得 None」的桩函数**，於是调用方原样写 `df = ak.xxx()` 会拿到 None，
  既有的 `if df is not None and not df.empty` / `if spot is None` 判断自然走回退分支
  —— 调用点无需任何改动。

用法::

    from smcore.utils.ak_compat import get_ak

    ak = get_ak()
    df = ak.macro_china_pmi()      # 缺 akshare 时得到 None，后续判空即回退
"""
from __future__ import annotations

import importlib


class _MissingCallable:
    """占位桩：可调用（返回 None），也可继续取属性（返回自身）。

    后者用于兼容链式写法 ``ak.stock.xxx()` —— 虽然 akshare 的公开 API 都是扁平函数
    （``ak.macro_china_pmi()``），不会真正链式取属性，但让占位对象在任意访问路径下
    都稳当返回 None，可避免将来新增调用点踩坑。
    """

    def __call__(self, *_args, **_kwargs):
        return None

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(name)
        return self

    def __repr__(self) -> str:
        return "<akshare MISSING 桩: 调用返回 None>"


_MISSING_CALL = _MissingCallable()


class _MissingAkshare:
    """akshare 缺失时的占位对象（见模块 docstring）。"""

    def __getattr__(self, name: str):
        # 属性访问一律返回占位桩；私有/魔法属性照常抛错，避免干扰内省
        if name.startswith("__") or name.startswith("_MissingAkshare__"):
            raise AttributeError(name)
        return _MISSING_CALL

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "<akshare MISSING: 占位对象，调用一律返回 None>"


MISSING = _MissingAkshare()

_ak = None
_resolved = False


def get_ak():
    """返回 akshare 模块；缺失时返回 :data:`MISSING` 占位对象（绝不抛 ImportError）。

    结果在进程内缓存，避免每次调用都重新探测。
    """
    global _ak, _resolved
    if not _resolved:
        try:
            _ak = importlib.import_module("akshare")
        except Exception:
            _ak = MISSING
        _resolved = True
    return _ak


def available() -> bool:
    """akshare 当前是否真实可用。"""
    return get_ak() is not MISSING
