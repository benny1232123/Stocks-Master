"""K线数据获取 —— 单一真相源（强制前复权）。

合并自 boll-visualizer/src/core/data_fetcher.py 的 K线部分，关键改动：
- 强制前复权(qfq)：此前 Boll 选股用不复权(adjustflag=3)，
  除权除息日布林带断裂、信号失真，是"结果不可信"的头号原因。
- 统一 baostock 会话：用 core.data.session 单例，避免每只股票重复登录。
- 云端后端：环境变量 KLINE_BACKEND=akshare 时改用 akshare HTTP 接口（东财数据源），
  不依赖 baostock 登录会话，适合 GitHub Actions / SCF 等云端环境。
"""
from __future__ import annotations

import os
import sys
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, time as _time, timedelta, timezone
from pathlib import Path

import pandas as pd

from smcore.config import ADJUST_FLAG_MAP, CACHE_DIR, CSV_ENCODING, DEFAULT_ADJUST, STOCK_DATA_DIR
from smcore.utils.code import format_stock_code, to_baostock_code

# K 线缓存单独放在 stock_data/k_data/（受追踪、随仓库提交），
# 不放在 stock_data/cache/ 下（该目录被 .gitignore 整目录忽略，会导致云端每次冷启动重抓）。
K_DATA_CACHE_DIR = STOCK_DATA_DIR / "k_data"
DAILY_K_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount"]

# ── 缓存读取失败可观测性（2026-09-09）──
# 背景：此前 `except Exception: pass` 遍布关键读路径，某个 parquet 损坏时会被静默跳过，
# 表现为「股票池悄悄缩水 / 扫描结果全空」而不报错——sweep_relativity_params 曾因此
# 让 close_map 全空却无任何提示。现在统一记录失败并首次告警。
_READ_FAIL_LOCK = threading.Lock()
_READ_FAILURES: dict[str, int] = {}
_WARNED_PATHS: set[str] = set()


def _record_read_failure(path, exc: BaseException, ctx: str = "") -> None:
    """记录一次缓存读取失败；同一路径只在首次告警，避免刷屏。"""
    key = f"{ctx}:{path}" if ctx else str(path)
    with _READ_FAIL_LOCK:
        _READ_FAILURES[key] = _READ_FAILURES.get(key, 0) + 1
        first = key not in _WARNED_PATHS
        _WARNED_PATHS.add(key)
    if first:
        tail = f"（{ctx}）" if ctx else ""
        print(f"[kline] WARN: 缓存读取失败{tail} {path}: {exc!r}", file=sys.stderr)


def get_read_failures() -> dict[str, int]:
    """返回自进程启动以来各路径的缓存读取失败次数（供巡检 / 测试断言）。"""
    with _READ_FAIL_LOCK:
        return dict(_READ_FAILURES)


def clear_read_failures() -> None:
    """清空失败计数（测试用）。"""
    with _READ_FAIL_LOCK:
        _READ_FAILURES.clear()
        _WARNED_PATHS.clear()

# ── 复权基准漂移守卫 ──
# 前复权价以「最新交易日」为锚：此后一旦发生分红送转，整条历史序列都会被重新缩放。
# 因此「把新拉的几天直接拼到旧缓存后面」在物理上就是错的 —— 旧段停留在过期基准、
# 新段用新基准，接缝处出现断层（实测 600900 长江电力接缝单日 +46%，物理不可能）。
# 断层不只毒化回测成交价，更会静默毒化所有跨接缝的回看指标（MA/布林/动量/相对强度）。
# 守卫：增量拉取强制与缓存重叠若干日，比对重叠日收盘；偏差超容差即判定基准漂移，
# 丢弃缓存全量重拉。容差取 0.5%（小于最低分红率，又大于浮点/数据源舍入噪声）。
KLINE_OVERLAP_DAYS = int(os.getenv("KLINE_OVERLAP_DAYS", "7"))
KLINE_DRIFT_TOL = float(os.getenv("KLINE_DRIFT_TOL", "0.005"))

# 重叠检测只能发现「接缝处」漂移；若污染位于缓存中段（历史某次追加留下的），
# 接缝可能完全一致而中段仍是坏的。因此落盘前还要做整段自洽性检查：
# 相邻交易日跳变一旦超过该板块涨跌停上限，就是非市场行为 —— 只可能是复权错误。
# 板块涨跌停随交易所规则固定，非策略超参；留环境变量仅为便于测试与规则变更。
PRICE_LIMIT_MAIN = float(os.getenv("KLINE_LIMIT_MAIN", "10"))      # 主板 60/00/002
PRICE_LIMIT_GROWTH = float(os.getenv("KLINE_LIMIT_GROWTH", "20"))  # 创业板 300/301、科创 688
PRICE_LIMIT_BJ = float(os.getenv("KLINE_LIMIT_BJ", "30"))          # 北交所 8/4
PRICE_LIMIT_MARGIN = float(os.getenv("KLINE_LIMIT_MARGIN", "1.15"))  # 余量，吸收停复牌等边缘情形
JUMP_SKIP_HEAD_BARS = int(os.getenv("KLINE_JUMP_SKIP_HEAD", "10"))   # 新股上市初期不设涨跌幅限制

# 同花顺复权因子事件流对 qfq 守卫的交叉校验（默认开；无 Key/异常自动跳过，不改动重拉行为）
HITHINK_QFQ_CHECK = os.getenv("HITHINK_QFQ_CHECK", "1") == "1"


def price_limit_ratio(code6: str) -> float:
    """该股单日价格变动的物理上限（比值，如 1.115）。超过即非市场行为。"""
    c = str(code6)
    if c.startswith(("300", "301", "688")):
        pct = PRICE_LIMIT_GROWTH
    elif c.startswith(("8", "4")):
        pct = PRICE_LIMIT_BJ
    else:
        pct = PRICE_LIMIT_MAIN
    return 1 + pct * PRICE_LIMIT_MARGIN / 100.0


def find_price_breaks(df: pd.DataFrame, code6: str) -> list[dict]:
    """扫描收盘价序列中的复权断层，返回断层点列表（正常序列返回 []）。"""
    if df is None or len(df) < 2 or "close" not in df.columns:
        return []
    close = pd.to_numeric(df["close"], errors="coerce").reset_index(drop=True)
    dates = df["date"].reset_index(drop=True) if "date" in df.columns else close.index.to_series()
    up = price_limit_ratio(code6)
    ratio = close / close.shift(1)
    hits = ratio[(ratio > up) | (ratio < 1 / up)]
    return [
        {
            "date": str(dates.iloc[i]),
            "prev_close": round(float(close.iloc[i - 1]), 4),
            "close": round(float(close.iloc[i]), 4),
            "ratio": round(float(ratio.iloc[i]), 4),
        }
        for i in hits.index
        if i >= JUMP_SKIP_HEAD_BARS and pd.notna(ratio.iloc[i])
    ]


# ── 「平滑累积缩放」巡检（2026-09-14 新增）──
# 病灶：缓存 close 相对「真实成交价」被**逐步缩放**（缓存 close = 真值 × g(t)，g 缓慢变化），
# close 全为正、相邻日也看不出极端跳变，属于最隐蔽的一类复权错误。
# 与既有两层守卫的分工（**2026-09-14 实测校正后的结论，勿照抄旧描述**）：
#   ① _detect_adjust_drift 只比「本次请求段」的重叠交易日 → 缓存中段的历史污染完全看不见；
#   ② find_price_breaks 只看**相邻日**跳变 → 覆盖「阶跃型」污染。实测旧缓存的分段重锚
#      正是阶跃型（000001 有 13 处 >11.5% 的物理不可能跳变），这类归 ② 管，本巡检抓不到；
#   ③ 本巡检补的是**平滑漂移型**：每日只移动 ~0.2%，远小于 close 与真实均价之间的
#      日间噪声（±1~2%），② 在物理上必然漏检。实测现库 213/4358 只（4.9%）属此类，
#      全部集中在 hithink qfq 的深历史段（2015-2016），与已知「加法式失真」一致；
#      而最近 250 交易日仅 145 个跳变点（旧库 1244 个）—— 近端是干净的。
# 依据的不变量：同一行里 amount/volume 是**当日真实成交均价**（不做复权），close 是复权价。
#   于是 r = close / (amount/volume) 对健康数据只在**除权除息日**发生阶跃，
#   两次除权之间近似常数。
# 判法（两个时间尺度）：先用滚动中位数抹掉 VWAP↔close 的日间噪声，再看「窗口级」变化占比：
#   健康序列 r 呈阶梯状 → 只有极少数窗口在变（实测 ~1~15%，取决于分红频次）
#   缩放污染 r 呈平滑漂移 → 几乎每个窗口都在变（~100%）
# 两项同时成立才判命中：变化窗口占比 > SHARE_TOL **且** r 总跨度 > FOLD_TOL。
# 该判据是纯比例量：与价格量纲、成交额单位（元/千元/万元）、复权档位全部无关，
# 也与涨跌停规则、分红率无关，因此无需按板块/市值分档。
KLINE_SCALE_WIN = int(os.getenv("KLINE_SCALE_WIN", "21"))
KLINE_SCALE_CHANGE_TOL = float(os.getenv("KLINE_SCALE_CHANGE_TOL", "0.01"))
KLINE_SCALE_SHARE_TOL = float(os.getenv("KLINE_SCALE_SHARE_TOL", "0.5"))
KLINE_SCALE_FOLD_TOL = float(os.getenv("KLINE_SCALE_FOLD_TOL", "3.0"))
KLINE_SCALE_MIN_BARS = int(os.getenv("KLINE_SCALE_MIN_BARS", "120"))
# amount/volume 自身若几乎不变，说明该源没给真实成交均价，本巡检无信息量 → 不判（防误报）
KLINE_SCALE_VWAP_MIN_FOLD = float(os.getenv("KLINE_SCALE_VWAP_MIN_FOLD", "1.05"))
KLINE_SCALE_CHECK = os.getenv("KLINE_SCALE_CHECK", "1") == "1"


def detect_scale_drift(df: pd.DataFrame, code6: str = "") -> dict:
    """巡检「平滑累积缩放」失真（复权基准被逐段重锚）。

    返回度量字典，字段：
      ok       —— 样本是否足够做判定（False = 不判，宁可不判也不误报）
      flagged  —— 是否命中（True = 疑似累积缩放污染）
      n        —— 有效行数（close/amount/volume 均为正）
      r_first / r_last —— 复权因子（含污染）的端点值
      fold     —— r 的总跨度 max/min
      share    —— 窗口级变化占比（健康 ~1~15%，平滑漂移 ~100%）
      rho      —— r 随时间的秩相关系数（平滑漂移趋近 ±1）
      vwap_fold —— amount/volume 自身跨度（< VWAP_MIN_FOLD 表示源没给真实均价）
    """
    import math  # 函数级导入：保持模块顶部零新增 import（Render 精简模式内存铁律）

    res = {"ok": False, "flagged": False, "n": 0, "r_first": None, "r_last": None,
           "fold": None, "share": None, "rho": None, "vwap_fold": None}
    if df is None or len(df) < KLINE_SCALE_MIN_BARS:
        return res
    if not {"date", "close", "volume", "amount"}.issubset(df.columns):
        return res

    d = df.loc[:, ["date", "close", "volume", "amount"]].copy()
    for col in ("close", "volume", "amount"):
        d[col] = pd.to_numeric(d[col], errors="coerce")
    d = d[(d["close"] > 0) & (d["volume"] > 0) & (d["amount"] > 0)]
    d = d.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    if len(d) < KLINE_SCALE_MIN_BARS:
        return res

    vwap = (d["amount"] / d["volume"]).astype(float)  # 当日真实成交均价（未复权）
    r = (d["close"] / vwap).astype(float)             # = 复权因子 × 缩放污染
    ok_mask = r.notna() & (r > 0) & vwap.notna() & (vwap > 0)
    r, vwap = r[ok_mask], vwap[ok_mask]
    if len(r) < KLINE_SCALE_MIN_BARS:
        return res
    vwap_fold = float(vwap.max() / vwap.min())
    res["vwap_fold"] = round(vwap_fold, 3)
    if vwap_fold < KLINE_SCALE_VWAP_MIN_FOLD:
        # 成交均价几乎不动 → 该源没有真实成交均价，r 退化成 close 的常数倍，
        # 任何价格趋势都会被误判成「缩放漂移」。没有信息量就不判。
        return res

    # 抹掉 VWAP↔close 的日间噪声（滚动中位数，对尖峰稳健）
    s = r.map(math.log).rolling(
        KLINE_SCALE_WIN, min_periods=max(3, KLINE_SCALE_WIN // 4), center=True
    ).median()
    step = s.diff(KLINE_SCALE_WIN).dropna()
    if len(step) < 4:
        return res

    share = float((step.abs() > KLINE_SCALE_CHANGE_TOL).mean())
    fold = float(r.max() / r.min())
    rho = None
    try:
        ss = s.dropna()
        if len(ss) >= 8:
            val = float(ss.rank().corr(pd.Series(range(len(ss)), index=ss.index).rank()))
            rho = None if val != val else val  # NaN → None
    except Exception:
        rho = None

    res.update({
        "ok": True,
        "n": int(len(d)),
        "r_first": round(float(r.iloc[0]), 6),
        "r_last": round(float(r.iloc[-1]), 6),
        "fold": round(fold, 3),
        "share": round(share, 4),
        "rho": None if rho is None else round(rho, 4),
    })
    # 单调性只用于**抑制误报**：平滑漂移必单调（|rho|→1）；
    # rho 缺失（样本过少）时不否决，交由 share/fold 判定。
    monotone = (rho is None) or (abs(rho) >= 0.5)
    res["flagged"] = bool(
        share > KLINE_SCALE_SHARE_TOL and fold > KLINE_SCALE_FOLD_TOL and monotone
    )
    return res


def _backend() -> str:
    """返回当前 K 线后端：tdx（最快）> baostock（本地）> akshare（云端兜底）。

    优先读取 KLINE_BACKEND 环境变量（可强制 tdx/baostock/akshare）；
    未设置时自动检测：通达信可用则优先（毫秒级、直连券商、最稳），
    否则 baostock，再否则 akshare。
    """
    backend = os.getenv("KLINE_BACKEND", "").strip().lower()
    if backend in ("tdx", "baostock", "akshare", "hithink"):
        return backend
    # 自动检测优先级：tdx(本地直连,毫秒级) > hithink(官方云API,有Key且联网) > baostock > akshare
    # GitHub Actions 等海外/无终端环境 tdx 不可用，hithink 有 Key 时自动成为云端最快首选。
    # 后端探测本就是「试下一个」的流程，探测失败=该后端不可用，属正常 fallback，
    # 故此处保持静默（否则每次启动都会在无 tdx 的环境刷屏）。真正需要告警的是
    # 「所有后端都不可用」，由调用方在拿不到数据时报错。
    try:
        from smcore.data.tdx_client import available as tdx_available
        if tdx_available():
            return "tdx"
    except Exception:
        pass
    try:
        from smcore.data import hithink as _hk
        if _hk.available():
            return "hithink"
    except Exception:
        pass
    try:
        import baostock as bs  # noqa: F401
        return "baostock"
    except ImportError:
        return "akshare"


def _to_date_string(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _to_date(value) -> date:
    return pd.to_datetime(value).date()


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=DAILY_K_COLUMNS)


def _call_with_timeout(func, timeout: float):
    """在 daemon 线程中执行 func，超时（挂起）则返回 None 而非永久阻塞。

    用于包裹 akshare 等无内置超时的网络调用，保证云端流水线「不会挂」。
    """
    box: dict = {}

    def _run():
        try:
            box["r"] = func()
        except BaseException:
            box["e"] = True

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive() or "e" in box:
        return None
    return box.get("r")


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return _empty_df()
    out = df.copy()
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date")
    # 同一交易日只保留最后一条：concat(缓存, 新拉段) 时重叠日必然重复，
    # 保留后者（新拉的）才是最新复权基准。此前缺失去重会让重叠日残留两行。
    out = out.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if out.empty:
        return _empty_df()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[DAILY_K_COLUMNS]


def _bucket_prefix(code6: str, adjust: str) -> str:
    """桶文件前缀：{adjust}_b{code[:2]}（前 2 位决定物理文件）。"""
    return f"{adjust}_b{code6[:2]}"


def _bucket_files(code6: str, adjust: str, base_dir=None) -> list[Path]:
    """返回某代码 K 线可能所在的全部 parquet 路径（主文件 + 分片）。

    单文件桶（00/30/68）只有 `qfq_bXX.parquet`；
    超大桶（如 b60）被拆成 `qfq_b60_{d}.parquet` 分片、按代码第 3 位路由，
    以绕过 GitHub 单文件 100MB 硬限（GH001）。读时扫全部候选，写时只落对应分片。
    """
    base = Path(base_dir) if base_dir else K_DATA_CACHE_DIR
    prefix = _bucket_prefix(code6, adjust)
    main = base / f"{prefix}.parquet"
    parts = sorted(base.glob(f"{prefix}_*.parquet"))
    out: list[Path] = []
    if main.exists():
        out.append(main)
    out.extend(p for p in parts if p not in out)
    return out


def _write_bucket_file(code6: str, adjust: str, base_dir=None) -> Path:
    """返回某代码 K 行应写入的 parquet 文件。

    若该桶已存在分片（说明被拆过），按代码第 3 位路由到对应分片；否则单文件。
    """
    base = Path(base_dir) if base_dir else K_DATA_CACHE_DIR
    prefix = _bucket_prefix(code6, adjust)
    parts = sorted(base.glob(f"{prefix}_*.parquet"))
    if parts:
        return base / f"{prefix}_{code6[2]}.parquet"
    return base / f"{prefix}.parquet"


def read_kline_cache(code, adjust: str = DEFAULT_ADJUST, base_dir=None) -> pd.DataFrame:
    """读单只股票的全量归一化 K 线（DAILY_K_COLUMNS，date 为字符串 YYYY-MM-DD）。

    优先读分桶 parquet 数据集（按 code 谓词下推，pyarrow 行组跳过）；
    若该股票不在 parquet 中，则兜底读 legacy 每股票 CSV（迁移过渡期 / 测试）。
    """
    code6 = format_stock_code(code)
    if not code6:
        return _empty_df()
    base = Path(base_dir) if base_dir else K_DATA_CACHE_DIR
    frames: list[pd.DataFrame] = []
    for pf in _bucket_files(code6, adjust, base):
        try:
            sub = pd.read_parquet(
                pf, columns=DAILY_K_COLUMNS + ["code"], filters=[("code", "==", code6)]
            )
            if not sub.empty:
                frames.append(sub)
        except Exception as exc:
            _record_read_failure(pf, exc, ctx=f"read_kline_cache({code6})")
    legacy = base / f"{code6}_{adjust}_full.csv"
    if legacy.exists():
        try:
            lf = _normalize(pd.read_csv(legacy))
            if not lf.empty:
                lf = lf.copy()
                lf.insert(0, "code", code6)
                frames.append(lf)
        except Exception as exc:
            _record_read_failure(legacy, exc, ctx=f"read_kline_cache({code6})/legacy")
    if not frames:
        return _empty_df()
    return _normalize(pd.concat(frames, ignore_index=True))


# ══ 分桶写入缓冲（2026-09-17 修「写放大」）═══════════════════════════════════════
# 问题（实测，见 .workbuddy/_bench_kline.txt）：k_data 只有 7 个分桶（最大
# qfq_b00.parquet 91.8MB / 3.70M 行），而全宇宙 ~4380 只。write_kline_cache 每写
# **一只票**都要 read_parquet(整桶 0.64s) → concat+sort(1.45s) → to_parquet(整桶, zstd 3.24s)
# = 5.33s/只 → 4380 只 ≈ **6.5 小时**。这正是 CI「K 线缓存刷新」60min 超时的真因
# （网络取数其次：hithink 实测 0.17–1.07s/只 ≈ 36min）。
#
# 修法：把一批代码的写入按**桶**聚合，退出时每桶只重写一次（重写次数 = 桶数，
# 而非代码数）。默认行为**不变**（仍然即时写），只有 kline_write_buffer() 上下文内才
# 缓冲——以免改变既有调用方与测试的「写完即可读」语义。
_BUCKET_WRITE_LOCK = threading.Lock()
_BUCKET_PENDING: dict[Path, pd.DataFrame] = {}
_BUCKET_BUFFER_ENABLED = False


@contextmanager
def kline_write_buffer():
    """上下文内 ``write_kline_cache`` 只做内存缓冲，退出时按桶一次性 upsert 落盘。

    用法（批处理场景，如 ``scripts/prepull_klines.py`` 的每个分块）::

        with kline_write_buffer():
            for code in codes:
                fetch_daily_k(code, start, end, adjust="qfq")

    退出时**即使发生异常也会 flush**，避免已取到的数据白丢。
    """
    global _BUCKET_BUFFER_ENABLED
    with _BUCKET_WRITE_LOCK:
        _BUCKET_BUFFER_ENABLED = True
    try:
        yield
    finally:
        with _BUCKET_WRITE_LOCK:
            _BUCKET_BUFFER_ENABLED = False
        flush_kline_writes()


def flush_kline_writes() -> int:
    """把缓冲中的行按桶一次性 upsert 落盘；返回实际重写的桶数。

    每桶只 read/sort/write 一次：先剔除缓冲涉及代码的旧行，再并入新行。
    """
    with _BUCKET_WRITE_LOCK:
        pending = dict(_BUCKET_PENDING)
        _BUCKET_PENDING.clear()
    n_ok = 0
    for pf, new_rows in pending.items():
        try:
            new_rows = new_rows.drop_duplicates(subset=["code", "date"], keep="last")
            existing = pd.read_parquet(pf) if pf.exists() else None
            if existing is not None and not existing.empty:
                touched = set(new_rows["code"].astype(str))
                existing = existing[~existing["code"].astype(str).isin(touched)]
                merged = pd.concat([existing, new_rows], ignore_index=True)
            else:
                merged = new_rows
            merged = merged.sort_values(["code", "date"]).reset_index(drop=True)
            merged.to_parquet(pf, index=False, compression="zstd")
            n_ok += 1
        except Exception as exc:
            # 桶级失败必须可见：静默吞掉会让「刷新报成功但数据没变」重演（缓存陈旧事故），
            # 且覆盖度门控会因此拦下构建 —— 这是有意的 fail-loud。
            print(f"[kline] ERROR: 桶写入失败 {pf}: {exc!r}"
                  f"（该桶 {len(new_rows)} 行未落盘，覆盖度门控会拦下）", file=sys.stderr)
    return n_ok


def write_kline_cache(df: pd.DataFrame, code, adjust: str = DEFAULT_ADJUST, base_dir=None) -> None:
    """把单只股票的行 upsert 进分桶 parquet（删除该股旧行后并入新行，按 code,date 排序写回）。"""
    code6 = format_stock_code(code)
    if not code6:
        return
    base = Path(base_dir) if base_dir else K_DATA_CACHE_DIR
    base.mkdir(parents=True, exist_ok=True)
    out = _normalize(df).copy()
    if out.empty:
        return
    out.insert(0, "code", code6)
    out["date"] = out["date"].astype(str)
    # 非正价硬防线（2026-09-14）：加法式前复权（如 hithink 对高分红长历史股的深历史段）
    # 会产出 close<=0。一旦落盘会污染全库（负价、pct_change 失真、各类 min/max/Drawdown 统计错乱）。
    # 这是**不变量**（A 股停牌记为无行而非 0 价），无容差、无魔数：宁拒不写，
    # 让调用方走回退链（源阶梯）或显式处理，避免静默写入坏数据。
    _close = pd.to_numeric(out["close"], errors="coerce")
    _n_nonpos = int((_close <= 0).sum())
    if _n_nonpos:
        print(
            f"[kline] WARN: 拒绝写入 {code6}：含 {_n_nonpos}/{len(out)} 行非正收盘价"
            f"（疑似复权口径失真，如加法式前复权；请改用乘法式头寸或换源）",
            file=sys.stderr,
        )
        return
    # 平滑累积缩放巡检（2026-09-14）：非正价是「显性」失真，好抓；
    # 而「缓存 = 真值 × 平滑单调的 g(t)」是**隐性**失真 —— close 全为正、
    # 相邻日也无跳变，_detect_adjust_drift / find_price_breaks 双双漏检。
    # 这里按 amount/volume（当日真实均价）与 close 的比值做长期巡检。
    # 只告警、不拒写：该判据是统计性的（阈值见 KLINE_SCALE_* 段），宁多报不漏报，
    # 由调用方决定是否对该股全量重拉。样本不足/源无真实均价时内部自会 skip。
    if KLINE_SCALE_CHECK:
        # 守卫绝不能成为写入失败源：巡检自身异常只告警、不影响落盘。
        # 但也不静默 —— 静默失败等于守卫失效（见下方 HITHINK_QFQ_CHECK 同样处理）。
        try:
            _sd = detect_scale_drift(out, code6)
            if _sd["flagged"]:
                print(
                    f"[kline] WARN: {code6} 疑似「平滑累积缩放」失真："
                    f"close/(amount/volume) 有 {_sd['share']:.0%} 的窗口在变、"
                    f"总跨度 {_sd['fold']}x、rho={_sd['rho']}"
                    f"（r: {_sd['r_first']} → {_sd['r_last']}）"
                    f"→ close 与真实成交均价不成比例（现库实测多为 hithink qfq 深历史失真），"
                    f"建议复核或对该股全量重拉",
                    file=sys.stderr,
                )
        except Exception as exc:
            print(
                f"[kline] WARN: {code6} 平滑累积缩放巡检异常，守卫未生效（{exc!r}）",
                file=sys.stderr,
            )
    pf = _write_bucket_file(code6, adjust, base)
    # 缓冲模式（kline_write_buffer 上下文内）：只登记待写行，退出时按桶一次重写。
    with _BUCKET_WRITE_LOCK:
        _buffered = _BUCKET_BUFFER_ENABLED
        if _buffered:
            _prev = _BUCKET_PENDING.get(pf)
            _BUCKET_PENDING[pf] = out if _prev is None else pd.concat([_prev, out], ignore_index=True)
    if not _buffered:
        existing = pd.read_parquet(pf) if pf.exists() else None
        if existing is not None and not existing.empty:
            existing = existing[existing["code"] != code6]
            merged = pd.concat([existing, out], ignore_index=True)
        else:
            merged = out
        merged = merged.sort_values(["code", "date"]).reset_index(drop=True)
        # 与迁移落盘的 zstd 分桶保持一致，避免增量写入把分片重新压成 snappy 而膨胀越界（GH001 100MB 硬限）。
        merged.to_parquet(pf, index=False, compression="zstd")
    # 迁移完成后 legacy CSV 应被清掉；这里顺手删除避免双份数据分歧
    legacy = base / f"{code6}_{adjust}_full.csv"
    if legacy.exists():
        try:
            legacy.unlink()
        except Exception:
            pass


def list_kline_codes(adjust: str = DEFAULT_ADJUST, base_dir=None) -> list[str]:
    """返回数据集中出现过的全部股票代码（parquet 优先，legacy CSV 兜底）。

    ⚠️ 曾有静默失败坑：某个分桶 parquet 损坏时旧实现 `except: pass` 直接跳过，
    整个桶的代码凭空消失，表现为「股票池悄悄缩水」而无任何报错。现在失败会
    计数并告警，可通过 :func:`get_read_failures` 巡检。
    """
    base = Path(base_dir) if base_dir else K_DATA_CACHE_DIR
    codes: set[str] = set()
    failed: list[str] = []
    for pf in sorted(base.glob(f"{adjust}_b*.parquet")):
        try:
            c = pd.read_parquet(pf, columns=["code"])["code"].astype(str).unique().tolist()
            codes.update(c)
        except Exception as exc:
            failed.append(pf.name)
            _record_read_failure(pf, exc, ctx="list_kline_codes")
    for csv in base.glob(f"*_{adjust}_full.csv"):
        try:
            codes.add(csv.name.split("_")[0])
        except Exception as exc:
            failed.append(csv.name)
            _record_read_failure(csv, exc, ctx="list_kline_codes/legacy")
    if failed:
        print(
            f"[kline] WARN: list_kline_codes 有 {len(failed)} 个数据文件读取失败，"
            f"返回的股票池不完整（缺失: {', '.join(failed[:5])}"
            f"{' ...' if len(failed) > 5 else ''}）",
            file=sys.stderr,
        )
    return sorted(codes)


def _is_fresh(path: Path, max_age_hours: float) -> bool:
    if not path.exists():
        return False
    if max_age_hours <= 0:
        return True
    age = datetime.now().timestamp() - path.stat().st_mtime
    return age <= max_age_hours * 3600


# A 股时区固定 UTC+8（无夏令时）。GitHub Actions runner 与 Render 容器默认 UTC：
# 若用本地时间判定「当日 bar 是否已收盘」，15:30 截止在 UTC 下等于北京 23:30，
# 会导致整个盘后选股窗口把已收盘的当日 bar 当盘中半成品丢弃（信号静默退化为 D-1 数据）。
_CN_TZ = timezone(timedelta(hours=8))


def _now_cn() -> datetime:
    """北京时间（显式 UTC+8，不依赖运行环境本地时区）。"""
    return datetime.now(_CN_TZ)


def _today_final_cutoff() -> datetime:
    """当日 bar 视为「已收盘可落盘」的北京时间时点（15:00 收盘 + 数据源落盘缓冲）。

    可用 KLINE_TODAY_CUTOFF=HH:MM 覆盖（如数据源延迟调晚）。
    """
    try:
        hh, mm = os.getenv("KLINE_TODAY_CUTOFF", "15:30").split(":")[:2]
        t = _time(int(hh), int(mm))
    except (ValueError, TypeError):
        t = _time(15, 30)
    return datetime.combine(_now_cn().date(), t, tzinfo=_CN_TZ)


def _drop_unfinished_today(df: pd.DataFrame) -> pd.DataFrame:
    """北京时间收盘前丢弃「今天」的 bar。

    盘中数据源返回的当日实时 bar 会被当成收盘价写进缓存并毒化之后所有同日请求
    （signals/布林/收益全部基于半成品 bar）；统一丢弃保证日线指标只依赖已收盘数据。
    """
    if df is None or df.empty or "date" not in df.columns:
        return df
    now_cn = _now_cn()
    if now_cn >= _today_final_cutoff():
        return df
    dts = pd.to_datetime(df["date"], errors="coerce").dt.date
    mask = dts != now_cn.date()
    dropped = int((~mask).sum())
    if dropped:
        print(
            f"[kline] 丢弃 {dropped} 根未收盘的当日 bar（{now_cn.date()}，收盘前不落盘）",
            file=sys.stderr,
        )
        return df[mask].reset_index(drop=True)
    return df


def _detect_adjust_drift(cached: pd.DataFrame, fresh: pd.DataFrame) -> float:
    """比对缓存与新拉数据在重叠交易日上的收盘价，返回最大相对偏差。

    返回 -1.0 表示无重叠、无法判定（调用方按「不阻断」处理）。
    """
    if cached is None or cached.empty or fresh is None or fresh.empty:
        return -1.0
    new = _normalize(fresh)
    if new.empty:
        return -1.0
    merged = cached[["date", "close"]].merge(
        new[["date", "close"]], on="date", suffixes=("_old", "_new")
    )
    if merged.empty:
        return -1.0
    old_c = pd.to_numeric(merged["close_old"], errors="coerce")
    new_c = pd.to_numeric(merged["close_new"], errors="coerce")
    ok = (old_c > 0) & (new_c > 0)
    if not ok.any():
        return -1.0
    return float(((new_c[ok] - old_c[ok]).abs() / old_c[ok]).max())


def _slice(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["_dt"] = pd.to_datetime(tmp["date"], errors="coerce")
    mask = (tmp["_dt"].dt.date >= start) & (tmp["_dt"].dt.date <= end)
    tmp = tmp[mask].drop(columns=["_dt"])
    return _normalize(tmp)


# ── 落盘前的「窗口化」自洽巡检参数（2026-09-17 事故后定案）──────────────────
# 守卫②（整段断层扫描）**只扫最近 KLINE_BREAK_SCAN_DAYS 个自然日**，不再扫全历史。
# 依据（全部实测，见 .workbuddy/_breaks_census.txt / _diag_breaks.txt / _probe_repull.txt）：
#   · 全宇宙 4380 只中 3009 只（68.7%）含断层，但断层点 **660/661 落在 2015–2024 深历史**，
#     落在最近 400 日的**只有 111 只（2.5%）**；A 批因子的最长窗口是 120 交易日。
#   · 深历史断层是 **hithink 源头自带**：用 force_refresh 重拉 11 年，得到的还是同样那批
#     断层（000019 重拉前后都是 79 个）→ 守卫的「全量重拉」对它**永远无解**，
#     却会**每次刷新都触发**（4380 只里的 3009 只，每只多一轮 11 年拉取）。
#   · 而「因子真正会用到」的窗口内的断层必须继续拦——所以是**收窄**，不是关闭。
# 取 500 天（> prepull 的 lookback 400 天）留余量；置 0 可退回扫全历史（仅供排查）。
KLINE_BREAK_SCAN_DAYS = int(os.getenv("KLINE_BREAK_SCAN_DAYS", "500"))

# akshare（新浪源）**默认不参与 K 线回退链**（2026-09-17 按用户要求摘除）。
# 摘除理由：hithink 超跨度静默返空时它曾「接单」，把 akshare 的 11 年序列**整段覆盖**进
# hithink 缓存（实测 000019：2844 → 2690 行），换源后断层归零、守卫误判「自愈成功」——
# 跨源混血污染且完全不可见。需要临时排查时用 KLINE_AKSHARE_FALLBACK=1 显式打开。
KLINE_AKSHARE_FALLBACK = os.getenv("KLINE_AKSHARE_FALLBACK", "0") == "1"


def _break_scan_window(df: pd.DataFrame, end: date, days: int) -> pd.DataFrame:
    """截取用于断层扫描的尾部窗口（含窗口前一根 bar，以便算窗口首根的相邻比值）。

    days <= 0 表示不截取（扫全历史）。df 为空/无 date 列时原样返回。
    """
    if days <= 0 or df is None or df.empty or "date" not in df.columns:
        return df
    d = df.reset_index(drop=True)
    dt = pd.to_datetime(d["date"], errors="coerce")
    pos = dt[dt >= (pd.Timestamp(end) - pd.Timedelta(days=int(days)))].index
    if len(pos) == 0:
        return d.iloc[0:0]
    # 多带一根前导 bar：窗口首根的比值需要它的前收盘，否则窗口边界漏检
    return d.iloc[max(0, int(pos[0]) - 1):]


def fetch_daily_k(
    code,
    start_date,
    end_date,
    adjust: str = DEFAULT_ADJUST,
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 24.0,
    _no_retry: bool = False,
) -> pd.DataFrame:
    """获取日 K 线（默认前复权），带文件缓存与增量合并。

    Args:
        code: 股票代码（任意格式）。
        start_date / end_date: 日期（date/datetime/字符串/YYYYMMDD 均可）。
        adjust: 复权方式 qfq(默认)/hfq/bfq。强制不传 "3"（不复权）以避免信号失真。
        use_cache / force_refresh / max_cache_age_hours: 缓存控制。
    """
    code6 = format_stock_code(code)
    if not code6:
        return _empty_df()
    adjust = str(adjust).lower()
    flag = ADJUST_FLAG_MAP.get(adjust, "2")  # 兜底前复权
    request_start = _to_date(start_date)
    request_end = _to_date(end_date)
    if request_start > request_end:
        return _empty_df()

    cached = pd.DataFrame()
    if use_cache and not force_refresh:
        try:
            cached = read_kline_cache(code6, adjust)
        except Exception:
            cached = pd.DataFrame()

    cache_min, cache_max = None, None
    if not cached.empty:
        dt = pd.to_datetime(cached["date"], errors="coerce").dropna()
        if not dt.empty:
            cache_min, cache_max = dt.min().date(), dt.max().date()

    covers = bool(cache_min and cache_max and cache_min <= request_start and cache_max >= request_end)
    bucket_file = _write_bucket_file(code6, adjust)
    fresh = _is_fresh(bucket_file, max_cache_age_hours) if bucket_file.exists() else False
    # 盘后自愈：请求包含今天（北京日历）、当前已过收盘时点、但缓存桶是收盘前写的 →
    # 缓存里的当日 bar 必是盘中半成品，不允许 fresh 短路放行，落到下方增量重拉取回真实收盘 bar。
    if (
        request_end >= _now_cn().date()
        and _now_cn() >= _today_final_cutoff()
        and bucket_file.exists()
        and datetime.fromtimestamp(bucket_file.stat().st_mtime, tz=_CN_TZ) < _today_final_cutoff()
    ):
        fresh = False
    if covers and (fresh or request_end < _now_cn().date() - timedelta(days=1)):
        return _drop_unfinished_today(_slice(cached, request_start, request_end))

    segments: list[tuple[date, date]] = []
    if force_refresh or cached.empty or cache_min is None:
        segments.append((request_start, request_end))
    else:
        # 前导缺口：缓存起点之前的请求区间（极少触发，缓存通常从上市起覆盖）
        if request_start < cache_min:
            segments.append((request_start, min(request_end, cache_min - timedelta(days=1))))
        # 尾部缺口：缓存终点之后的请求区间（每个交易日新增的部分）。
        # 起点强制前移 KLINE_OVERLAP_DAYS 与缓存重叠，重叠段用于复权基准漂移检测；
        # 重叠行会在 _normalize 去重时被新数据覆盖，不会产生重复行。
        if request_end > cache_max:
            segments.append((cache_max - timedelta(days=KLINE_OVERLAP_DAYS), request_end))

    parts: list[pd.DataFrame] = []

    def _fetch_segment(seg_start: date, seg_end: date, backend: str) -> pd.DataFrame:
        """按单个后端取一段 K 线（不回退）。"""
        if backend == "tdx":
            return _fetch_via_tdx(code6, seg_start, seg_end, adjust)
        if backend == "akshare":
            return _fetch_via_akshare(code6, seg_start, seg_end, adjust)
        if backend == "hithink":
            return _fetch_via_hithink(code6, seg_start, seg_end, adjust)
        # baostock
        import baostock as bs
        from smcore.data.session import session
        with session() as ok:
            if not ok:
                return pd.DataFrame()
            rs = bs.query_history_k_data_plus(
                to_baostock_code(code6),
                "date,code,open,high,low,close,volume,amount",
                start_date=_to_date_string(seg_start),
                end_date=_to_date_string(seg_end),
                frequency="d",
                adjustflag=flag,
            )
            if rs.error_code != "0":
                return pd.DataFrame()
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            return pd.DataFrame(rows, columns=rs.fields) if rows else pd.DataFrame()

    # 后端优先级：首选 _backend()（可经 KLINE_BACKEND 强制），失败自动回退其余源。
    #
    # ⚠️ 关键（2026-08-31 修复）：旧逻辑是 `if parts or not cached.empty: break`，
    # 只要本地/仓库有缓存就会在**第一个后端**后就 break —— 即使该后端（如 hithink
    # 缺 Key）一段新数据都没取到，也会直接返回**陈旧缓存**，导致持仓日报用旧 K 线。
    # 现改为：只有当某后端把**所有缺失段**都取到（segments 为空表示缓存已覆盖全部）
    # 才接受它；否则换下一个源，全失败才退回缓存。
    preferred = _backend()
    # ⚠️ akshare 默认不在回退链上（见 KLINE_AKSHARE_FALLBACK 的说明）：它的 11 年序列曾在
    # hithink 超跨度静默返空时「接单」，整段覆盖 hithink 缓存，造成完全不可见的跨源混血污染。
    # 需要临时排查时置 KLINE_AKSHARE_FALLBACK=1。
    _chain = ["tdx", "hithink", "baostock"]
    if KLINE_AKSHARE_FALLBACK:
        _chain.insert(2, "akshare")
    fallback_chain = [preferred] + [b for b in _chain if b != preferred]
    for backend in fallback_chain:
        parts = []
        fetched_all = True
        for seg_start, seg_end in segments:
            seg_df = _fetch_segment(seg_start, seg_end, backend)
            if seg_df.empty:
                fetched_all = False
                parts = []  # 丢弃半成品，避免拼接出跨源混合数据
                break
            parts.append(seg_df)
        if fetched_all:
            break

    if cached.empty and not parts:
        return _empty_df()

    # ── 复权基准守卫 ──
    # 两层防御，确保落盘数据「单一复权基准、物理自洽」：
    #  ① 接缝漂移检测：缓存与新段在重叠交易日的收盘偏差 > 容差，说明缓存基准已过期，
    #     丢弃缓存、从最早日全量重拉（保留历史区间）。
    #  ② 整段自洽性检查（落盘前）：相邻交易日跳变超该板块涨跌停即物理不可能
    #     （只可能是复权错误）——含缓存中段的历史污染（接缝一致也抓不到）。
    #     ⚠️ 2026-09-17 起**只扫最近 KLINE_BREAK_SCAN_DAYS 天**（原为全历史），依据见该常量。
    # 注意：重拉调用传入 force_refresh=True，本守卫的 `not force_refresh` 条件使其不会重入，
    # 因此不会无限递归；重拉得到的是单一源全量数据，至少内部自洽（真有跳变则告警接受）。
    # ⚠️ 两处重拉都**先判返回值**：重拉没拿到数据时不许把已有数据丢掉 —— ① 退回单一基准的
    # 缓存切片（避免把两套基准缝在一起），② 保留已拼好的单一基准序列继续落盘。
    if parts and not cached.empty and not force_refresh:
        drift = _detect_adjust_drift(cached, pd.concat(parts, ignore_index=True))
        if drift > KLINE_DRIFT_TOL:
            healed = fetch_daily_k(
                code6,
                min(request_start, cache_min or request_start),
                request_end,
                adjust=adjust,
                use_cache=use_cache,
                force_refresh=True,
                max_cache_age_hours=max_cache_age_hours,
                _no_retry=True,
            )
            if healed is not None and not healed.empty:
                return healed
            # ⚠️ 重拉没取到数据（源不可用 / 超出单次跨度上限）时，**绝不能**把「旧基准缓存 +
            # 新基准段」的拼接结果回给调用方或落盘 —— 那正是 2026-08-09 那个 +46% 接缝事故
            # 的成因。退回**单一基准**的缓存切片（本次不落盘、不污染缓存），并明确告警。
            import warnings as _w
            _w.warn(
                f"[kline] {code6} 缓存基准漂移 {drift:.4f}（容差 {KLINE_DRIFT_TOL}），"
                f"全量重拉未取到数据 → 退回缓存切片（单一基准，本次不落盘）"
            )
            return _drop_unfinished_today(_slice(cached, request_start, request_end))

    merged = _normalize(pd.concat([cached, *parts], ignore_index=True)) if (not cached.empty or parts) else _empty_df()
    if merged.empty and not cached.empty:
        merged = _normalize(cached)

    # ② 整段自洽性检查：任何非市场跳变都触发全量重拉
    if not merged.empty:
        # ⚠️ 只扫「因子真正会用到」的尾部窗口（依据见 KLINE_BREAK_SCAN_DAYS 的实测说明）：
        # 深历史断层是源头自带、重拉无解，全历史扫描会让 68.7% 的票每次刷新都白拉一轮 11 年。
        # 窗口内的断层仍然照拦 —— 这是**收窄**，不是关闭。
        _scan = _break_scan_window(merged, request_end, KLINE_BREAK_SCAN_DAYS)
        breaks = find_price_breaks(_scan, code6) if not _scan.empty else []
        if breaks and HITHINK_QFQ_CHECK:
            # 交叉校验：断层若由真实分红/送股解释，则属「缓存基准过期需重拉」的预期事件；
            # 无法解释的断层疑似真实数据错误，单独告警（仍触发下方全量重拉，安全不变）。
            try:
                from smcore.data import hithink as _hk
                if _hk.available():
                    from smcore.data.hithink_special import classify_breaks
                    breaks = classify_breaks(code6, breaks)
                    _unexplained = [b for b in breaks if not b.get("explained_by_corporate_action")]
                    if _unexplained:
                        import warnings as _w
                        _w.warn(
                            f"[kline] {code6} {len(_unexplained)} 处复权断层无法用分红/送股解释"
                            f"（疑似真实数据错误，将触发全量重拉）；可解释={len(breaks) - len(_unexplained)}"
                        )
            except Exception as exc:
                # 复权断层校验是数据质量守卫，静默失败等于守卫失效（坏 K 线直接进策略）
                print(
                    f"[kline] WARN: {code6} 复权断层校验异常，守卫未生效（{exc!r}）",
                    file=sys.stderr,
                )
        if breaks:
            if (not force_refresh) and (not _no_retry):
                healed = fetch_daily_k(
                    code6,
                    min(request_start, cache_min or request_start),
                    request_end,
                    adjust=adjust,
                    use_cache=use_cache,
                    force_refresh=True,
                    max_cache_age_hours=max_cache_age_hours,
                    _no_retry=True,
                )
                if healed is not None and not healed.empty:
                    return healed
                # 重拉没取到数据 → **保留已经拼好的单一基准序列**继续落盘，
                # 不因「自愈失败」把已经拿到的数据一起丢掉。
                import warnings
                warnings.warn(
                    f"[kline] {code6} {len(breaks)} 处复权跳变；全量重拉未取到数据，"
                    f"保留现有序列（{len(merged)} 行，单一基准）继续落盘"
                )
            else:
                import warnings
                warnings.warn(
                    f"[kline] {code6} {len(breaks)} 处复权跳变未被自愈，已用单次全量拉取覆盖"
                    f"（可能是真实除权/停复牌导致的合法大跳变）。"
                )

    # 收盘前丢弃当日未完成 bar（含缓存里历史残留的半成品），再落盘
    merged = _drop_unfinished_today(merged)
    if use_cache and not merged.empty:
        write_kline_cache(merged, code6, adjust)
    return _slice(merged, request_start, request_end) if not merged.empty else _empty_df()


# ── 通达信后端（高速主源）──

def _fetch_via_tdx(code6: str, start: date, end: date, adjust: str) -> pd.DataFrame:
    """通过通达信直连获取 K 线（自带前复权，毫秒级）。失败返回空。"""
    try:
        from smcore.data.tdx_client import get_client
        df = get_client().get_daily_k(code6, start, end, adjust)
        if df is None or df.empty:
            return pd.DataFrame()
        return df[DAILY_K_COLUMNS]
    except Exception:
        return pd.DataFrame()


# ── 同花顺官方 Financial-API 后端（云端，需 HITHINK_FINANCE_API_KEY）──

def _fetch_via_hithink(code6: str, start: date, end: date, adjust: str) -> pd.DataFrame:
    """通过同花顺官方 Financial-API 获取历史日 K（云端，需 API Key）。

    复用 hithink.fetch_historical_k，返回 kline.py 规范列，由 _normalize 统一。
    Key 缺失或失败返回空（fail-soft，交给回退链）。
    """
    try:
        from smcore.data import hithink as _hk
        if not _hk.available():
            return pd.DataFrame()
        return _hk.fetch_historical_k(code6, start, end, adjust)
    except Exception:
        return pd.DataFrame()


# ── akshare 后端（云端用） ──

_AK_COL_MAP = {
    "日期": "date", "开盘": "open", "收盘": "close",
    "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount",
}


def _fetch_via_akshare(code6: str, start: date, end: date, adjust: str) -> pd.DataFrame:
    """通过 akshare 新浪接口获取 K 线（无需登录会话）。

    使用 stock_zh_a_daily（新浪数据源），不依赖东财接口。
    """
    try:
        import akshare as ak
    except ImportError:
        return pd.DataFrame()

    # 新浪格式 symbol：sh600519 / sz000001
    sina_symbol = ("sh" if code6.startswith(("5", "6", "9")) else "sz") + code6

    # akshare 复权参数：qfq/hfq/"" (空=不复权)
    ak_adjust = adjust if adjust in ("qfq", "hfq") else ""
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    # 云端环境 akshare 偶发挂起/瞬断：超时(30s) + 重试(2 次) 兜底，保证「不会挂」
    raw = None
    for attempt in range(2):
        raw = _call_with_timeout(
            lambda: ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=start_str,
                end_date=end_str,
                adjust=ak_adjust,
            ),
            30,
        )
        if raw is not None and not raw.empty:
            break
        if attempt < 1:
            time.sleep(1.0)

    if raw is None or raw.empty:
        return pd.DataFrame()

    # stock_zh_a_daily 返回英文列名：date, open, high, low, close, volume, amount
    out = raw.copy()
    for col in DAILY_K_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    return out[DAILY_K_COLUMNS]
