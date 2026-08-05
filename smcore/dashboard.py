"""Dashboard data helpers shared by the API and cache prewarm script."""
from __future__ import annotations

import concurrent.futures
import os
import pickle
import threading
import time

import requests
import json as _json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "stock_data" / "daily_cache"

INDEX_MAP = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
    "沪深300": "sh000300",
}


def configure_runtime() -> None:
    """Apply the runtime defaults needed by the data layer.

    K 线默认优先通达信（最快最稳）；CI/云端若无 pytdx 或不可达，kline 的
    回退链会自动切到 akshare，无需手动配置。
    """
    os.environ.setdefault("KLINE_BACKEND", "tdx")


# 看板数据拉取超时（秒）。超时即视为失败并跳过该数据源，避免单接口卡死拖垮预热。
# 默认 60s：stock_zh_a_spot 拉全市场实时快照较大，CI 网络慢时需更长时间。
DASHBOARD_API_TIMEOUT = float(os.getenv("DASHBOARD_API_TIMEOUT", "60"))


def _call_with_timeout(func, timeout_seconds):
    """单任务超时包装：daemon 线程执行，超时抛 TimeoutError。非并发，不增加接口压力；daemon 线程超时后不阻塞进程退出。"""
    box: dict[str, Any] = {}

    def _run() -> None:
        try:
            box["result"] = func()
        except BaseException as err:  # noqa: BLE001 - 透传异常到主线程
            box["error"] = err

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout=timeout_seconds)
    if worker.is_alive():
        raise concurrent.futures.TimeoutError(f"调用超时（>{timeout_seconds}s）")
    if "error" in box:
        raise box["error"]
    return box.get("result")


def _call_with_retry(func, timeout_seconds, retries=2, backoff=3.0):
    """超时调用 + 重试：瞬断网络/超时错误自动重试，避免一次失败就放弃数据源。"""
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return _call_with_timeout(func, timeout_seconds)
        except Exception as exc:  # noqa: BLE001 - 透传异常，重试后由调用方决定
            last_exc = exc
            if attempt < retries:
                print(f"[dashboard] 调用失败，{backoff}s 后重试 ({attempt + 1}/{retries}): {exc}")
                time.sleep(backoff)
                continue
    raise last_exc


def _safe_fetch(func, timeout_seconds, label, default, retries=2):
    """超时 + 重试 + 容错：重试耗尽仍失败才返回 default。"""
    try:
        return _call_with_retry(func, timeout_seconds, retries=retries)
    except Exception as exc:
        print(f"[dashboard] {label} 获取失败（已跳过）: {exc}")
        return default


def _load_cache(key: str) -> Any:
    """Load a dated cache file if it exists."""
    today = date.today().strftime("%Y-%m-%d")
    path = CACHE_DIR / f"{key}_{today}.pkl"
    if not path.exists():
        return None
    try:
        with open(path, "rb") as file_handle:
            return pickle.load(file_handle)
    except Exception:
        return None


def fetch_index_snapshot() -> pd.DataFrame:
    """Fetch the latest index snapshot.

    优先通达信直连（毫秒级、稳），失败回退新浪 HTTP 源。
    """
    # 通达信优先
    try:
        from smcore.data.tdx_client import available as tdx_available, get_client
        if tdx_available():
            cli = get_client()
            snap = _call_with_timeout(lambda: cli.get_index_snapshot(INDEX_MAP), 20)
            if snap:
                return pd.DataFrame(snap)
    except Exception as exc:
        print(f"[dashboard] 指数快照 Tdx 失败，回退新浪: {exc}")

    # 回退：新浪 HTTP
    from smcore.data.quote_sina import fetch_sina_index_quotes

    try:
        quotes = fetch_sina_index_quotes(INDEX_MAP.values())
    except Exception as exc:
        print(f"[dashboard] 指数快照获取失败（已跳过）: {exc}")
        return pd.DataFrame()
    if not quotes:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for name, code in INDEX_MAP.items():
        code6 = code[2:]
        info = quotes.get(code6)
        if info and info.get("price") is not None:
            price = float(info["price"])
            pre_close = info.get("pre_close")
            change_pct = ((price - pre_close) / pre_close * 100) if pre_close else 0.0
            change_amt = (price - pre_close) if pre_close else 0.0
            rows.append(
                {
                    "指数": name,
                    "最新价": price,
                    "涨跌幅": change_pct,
                    "涨跌额": change_amt,
                }
            )
    return pd.DataFrame(rows)


# ── 东方财富轻量计数接口 ───────────────────────────────────────
# 不拉全量快照，按涨跌幅过滤后读取 data.total（单次仅返回计数，
# payload 极小）。注意：push2/82.push2 域名在 GitHub Actions / Render
# 等海外环境可能不可达，因此仅作为"国内友好"源；海外主源见下方腾讯接口。
_EM_FS_ALL = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
_EM_HOSTS = [
    "https://82.push2.eastmoney.com/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
]
_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/",
}


def _em_breadth_count(fs: str) -> int | None:
    """单次请求东财 clist，按过滤条件读取 data.total（仅计数）。"""
    params = {
        "pn": 1,
        "pz": 1,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": fs,
        "fields": "f12,f14",
        "_": int(time.time() * 1000),
    }
    last_err: Exception | None = None
    for host in _EM_HOSTS:
        try:
            r = requests.get(host, params=params, timeout=8, headers=_EM_HEADERS)
            j = r.json()
            data = j.get("data")
            if isinstance(data, dict) and data.get("total") is not None:
                return int(data["total"])
        except Exception as exc:  # noqa: BLE001 - 换 host 重试
            last_err = exc
            continue
    if last_err:
        raise last_err
    return None


def _fetch_breadth_eastmoney_count() -> dict[str, Any] | None:
    """东财计数接口（4 次 tiny 请求），国内可达时最快最准。"""
    up = _em_breadth_count(_EM_FS_ALL + "+f3>0")
    dn = _em_breadth_count(_EM_FS_ALL + "+f3<0")
    fl = _em_breadth_count(_EM_FS_ALL + "+f3=0")
    tot = _em_breadth_count(_EM_FS_ALL)
    if not tot:
        return None
    return {
        "上涨": int(up or 0),
        "下跌": int(dn or 0),
        "平盘": int(fl or 0),
        "总数": int(tot),
        "上涨比例": round((up or 0) / tot * 100, 1),
    }


# ── 腾讯行情接口（海外友好主源）───────────────────────────────
# qt.gtimg.cn 在海外（GitHub Actions / Render）实测可达（~101ms）。
# 策略：取沪深核心成分股（中证100/上证50/深证成指成分）样本，
# 统计样本内涨跌比例作为市场宽度估计值。不拉全量，payload 小。
_TX_HOST = "http://qt.gtimg.cn"
_TX_SAMPLE_CODES = [
    # 上证50权重股（金融/消费/能源/科技）
    "sh600519","sh601318","sh600036","sh601398","sh601988","sh600900",
    "sh601012","sh601668","sh600276","sh601888","sh600887","sh601088",
    "sh600048","sh601628","sh600585","sh603259","sh601985","sh603160",
    # 深证成指权重股
    "sz000858","sz000333","sz002594","sz300750","sz002475","sz300059",
    "sz300142","sz002714","sz002230","sz300124","sz002415","sz300274",
    # 创业板权重
    "sz300760","sz300122","sz300003","sz300014","sz300033","sz300002",
    # 科创50权重
    "sh688981","sh688256","sh688005","sh688012","sh688111",
]

# 腾讯返回字段说明（`~` 分隔）：
#  0=未知 1=名称 2=代码 3=当前价 4=昨收 5=开盘价 6=交易量 ...
# 31=涨跌额 32=涨跌幅(%) 33=换手率 34=PE ... 45=最高 46=最低 47=振幅
# 32号字段是涨跌幅%，直接可用
_TX_CHG_FIELD_IDX = 32


def _fetch_breadth_tencent_sample() -> dict[str, Any] | None:
    """腾讯行情采样估计市场宽度（海外友好）。

    请求 ~40 只核心权重股的实时行情，统计涨跌数量和比例，
    作为全市场的近似估计。腾讯 qt.gtimg.cn 海外可达、响应快。
    """
    codes_str = ",".join(_TX_SAMPLE_CODES)
    url = f"{_TX_HOST}/q={codes_str}"
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        r.encoding = "gbk"
        text = r.text.strip()
        if not text:
            return None
    except Exception as exc:
        print(f"[dashboard] 腾讯行情请求失败: {exc}")
        return None

    up = dn = fl = total = 0
    for line in text.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        try:
            # 格式: v_sh600519="1~贵州茅台~600519~1199.30~...~0.88~..."
            parts = line.split("=", 1)
            if len(parts) < 2:
                continue
            raw = parts[1].strip()
            # 去掉首尾引号（可能有单/双引号或无引号）
            if len(raw) >= 2 and raw[0] == '"':
                raw = raw[1:]
            if len(raw) >= 1 and raw[-1] == '"':
                raw = raw[:-1]
            if not raw:
                continue
            fields = raw.split("~")
            if len(fields) < _TX_CHG_FIELD_IDX + 1:
                continue
            chg_str = fields[_TX_CHG_FIELD_IDX].strip()
            if not chg_str or chg_str == "0.00":
                chg = 0.0
            else:
                chg = float(chg_str)
            total += 1
            if chg > 0:
                up += 1
            elif chg < 0:
                dn += 1
            else:
                fl += 1
        except (ValueError, IndexError):
            continue

    if total == 0:
        return None
    # 按样本比例推算全市场（A 股约 ~5000 只）
    estimated_total = 5000
    ratio = estimated_total / total
    return {
        "上涨": int(up * ratio),
        "下跌": int(dn * ratio),
        "平盘": int(fl * ratio),
        "总数": estimated_total,
        "上涨比例": round(up / total * 100, 1),
        "_source": f"tencent_sample_{total}stocks",
    }


def fetch_market_breadth() -> dict[str, Any] | None:
    """Fetch the market breadth snapshot (up/down counts across A-shares).

    数据链路（按优先级）：
      1) 通达信直连（仅本机/国内可达，毫秒级全市场，最准）
      2) 腾讯行情采样（~40 只权重股，qt.gtimg.cn 海外可达，快速估计）
      3) 东财计数接口（4 次 tiny 请求，国内精确，海外可能不可达）
      4) 全量快照兜底（akshare 东财/新浪，重，仅最后手段）
    任意一层拿到数据即返回，全失败返回 None（前端显示「暂无」）。
    """
    # 1) 通达信优先（直连券商行情服务器，毫秒级；云端无 pytdx 自动跳过）
    try:
        from smcore.data.tdx_client import available as tdx_available, get_client
        if tdx_available():
            cli = get_client()
            b = _call_with_timeout(cli.get_market_breadth, 30)
            if b and b.get("总数"):
                return b
    except Exception as exc:
        print(f"[dashboard] 市场宽度 Tdx 失败，回退: {exc}")

    # 2) 腾讯行情采样（海外友好主源：qt.gtimg.cn 可达，~40 权重股估算）
    b = _safe_fetch(_fetch_breadth_tencent_sample, 12, "市场宽度(腾讯采样)", None, retries=2)
    if b:
        return b

    # 3) 东财计数接口（国内精确；海外 push2 域名可能不可达）
    b = _safe_fetch(_fetch_breadth_eastmoney_count, 15, "市场宽度(东财计数)", None, retries=1)
    if b:
        return b

    # 4) 全量快照兜底（重，海外可能超时）
    import akshare as ak

    for name, fn in (
        ("东方财富", lambda: ak.stock_zh_a_spot_em()),
        ("新浪", lambda: ak.stock_zh_a_spot()),
    ):
        df = _safe_fetch(fn, 45, f"市场宽度({name})", None, retries=1)
        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
            chg_col = "涨跌幅" if "涨跌幅" in df.columns else None
            if chg_col is None:
                continue
            up = (df[chg_col] > 0).sum()
            dn = (df[chg_col] < 0).sum()
            fl = (df[chg_col] == 0).sum()
            tot = len(df)
            return {
                "上涨": int(up),
                "下跌": int(dn),
                "平盘": int(fl),
                "总数": int(tot),
                "上涨比例": round(up / tot * 100, 1) if tot else 0,
            }
    return None


# ── 宏观快照：持久化「最近一次真实值」缓存 ──────────────────
# 部署端（Render）能稳定访问 akshare / 外汇 API，实时拉取；本缓存保证：
#   1) 实时拉取成功 → 存真实值 + 标注「实时」
#   2) 实时拉取失败 → 复用上次真实值 + 标注「缓存(日期)」（不再显示写死假常量）
#   3) 从未成功过 → 才用 SEED 种子常量（标注「静态预估」），正常不应出现
MACRO_TTL_SECONDS = float(os.getenv("MACRO_TTL_SECONDS", "86400"))  # 24 小时（日更）
LAST_GOOD_PATH = CACHE_DIR / "macro_last_good.json"
# 绝对最后的种子常量（仅当从未成功联网取数时使用）——保证每个指标永远有值显示
_SEED_MACRO = {
    "美元/人民币": 6.77, "欧元/人民币": 7.79, "日元/人民币": 0.043, "港币/人民币": 0.92,
    "Shibor隔夜": 1.36, "Shibor_1周": 1.38, "Shibor_1月": 1.42,
    "LPR_1年": 3.35, "LPR_5年": 3.95,
    "10Y国债收益率": 2.18,
    "制造业PMI": 49.3, "CPI同比": 0.3, "PPI同比": -1.2,
}


def _load_last_good() -> dict:
    if LAST_GOOD_PATH.exists():
        try:
            return _json.loads(LAST_GOOD_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_last_good(data: dict) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        LAST_GOOD_PATH.write_text(_json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _ingest(key, live_fn, last_good, sane=None):
    """尝试实时取值；失败→上次真实值(标注缓存)；再失败→种子常量。
    返回 (value, src, stale, date_str)。"""
    now = datetime.now()
    val = None
    dt = None
    try:
        res = live_fn()
        if res:
            v, d = res
            if v is not None and (sane is None or sane[0] < float(v) < sane[1]):
                val, dt = float(v), d
    except Exception as exc:
        print(f"[dashboard] {key} 实时获取失败: {exc}")
    if val is not None:
        last_good[key] = {"value": val, "ts": now.isoformat(timespec="seconds"),
                          "date": dt or now.strftime("%Y-%m-%d")}
        return val, "实时", False, dt
    lg = last_good.get(key)
    if lg and lg.get("value") is not None:
        return lg["value"], "缓存 " + str(lg.get("date") or str(lg.get("ts", ""))[:10]), True, lg.get("date")
    seed = _SEED_MACRO.get(key)
    if seed is not None:
        return seed, "静态预估", True, None
    return None, None, True, None


def _live_fx_rates():
    """免费外汇 API（open.er-api.com，无需 key，海外可达）。base=CNY。"""
    r = requests.get("https://open.er-api.com/v6/latest/CNY", timeout=10,
                     headers={"User-Agent": "Mozilla/5.0"})
    d = r.json()
    rates = d.get("rates") or {}
    if not rates:
        return None
    out = {}
    for sym in ("USD", "EUR", "JPY", "HKD"):
        rv = rates.get(sym)
        if rv:
            out[sym] = 1.0 / rv  # 1 外币 = ? 人民币
    return out or None


def _live_lpr():
    import akshare as ak
    df = ak.macro_china_lpr()
    if df is None or df.empty:
        return None
    last = df.iloc[-1]
    return {"LPR1Y": last.get("LPR1Y"), "LPR5Y": last.get("LPR5Y"),
            "date": str(last.get("TRADE_DATE")) if last.get("TRADE_DATE") is not None else None}


def _live_bond_10y():
    import akshare as ak
    end = date.today()
    start = end - timedelta(days=330)  # 窗口必须 < 1 年
    df = ak.bond_china_yield(start_date=start.strftime("%Y%m%d"),
                             end_date=end.strftime("%Y%m%d"))
    if df is None or df.empty or "10年" not in df.columns:
        return None
    row = df.iloc[-1]
    v = row.get("10年")
    if v is None or pd.isna(v):
        return None
    dt = row.get("日期")
    return (float(v), str(dt) if dt is not None else None)


def _live_pmi():
    import akshare as ak
    # 主源：年度（jin10）
    try:
        df = ak.macro_china_pmi_yearly()
        if df is not None and not df.empty:
            last = df.iloc[-1]
            for col in ("制造业PMI", "PMI", "值", "数值", "制造业-指数"):
                if col in last and pd.notna(last[col]):
                    v = float(last[col])
                    if 30 < v < 70:
                        dt = last.get("月份") or last.get("date") or last.get("时间")
                        return (v, str(dt) if dt is not None else None)
    except Exception:
        pass
    # 备源：月度
    try:
        df = ak.macro_china_pmi()
        if df is not None and not df.empty:
            last = df.iloc[-1]
            for col in ("制造业PMI", "PMI", "值", "数值", "制造业-指数"):
                if col in last and pd.notna(last[col]):
                    v = float(last[col])
                    if 30 < v < 70:
                        dt = last.get("月份") or last.get("date") or last.get("时间")
                        return (v, str(dt) if dt is not None else None)
    except Exception:
        pass
    return None


def _live_cpi():
    import akshare as ak
    # 主源：年度
    try:
        df = ak.macro_china_cpi_yearly()
        if df is not None and not df.empty:
            last = df.iloc[-1]
            for col in ("全国CPI_当月同比", "CPI", "CPI年率", "居民消费价格指数_当月同比", "同比增长"):
                if col in last and pd.notna(last[col]):
                    v = float(last[col])
                    if -10 < v < 20:
                        dt = last.get("月份") or last.get("date")
                        return (v, str(dt) if dt is not None else None)
    except Exception:
        pass
    # 备源：月度
    try:
        df = ak.macro_china_cpi_monthly()
        if df is not None and not df.empty:
            last = df.iloc[-1]
            for col in ("全国CPI_当月同比", "CPI", "CPI年率", "居民消费价格指数_当月同比", "同比增长"):
                if col in last and pd.notna(last[col]):
                    v = float(last[col])
                    if -10 < v < 20:
                        dt = last.get("月份") or last.get("date")
                        return (v, str(dt) if dt is not None else None)
    except Exception:
        pass
    return None


def _live_ppi():
    import akshare as ak
    # 主源：年度
    try:
        df = ak.macro_china_ppi_yearly()
        if df is not None and not df.empty:
            last = df.iloc[-1]
            for col in ("全国PPI_当月同比", "PPI", "PPI年率", "工业生产者出厂价格指数_当月同比", "同比增长"):
                if col in last and pd.notna(last[col]):
                    v = float(last[col])
                    if -20 < v < 20:
                        dt = last.get("月份") or last.get("date")
                        return (v, str(dt) if dt is not None else None)
    except Exception:
        pass
    # 备源：月度
    try:
        df = ak.macro_china_ppi_monthly()
        if df is not None and not df.empty:
            last = df.iloc[-1]
            for col in ("全国PPI_当月同比", "PPI", "PPI年率", "工业生产者出厂价格指数_当月同比", "同比增长"):
                if col in last and pd.notna(last[col]):
                    v = float(last[col])
                    if -20 < v < 20:
                        dt = last.get("月份") or last.get("date")
                        return (v, str(dt) if dt is not None else None)
    except Exception:
        pass
    return None


def fetch_macro_snapshot() -> dict[str, Any] | None:
    """Fetch a comprehensive macro snapshot for the dashboard.

    指标体系（全部 fail-soft，单源失败不影响其他）：
      - 汇率：美元/欧元/日元/港币 vs 人民币（open.er-api.com，海外可达）
      - 利率：SHIBOR O/N + 1W + 1M（akshare）；LPR 1Y + 5Y（akshare macro_china_lpr）
      - 债券：10Y 国债收益率（akshare bond_china_yield，窗口 <1 年）
      - 景气：制造业 PMI（akshare macro_china_pmi_yearly，jin10）
      - 物价：CPI 同比、PPI 同比（akshare *_yearly，jin10）

    每个指标附带 *_src 字段标注来源（实时 / 缓存(日期) / 静态预估），
    并写入 *_stale=True 标记非实时；顶部 _data_status 汇总实时/缓存/静态项数。
    通过持久化 macro_last_good.json，实时拉取失败时复用上次真实值，
    而非显示写死的假常量——看板因此持续动态更新。
    """
    try:
        return _fetch_macro_snapshot_inner()
    except Exception as exc:
        print(f"[dashboard] fetch_macro_snapshot 异常，回退纯种子: {exc}")
        # 最终安全网：确保永远不返回 None 或含空 key 的 dict
        return _seed_only_snapshot()


def _seed_only_snapshot() -> dict[str, Any]:
    """所有实时源都失败时的纯种子兜底。"""
    result: dict[str, Any] = {"_generated_at": date.today().isoformat(), "_data_status": {"实时": 0, "缓存": 0, "静态": 14}}
    for k, v in _SEED_MACRO.items():
        result[k] = v
        result[k + "_src"] = "静态预估"
        result[k + "_stale"] = True
    # 汇率 inverted
    if "日元/人民币" in result and result["日元/人民币"] is not None:
        result["日元/人民币_inverted"] = round(1.0 / float(result["日元/人民币"]), 3)
    return result


def _fetch_macro_snapshot_inner() -> dict[str, Any]:
    last_good = _load_last_good()
    result: dict[str, Any] = {"_generated_at": date.today().isoformat()}
    status = {"实时": 0, "缓存": 0, "静态": 0}

    def place(key, value, src, stale, dt):
        if value is None:
            return
        result[key] = round(float(value), 4) if isinstance(value, float) else value
        result[key + "_src"] = src
        if stale:
            result[key + "_stale"] = True
        if src.startswith("实时"):
            status["实时"] += 1
        elif src.startswith("缓存"):
            status["缓存"] += 1
        else:
            status["静态"] += 1
        if dt and key in ("制造业PMI", "CPI同比", "PPI同比", "LPR_1年", "LPR_5年"):
            result.setdefault(key + "_date", dt)

    # ── 汇率（open.er-api.com，base=CNY）──────────────
    fx_map = {
        "美元/人民币": ("USD", (5, 8)),
        "欧元/人民币": ("EUR", (5, 12)),
        "日元/人民币": ("JPY", (0.02, 0.1)),
        "港币/人民币": ("HKD", (0.5, 1.5)),
    }
    try:
        fx = _live_fx_rates()
    except Exception as exc:
        print(f"[dashboard] FX 实时获取失败: {exc}")
        fx = None
    for key, (sym, sane) in fx_map.items():
        rv = fx.get(sym) if fx else None
        if rv:
            v, s, st, _ = _ingest(key, lambda r=rv: (r, None), last_good, sane)
        else:
            v, s, st, _ = _ingest(key, lambda: None, last_good, sane)
        place(key, v, s, st, None)
        if key == "日元/人民币" and v:
            result["日元/人民币_inverted"] = round(1.0 / float(v), 3)

    # ── SHIBOR 利率（akshare，海外可达）────────────────
    try:
        shibor = _fetch_shibor_multi()
    except Exception as exc:
        print(f"[dashboard] SHIBOR 获取异常: {exc}")
        shibor = None
    for k, sane in (("Shibor隔夜", (0, 6)), ("Shibor_1周", (0, 6)), ("Shibor_1月", (0, 6))):
        if shibor and k in shibor and shibor[k] is not None:
            v = float(shibor[k])
            if sane[0] < v < sane[1]:
                last_good[k] = {"value": v, "ts": datetime.now().isoformat(timespec="seconds"),
                               "date": date.today().isoformat()}
                place(k, v, "实时", False, None)
                continue
        v, s, st, _ = _ingest(k, lambda: None, last_good, sane)
        place(k, v, s, st, None)

    # ── LPR（贷款市场报价利率，每月 20 日发布）──────────
    try:
        lpr = _live_lpr()
    except Exception as exc:
        print(f"[dashboard] LPR 实时获取失败: {exc}")
        lpr = None
    if lpr and lpr.get("LPR1Y") is not None:
        for key, field in (("LPR_1年", "LPR1Y"), ("LPR_5年", "LPR5Y")):
            v, s, st, _ = _ingest(key, lambda f=field: (lpr.get(f), lpr.get("date")), last_good, (2, 6))
            place(key, v, s, st, lpr.get("date"))
    else:
        for key in ("LPR_1年", "LPR_5年"):
            v, s, st, _ = _ingest(key, lambda: None, last_good, (2, 6))
            place(key, v, s, st, None)

    # ── 10Y 国债收益率 ───────────────────────────────
    v, s, st, dt = _ingest("10Y国债收益率", _live_bond_10y, last_good, (0, 10))
    place("10Y国债收益率", v, s, st, dt)

    # ── PMI / CPI / PPI ──────────────────────────────
    v, s, st, dt = _ingest("制造业PMI", _live_pmi, last_good, (30, 70))
    place("制造业PMI", v, s, st, dt)
    v, s, st, dt = _ingest("CPI同比", _live_cpi, last_good, (-10, 20))
    place("CPI同比", v, s, st, dt)
    v, s, st, dt = _ingest("PPI同比", _live_ppi, last_good, (-20, 20))
    place("PPI同比", v, s, st, dt)

    result["_data_status"] = status
    _save_last_good(last_good)
    return result or None


def _macro_cache_path() -> Path:
    today = date.today().strftime("%Y-%m-%d")
    return CACHE_DIR / f"macro_snapshot_{today}.pkl"


def _macro_needs_refresh() -> bool:
    p = _macro_cache_path()
    if not p.exists():
        return True
    age = datetime.now().timestamp() - p.stat().st_mtime
    return age > MACRO_TTL_SECONDS


def _refresh_macro_cache() -> None:
    try:
        snap = fetch_macro_snapshot()
        if snap:
            save_cache("macro_snapshot", snap)
    except Exception as exc:
        print(f"[dashboard] 宏观缓存刷新失败: {exc}")


def maybe_refresh_macro_cache() -> None:
    """宏观缓存过期/缺失时后台刷新，保证看板数据持续动态更新（不阻塞请求）。"""
    if _macro_needs_refresh():
        threading.Thread(target=_refresh_macro_cache, daemon=True).start()


def _fetch_shibor_multi() -> dict[str, float] | None:
    """获取多条 SHIBOR 期限利率，返回 {SHIBOR_O/N: x.x, SHIBOR_1W: x.x, ...}。

    数据源优先级：
      1) macro_china_shibor_all（全期限，最快）
      2) 各期限单独 rate_interbank（备源）
      3) 昨日缓存兜底（保证不返回空）
    """
    import akshare as ak
    out: dict[str, float] = {}

    # --- 主源：macro_china_shibor_all ---
    try:
        df = _call_with_retry(
            lambda: ak.macro_china_shibor_all(),
            DASHBOARD_API_TIMEOUT,
        )
        if df is not None and not df.empty:
            mapping = {
                "O/N定价": "Shibor隔夜",
                "O/N-定价": "Shibor隔夜",
                "1W定价": "Shibor_1周",
                "1W-定价": "Shibor_1周",
                "1M定价": "Shibor_1月",
                "1M-定价": "Shibor_1月",
            }
            for src_col, out_key in mapping.items():
                if src_col in df.columns:
                    val = float(df.iloc[-1][src_col]) if pd.notna(df.iloc[-1][src_col]) else None
                    if val and val > 0:
                        out[out_key] = val
            if len(out) >= 2:
                return out
    except Exception as exc:
        print(f"[dashboard] SHIBOR multi 源1失败: {exc}")

    # --- 备源：逐期限 rate_interbank ---
    for period, key in [("隔夜", "Shibor隔夜"), ("1周", "Shibor_1周"), ("1月", "Shibor_1月")]:
        if key in out:
            continue
        try:
            df = _call_with_retry(
                lambda p=period: ak.rate_interbank(
                    market="上海银行间同业拆放利率", symbol="Shibor", indicator=p,
                ),
                DASHBOARD_API_TIMEOUT,
            )
            if df is not None and not df.empty and "利率" in df.columns:
                val = float(df.iloc[-1]["利率"])
                if val > 0:
                    out[key] = val
        except Exception:
            pass

    # --- 兜底：昨日缓存 ---
    if len(out) == 0:
        single = _fetch_shibor_overnight()
        if single is not None:
            out["Shibor隔夜"] = single

    return out if out else None


def _fetch_shibor_overnight() -> float | None:
    """获取 SHIBOR 隔夜利率，多源回退 + 昨日缓存兜底。

    数据源优先级：
      1) ak.rate_interbank（主源，实时）
      2) ak.shibor_data（备源，历史数据取最新）
      3) 昨日 macro_snapshot 缓存文件（兜底，保证不返回 None）

    Returns:
        float | None: 隔夜利率（%），None 仅在完全无法获取时返回。
    """
    import akshare as ak

    # --- 源 1: macro_china_shibor_all（主源，最快最稳）---
    try:
        df = _call_with_retry(
            lambda: ak.macro_china_shibor_all(),
            DASHBOARD_API_TIMEOUT,
        )
        if df is not None and not df.empty and "O/N-定价" in df.columns:
            val = float(df.iloc[-1]["O/N-定价"])
            if val > 0:
                print(f"[dashboard] SHIBOR 隔夜 (macro_china_shibor_all): {val}%")
                return val
    except Exception as exc:
        print(f"[dashboard] SHIBOR 源1 macro_china_shibor_all 失败: {exc}")

    # --- 源 2: rate_interbank（备源）---
    try:
        shibor = _call_with_retry(
            lambda: ak.rate_interbank(market="上海银行间同业拆放利率", symbol="Shibor", indicator="隔夜"),
            DASHBOARD_API_TIMEOUT,
        )
        if shibor is not None and not shibor.empty and "利率" in shibor.columns:
            val = float(shibor.iloc[-1].get("利率", 0))
            if val > 0:
                print(f"[dashboard] SHIBOR 隔夜 (rate_interbank): {val}%")
                return val
    except Exception as exc:
        print(f"[dashboard] SHIBOR 源2 rate_interbank 失败: {exc}")

    # --- 兜底: 读昨日缓存 ---
    try:
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        cache_path = CACHE_DIR / f"macro_snapshot_{yesterday}.pkl"
        if cache_path.exists():
            with open(cache_path, "rb") as f:
                cached = pickle.load(f)
            cached_val = cached.get("Shibor隔夜")
            if cached_val is not None and float(cached_val) > 0:
                print(f"[dashboard] SHIBOR 隔夜 (昨日缓存兜底): {cached_val}%")
                return float(cached_val)
    except Exception as exc:
        print(f"[dashboard] SHIBOR 缓存兜底读取失败: {exc}")

    print("[dashboard] ⚠️ SHIBOR 隔夜所有数据源均失败，返回 None")
    return None


def build_dashboard_payload() -> dict[str, Any]:
    """Build a JSON-friendly dashboard payload for the frontend."""
    payload: dict[str, Any] = {"generated_at": datetime.now().isoformat(timespec="seconds")}

    # ── 指数快照：缓存优先，缺失时实时回退 ────────────────
    cached_index = _load_cache("index_snapshot")
    if isinstance(cached_index, pd.DataFrame) and not cached_index.empty:
        payload["index_snapshot"] = cached_index.to_dict(orient="records")
    else:
        # 缓存未命中（Render 上 prewarm 可能因 TDX/新浪失败而跳过）→ 同步重拉
        print("[dashboard] 指数快照缓存为空，尝试实时获取...")
        try:
            fresh_index = fetch_index_snapshot()
            if not fresh_index.empty:
                payload["index_snapshot"] = fresh_index.to_dict(orient="records")
                save_cache("index_snapshot", fresh_index)
                print("[dashboard] 指数快照实时获取成功")
            else:
                payload["index_snapshot"] = []
                print("[dashboard] 指数快照实时获取也失败（TDX+新浪均不可达）")
        except Exception as exc:
            payload["index_snapshot"] = []
            print(f"[dashboard] 指数快照实时获取异常: {exc}")

    cached_breadth = _load_cache("market_breadth")
    payload["market_breadth"] = cached_breadth if isinstance(cached_breadth, dict) else {}

    # ── 宏观快照：缓存完整校验 + TTL 懒刷新 ─────────────────
    maybe_refresh_macro_cache()
    cached_macro = _load_cache("macro_snapshot")
    if not isinstance(cached_macro, dict):
        cached_macro = {}
    # 如果缓存中任何核心指标为空（旧代码遗留的 None），强制同步重拉
    _CORE_MACRO_KEYS = [
        "美元/人民币", "Shibor隔夜", "LPR_1年", "10Y国债收益率",
        "制造业PMI", "CPI同比",
    ]
    if any(cached_macro.get(k) is None for k in _CORE_MACRO_KEYS):
        print("[dashboard] 宏观缓存存在空指标，强制同步重拉...")
        try:
            fresh = fetch_macro_snapshot()
            if fresh and isinstance(fresh, dict):
                # 确认新数据补全了之前的空项（种子兜底保证一定有值）
                if all(fresh.get(k) is not None for k in _CORE_MACRO_KEYS):
                    save_cache("macro_snapshot", fresh)
                    cached_macro = fresh
                    print("[dashboard] 宏观缓存已更新")
                else:
                    print(f"[dashboard] 重拉后仍有空项: {[k for k in _CORE_MACRO_KEYS if fresh.get(k) is None]}")
                    # 即使没全补全也比旧缓存好（至少部分有值）
                    cached_macro = fresh
            else:
                print("[dashboard] fetch_macro_snapshot 返回空")
        except Exception as exc:
            # Render 上 akshare 可能装了但网络全挂 → 不让整个 dashboard 炸掉
            print(f"[dashboard] 宏观强制重拉异常（保留旧缓存）: {exc}")
    payload["macro_snapshot"] = cached_macro
    return payload


def save_cache(key: str, data: Any) -> Path:
    """Save a dashboard cache file under stock_data/daily_cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y-%m-%d")
    path = CACHE_DIR / f"{key}_{today}.pkl"
    with open(path, "wb") as file_handle:
        pickle.dump(data, file_handle)
    return path


def clean_old_cache(keep_days: int = 7) -> int:
    """Delete cache files older than keep_days."""
    if not CACHE_DIR.exists():
        return 0

    cutoff = datetime.now().timestamp() - keep_days * 86400
    removed = 0
    for file_path in CACHE_DIR.glob("*.pkl"):
        if file_path.stat().st_mtime < cutoff:
            file_path.unlink(missing_ok=True)
            removed += 1
    return removed


def prewarm_dashboard_cache(keep_days: int = 7) -> dict[str, Any]:
    """Refresh the dashboard cache files used by the UI."""
    configure_runtime()

    result: dict[str, Any] = {}

    index_snapshot = fetch_index_snapshot()
    if not index_snapshot.empty:
        result["index_snapshot"] = save_cache("index_snapshot", index_snapshot).name

    market_breadth = fetch_market_breadth()
    if market_breadth:
        result["market_breadth"] = save_cache("market_breadth", market_breadth).name

    macro_snapshot = fetch_macro_snapshot()
    if macro_snapshot:
        result["macro_snapshot"] = save_cache("macro_snapshot", macro_snapshot).name

    result["removed_cache_files"] = clean_old_cache(keep_days=keep_days)
    return result