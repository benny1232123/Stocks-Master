"""FastAPI entrypoint for Stocks-Master."""
from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIST = ROOT / "frontend" / "dist"

load_dotenv(ROOT / ".env")

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("KLINE_BACKEND", "akshare")

from smcore.artifacts import ArtifactFile, find_latest_file, find_latest_file_any, preview_csv, read_csv_file, STOCK_DATA_DIR
from smcore.analysis import build_stock_analysis
from smcore.backtest import run_signal_backtest, run_multi_strategy_backtest
from smcore.dashboard import build_dashboard_payload, prewarm_dashboard_cache
from smcore.holdings import add_trade, clear_trades, portfolio_snapshot, trades_backend_name
from smcore.selection import get_candidate_codes, run_strategy_fusion, scan_boll_batch

@asynccontextmanager
async def lifespan(_app: FastAPI):
    threading.Thread(target=prewarm_dashboard_cache, daemon=True).start()
    threading.Thread(target=_periodic_sweep, daemon=True).start()
    yield


app = FastAPI(title="Stocks-Master API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

if FRONTEND_DIST.exists():
    assets_dir = FRONTEND_DIST / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")


_tasks_lock = threading.Lock()
_tasks: dict[str, dict] = {}
_TASK_TTL = 1800  # 30 minutes


def _sweep_tasks() -> None:
    """Remove completed tasks older than _TASK_TTL to prevent memory leak."""
    now = time.time()
    with _tasks_lock:
        expired = [
            tid for tid, t in _tasks.items()
            if t["status"] != "running" and (now - t.get("started_at", now)) > _TASK_TTL
        ]
        for tid in expired:
            del _tasks[tid]


def _periodic_sweep() -> None:
    """Background thread that sweeps stale tasks every 5 minutes."""
    while True:
        time.sleep(300)
        try:
            _sweep_tasks()
        except Exception:
            pass


def _new_task(task_type: str) -> str:
    task_id = uuid.uuid4().hex[:12]
    with _tasks_lock:
        _tasks[task_id] = {
            "type": task_type,
            "status": "running",
            "logs": [],
            "result": None,
            "cancelled": False,
            "started_at": time.time(),
        }
    return task_id


def _is_cancelled(task_id: str) -> bool:
    with _tasks_lock:
        t = _tasks.get(task_id)
        return t is not None and t.get("cancelled", False)


def _append_log(task_id: str, msg: str) -> None:
    with _tasks_lock:
        t = _tasks.get(task_id)
        if t is not None:
            t["logs"].append(msg)


def _finish_task(task_id: str, result=None, error: str | None = None) -> None:
    with _tasks_lock:
        t = _tasks.get(task_id)
        if t is not None:
            t["status"] = "error" if error else "done"
            t["result"] = result
            if error:
                t["logs"].append(f"[错误] {error}")


@app.get("/")
def root():
    if FRONTEND_DIST.exists():
        index_file = FRONTEND_DIST / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
    return {"message": "Stocks-Master API", "status": "ok"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/status")
def app_status() -> dict:
    backend = trades_backend_name()
    supabase_configured = bool(os.getenv("SUPABASE_URL", "").strip() and os.getenv("SUPABASE_KEY", "").strip())
    return {
        "storage_backend": backend,
        "supabase_configured": supabase_configured,
        "supabase_url": os.getenv("SUPABASE_URL", "")[:30] + "..." if os.getenv("SUPABASE_URL", "") else "",
    }


@app.get("/api/dashboard")
def dashboard() -> dict:
    return build_dashboard_payload()


@app.post("/api/dashboard/prewarm")
def prewarm_dashboard() -> dict:
    return prewarm_dashboard_cache()


@app.get("/api/artifacts/daily-action-list")
def daily_action_list() -> dict:
    latest = find_latest_file("Daily-Action-List-*.csv")
    if latest is None:
        return {"latest": None, "preview": {"rows": [], "columns": []}}

    return {"latest": latest.__dict__, "preview": preview_csv(latest.path)}


@app.get("/api/artifacts/daily-action-list/full")
def daily_action_list_full(date: str = None) -> dict:
    """返回完整日报数据（全部行），供前端「日报」页全量查看。
    可选 ?date=YYYYMMDD 指定某天；缺省返回最新一天。"""
    target = None
    if date:
        p = STOCK_DATA_DIR / f"Daily-Action-List-{date}.csv"
        if p.exists():
            target = ArtifactFile(name=p.name, path=str(p.relative_to(ROOT)), modified_at=p.stat().st_mtime)
    if target is None:
        target = find_latest_file("Daily-Action-List-*.csv")
    if target is None:
        return {"latest": None, "columns": [], "rows": [], "total": 0}

    frame = read_csv_file(target.path)
    if frame.empty:
        return {"latest": target.__dict__, "columns": frame.columns.tolist(), "rows": [], "total": 0}

    return {
        "latest": target.__dict__,
        "columns": frame.columns.tolist(),
        "rows": frame.to_dict(orient="records"),
        "total": len(frame),
    }


@app.get("/api/artifacts/daily-action-list/dates")
def daily_action_list_dates() -> dict:
    """返回全部历史日报日期列表，供前端日报页「日期选择器」切换。"""
    import glob as _glob
    import re as _re

    files = sorted(_glob.glob(str(STOCK_DATA_DIR / "Daily-Action-List-*.csv")), reverse=True)
    items = []
    for f in files:
        m = _re.search(r"(\d{8})", os.path.basename(f))
        if not m:
            continue
        tag = m.group(1)
        try:
            df = read_csv_file(str(Path(f).relative_to(ROOT)))
        except Exception:
            df = None
        items.append({
            "date": tag,
            "name": os.path.basename(f),
            "path": str(Path(f).relative_to(ROOT)),
            "modified_at": Path(f).stat().st_mtime,
            "total": len(df) if df is not None else 0,
        })
    return {"items": items}



@app.get("/api/portfolio")
def portfolio() -> dict:
    return portfolio_snapshot()


@app.post("/api/trades")
def create_trade(payload: dict) -> dict:
    code = str(payload.get("code", "")).strip()
    if not code:
        return JSONResponse({"error": "股票代码不能为空"}, status_code=400)
    try:
        price = float(payload.get("price", 0))
        qty = int(payload.get("qty", 0))
        fee = float(payload.get("fee", 0))
    except (TypeError, ValueError):
        return JSONResponse({"error": "价格/数量/手续费格式无效"}, status_code=400)
    if price < 0 or qty <= 0 or fee < 0:
        return JSONResponse({"error": "价格不能为负，数量必须大于0"}, status_code=400)
    side = payload.get("side", "buy")
    if side not in ("buy", "sell"):
        side = "buy"
    trade = {
        "date": payload.get("date") or date.today().isoformat(),
        "code": code,
        "name": str(payload.get("name", "")).strip() or code,
        "side": side,
        "price": price,
        "qty": qty,
        "fee": fee,
        "notes": str(payload.get("notes", "")),
    }
    try:
        trades = add_trade(trade)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return {"count": len(trades), "latest": trade}


@app.delete("/api/trades")
def remove_trades() -> dict:
    clear_trades()
    return {"status": "ok"}


@app.get("/api/backtests/latest")
def latest_backtest() -> dict:
    latest = find_latest_file_any(
        [
            "Signal-Backtest-*-summary.csv",
            "Trade-Backtest-*-summary.csv",
            "*-portfolio-summary.csv",
        ]
    )
    if latest is None:
        return {"latest": None, "preview": {"rows": [], "columns": []}}

    return {"latest": latest.__dict__, "preview": preview_csv(latest.path)}


@app.get("/api/backtests/daily-latest")
def daily_latest_backtest() -> dict:
    """读取每日 CI 自动对全策略清单跑出的前向信号回测结果（Multi-Backtest-*）。

    返回全部历史批次（按信号日倒序），前端以「信号日选择器」形式展示，
    每个信号日对应一次独立的「从历史某天开始 → 往后持有 N 天」的前向回测。
    """
    import glob as _glob

    from smcore.artifacts import STOCK_DATA_DIR

    files = sorted(_glob.glob(str(STOCK_DATA_DIR / "Multi-Backtest-*-summary.csv")), reverse=True)
    items = []
    for f in files:
        name = os.path.basename(f)
        date_tag = name[len("Multi-Backtest-"):-len("-summary.csv")]

        def _read(suffix: str):
            df = read_csv_file(f"stock_data/Multi-Backtest-{date_tag}-{suffix}.csv")
            recs = df.to_dict(orient="records") if not df.empty else []
            if suffix == "trades":
                # 附带股票名称：从当日信号清单读取 代码→名称
                name_map = {}
                dal = read_csv_file(f"stock_data/Daily-Action-List-{date_tag}.csv")
                if not dal.empty and {"股票代码", "股票名称"}.issubset(dal.columns):
                    for _, nr in dal.iterrows():
                        c = str(nr.get("股票代码", "")).strip()
                        if c:
                            name_map[c] = str(nr.get("股票名称", ""))
                for rec in recs:
                    c = str(rec.get("code", "")).strip().zfill(6)
                    rec["name"] = name_map.get(c, "") or name_map.get(c.lstrip("0"), "")
            return recs

        summary_df = read_csv_file(f"stock_data/Multi-Backtest-{date_tag}-summary.csv")
        summary = summary_df.to_dict(orient="records")[0] if not summary_df.empty else None
        if summary is None:
            continue
        items.append({
            "date": date_tag,
            "summary": summary,
            "equity": _read("equity"),
            "trades": _read("trades"),
        })
    latest = items[0] if items else None
    return {"items": items, "latest": latest}


@app.get("/api/backtests/daily-summary")
def daily_backtest_summary() -> dict:
    """聚合全部每日前向回测批次（Multi-Backtest-*-summary.csv），产出总体总结指标。"""
    import glob as _glob
    import statistics as _stats

    from smcore.artifacts import STOCK_DATA_DIR

    files = sorted(_glob.glob(str(STOCK_DATA_DIR / "Multi-Backtest-*-summary.csv")), reverse=True)
    rows = []
    for f in files:
        name = os.path.basename(f)
        date_tag = name[len("Multi-Backtest-"):-len("-summary.csv")]
        summary_df = read_csv_file(f"stock_data/Multi-Backtest-{date_tag}-summary.csv")
        if summary_df.empty:
            continue
        rec = summary_df.to_dict(orient="records")[0]
        rec["date"] = date_tag
        rows.append(rec)

    if not rows:
        return {"count": 0}

    def _num(x, default=0.0):
        try:
            return float(x)
        except (TypeError, ValueError):
            return default

    # 区分「已完成」（有真实成交）与「未走完」（num_trades=0 的部分前向批次）。
    # 未走完批次收益≈0、win_rate=0，纳入均值会严重低估真实表现，故聚合指标只基于已完成批次。
    completed = [r for r in rows if _num(r.get("num_trades")) > 0]
    base_rows = completed if completed else rows

    rets = [_num(r.get("total_return")) for r in base_rows]
    dds = [_num(r.get("max_drawdown")) for r in base_rows]
    wrs = [_num(r.get("win_rate")) for r in base_rows]
    shps = [_num(r.get("sharpe")) for r in base_rows]
    holds = [_num(r.get("hold_days")) for r in base_rows]
    total_trades = sum(int(_num(r.get("num_trades"), 0)) for r in base_rows)

    # 胜率按「单笔交易」层面统计（更真实）：汇总各已完成批次 trades 中盈利笔数。
    win_trades = 0
    for r in base_rows:
        tag = r.get("date")
        tdf = read_csv_file(f"stock_data/Multi-Backtest-{tag}-trades.csv")
        if tdf.empty or "return_pct" not in tdf.columns:
            continue
        win_trades += int((tdf["return_pct"] > 0).sum())
    avg_win_rate = round(win_trades / total_trades * 100, 1) if total_trades else 0.0

    positive_days = sum(1 for v in rets if v > 0)
    best = max(base_rows, key=lambda r: _num(r.get("total_return")))
    worst = min(base_rows, key=lambda r: _num(r.get("total_return")))

    def _med(lst):
        return _stats.median(lst) if lst else 0.0

    return {
        "count": len(rows),
        "completed_count": len(completed),
        "avg_return": round(_stats.mean(rets), 2),
        "median_return": round(_med(rets), 2),
        "avg_drawdown": round(_stats.mean(dds), 2),
        "median_drawdown": round(_med(dds), 2),
        "avg_win_rate": avg_win_rate,
        "median_win_rate": round(_med(wrs), 1),
        "avg_sharpe": round(_stats.mean(shps), 2),
        "total_trades": total_trades,
        "positive_days": positive_days,
        "positive_ratio": round(positive_days / len(base_rows) * 100, 1),
        "avg_hold_days": round(_stats.mean(holds), 1),
        "best_day": {"date": best.get("date"), "return": _num(best.get("total_return"))},
        "worst_day": {"date": worst.get("date"), "return": _num(worst.get("total_return"))},
    }


@app.post("/api/backtests/run-latest")
def run_latest_backtest(payload: dict | None = None) -> dict:
    payload = payload or {}
    latest = find_latest_file("Daily-Action-List-*.csv")
    if latest is None:
        return {"summary": {"error": "未找到操作清单"}}
    signals = read_csv_file(latest.path)
    result = run_signal_backtest(
        signals,
        hold_days=int(payload.get("hold_days", 5)),
        initial_capital=float(payload.get("initial_capital", 100000)),
        max_positions=int(payload.get("max_positions", 10)),
        slippage=float(payload.get("slippage", 0.001)),
    )
    return {
        "source": latest.__dict__,
        "summary": result.summary,
        "equity_preview": result.equity.head(25).to_dict(orient="records"),
        "trades_preview": result.trades.head(25).to_dict(orient="records"),
    }


@app.post("/api/backtests/run")
def run_backtest(payload: dict) -> dict:
    codes = payload.get("codes") or []
    signal_date = payload.get("date") or date.today().strftime("%Y%m%d")
    if isinstance(codes, str):
        codes = [c.strip() for c in codes.replace("\n", ",").split(",") if c.strip()]
    if not codes:
        return {"summary": {"error": "未提供股票代码"}}
    codes = codes[:3000]

    # 多策略 Backtrader 模式：传入 mode="multi" + start/end/strategies
    mode = str(payload.get("mode", "signal")).lower()
    if mode == "multi":
        start = _parse_date(payload.get("start"), date.today() - timedelta(days=365))
        end = _parse_date(payload.get("end"), date.today())
        strategies = payload.get("strategies", "boll,relativity,theme")
        task_id = _new_task("backtest")
        _append_log(task_id, f"开始多策略回测({strategies})，共 {len(codes)} 只股票，区间 {start}~{end}")

        def _run_multi():
            try:
                _append_log(task_id, "正在拉取K线并运行多策略 Backtrader 引擎...")
                result = run_multi_strategy_backtest(
                    codes,
                    start,
                    end,
                    initial_capital=float(payload.get("initial_capital", 100000)),
                    strategies=strategies,
                )
                _append_log(task_id, f"回测完成：{result.summary.get('num_trades', 0)} 笔交易")
                _finish_task(task_id, result={
                    "summary": result.summary,
                    "equity": result.equity.to_dict(orient="records"),
                    "trades": result.trades.to_dict(orient="records"),
                })
            except Exception as e:
                _finish_task(task_id, error=str(e))

        threading.Thread(target=_run_multi, daemon=True).start()
        return {"task_id": task_id}

    import pandas as pd
    signals = pd.DataFrame({"日期": [signal_date] * len(codes), "代码": codes})

    task_id = _new_task("backtest")
    _append_log(task_id, f"开始回测，共 {len(codes)} 只股票")

    def _run():
        try:
            _append_log(task_id, "正在拉取K线并模拟交易...")
            result = run_signal_backtest(
                signals,
                hold_days=int(payload.get("hold_days", 5)),
                initial_capital=float(payload.get("initial_capital", 100000)),
                max_positions=int(payload.get("max_positions", 10)),
                slippage=float(payload.get("slippage", 0.001)),
            )
            _append_log(task_id, f"回测完成：{result.summary.get('num_trades', 0)} 笔交易")
            _finish_task(task_id, result={
                "summary": result.summary,
                "equity": result.equity.to_dict(orient="records"),
                "trades": result.trades.to_dict(orient="records"),
            })
        except Exception as e:
            _finish_task(task_id, error=str(e))

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


def _parse_date(value, default: date) -> date:
    """解析 YYYY-MM-DD / YYYYMMDD 为 date，失败返回 default。"""
    if not value:
        return default
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return default


@app.get("/api/analysis/{code}")
def analysis(code: str, window: int = 20, k: float = 1.645, days_back: int = 180) -> dict:
    return build_stock_analysis(code, window=window, k=k, days_back=days_back)


@app.get("/api/selection/candidates")
def selection_candidates(price_min: float = 5.0, price_max: float = 30.0) -> dict:
    codes, cache_date = get_candidate_codes(price_min, price_max)
    return {"codes": codes, "count": len(codes), "cache_date": cache_date}


@app.post("/api/selection/boll-scan")
def selection_boll_scan(payload: dict) -> dict:
    codes = payload.get("codes") or []
    if isinstance(codes, str):
        codes = [item.strip() for item in codes.replace("\n", ",").replace(" ", ",").split(",") if item.strip()]
    codes = codes[:3000]
    window = int(payload.get("window", 20))
    k = float(payload.get("k", 1.645))
    near_ratio = float(payload.get("near_ratio", 1.015))
    days_back = int(payload.get("days_back", 180))

    task_id = _new_task("boll-scan")
    _append_log(task_id, f"开始布林扫描，共 {len(codes)} 只股票")

    def _run():
        def on_progress(idx, total, code, msg):
            _append_log(task_id, f"[{idx}/{total}] {code} {msg}")
        try:
            result = scan_boll_batch(
                codes, window=window, k=k, near_ratio=near_ratio,
                days_back=days_back, on_progress=on_progress,
                is_cancelled=lambda: _is_cancelled(task_id),
            )
            if _is_cancelled(task_id):
                return
            _append_log(task_id, f"扫描完成，命中 {len(result)} 只")
            _finish_task(task_id, result={"count": int(len(result)), "rows": result.to_dict(orient="records")})
        except Exception as e:
            if not _is_cancelled(task_id):
                _finish_task(task_id, error=str(e))

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@app.get("/api/selection/task-logs/{task_id}")
def selection_task_logs(task_id: str) -> dict:
    with _tasks_lock:
        t = _tasks.get(task_id)
        if t is None:
            return {"status": "not_found", "logs": [], "result": None}
        snapshot = {"status": t["status"], "logs": list(t["logs"]), "result": t.get("result")}
    return snapshot


@app.post("/api/selection/cancel-task/{task_id}")
def selection_cancel_task(task_id: str) -> dict:
    with _tasks_lock:
        t = _tasks.get(task_id)
        if t is None:
            return {"ok": False, "error": "task not found"}
        if t["status"] != "running":
            return {"ok": False, "error": f"task already {t['status']}"}
        t["cancelled"] = True
        t["status"] = "cancelled"
        t["logs"].append("[系统] 用户取消任务")
    return {"ok": True}


@app.post("/api/selection/fusion")
def selection_fusion(payload: dict) -> dict:
    task_id = _new_task("fusion")
    _append_log(task_id, "开始策略融合")

    def _run():
        try:
            _append_log(task_id, "加载四策略 CSV ...")
            result = run_strategy_fusion(
                date_yyyymmdd=payload.get("date"),
                total_capital=float(payload.get("total_capital", 100000.0)),
                max_picks=int(payload.get("max_picks", 15)),
            )
            _append_log(task_id, f"融合完成，命中 {result.get('count', 0)} 只")
            _finish_task(task_id, result=result)
        except Exception as e:
            _finish_task(task_id, error=str(e))

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@app.get("/{path:path}")
def spa_fallback(path: str):
    if FRONTEND_DIST.exists():
        index_file = FRONTEND_DIST / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
    return JSONResponse({"error": "not found"}, status_code=404)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        reload=os.environ.get("RELOAD", "0") == "1",
    )
