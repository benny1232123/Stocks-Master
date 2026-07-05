"""FastAPI entrypoint for Stocks-Master."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIST = ROOT / "frontend" / "dist"
VIZ_SRC = ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"

for candidate in [str(ROOT), str(VIZ_SRC)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

os.environ.setdefault("KLINE_BACKEND", "akshare")

from smcore.artifacts import find_latest_file, find_latest_file_any, preview_csv, read_csv_file
from smcore.analysis import build_stock_analysis
from smcore.backtest import run_signal_backtest
from smcore.dashboard import build_dashboard_payload, prewarm_dashboard_cache
from smcore.holdings import add_trade, clear_trades, portfolio_snapshot
from smcore.selection import get_candidate_codes, run_strategy_fusion, scan_boll_batch

app = FastAPI(title="Stocks-Master API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if FRONTEND_DIST.exists():
    assets_dir = FRONTEND_DIST / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")


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


@app.get("/api/portfolio")
def portfolio() -> dict:
    return portfolio_snapshot()


@app.post("/api/trades")
def create_trade(payload: dict) -> dict:
    trade = {
        "date": payload.get("date"),
        "code": str(payload.get("code", "")).strip(),
        "name": str(payload.get("name", "")).strip() or str(payload.get("code", "")).strip(),
        "side": payload.get("side", "buy"),
        "price": float(payload.get("price", 0)),
        "qty": int(payload.get("qty", 0)),
        "fee": float(payload.get("fee", 0)),
        "notes": str(payload.get("notes", "")),
    }
    trades = add_trade(trade)
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
    window = int(payload.get("window", 20))
    k = float(payload.get("k", 1.645))
    near_ratio = float(payload.get("near_ratio", 1.015))
    days_back = int(payload.get("days_back", 180))


    @app.get("/{path:path}")
    def spa_fallback(path: str) -> FileResponse:
        if FRONTEND_DIST.exists():
            index_file = FRONTEND_DIST / "index.html"
            if index_file.exists():
                return FileResponse(index_file)
        return FileResponse(ROOT / "Readme.md")
    result = scan_boll_batch(codes, window=window, k=k, near_ratio=near_ratio, days_back=days_back)
    return {"count": int(len(result)), "rows": result.to_dict(orient="records")}


@app.post("/api/selection/fusion")
def selection_fusion(payload: dict) -> dict:
    return run_strategy_fusion(
        date_yyyymmdd=payload.get("date"),
        total_capital=float(payload.get("total_capital", 100000.0)),
        max_picks=int(payload.get("max_picks", 15)),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        reload=os.environ.get("RELOAD", "0") == "1",
    )