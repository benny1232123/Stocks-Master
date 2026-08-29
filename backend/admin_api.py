"""持仓管理接口（供 /admin 页面使用）。

所有写操作都需要管理员令牌（``X-Admin-Token``），令牌由
:mod:`smcore.admin_auth` 用 ``ADMIN_PASSWORD`` 签发。未设置 ``ADMIN_PASSWORD``
时管理端**整体禁用**（安全默认）。

核心能力：
- 快照录入：**只需代码 + 数量**，日期/成本价自动占位（见 :mod:`smcore.holdings_snapshot`）。
- 完整录入、删除单条、清空。
- 读取当前持仓 + 流水（补上前端 ``open_positions`` 从未渲染的缺口）。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from smcore import admin_auth
from smcore.holdings import (
    add_trade,
    clear_trades,
    compute_fifo_positions,
    load_trades,
    portfolio_snapshot,
)
from smcore.holdings_snapshot import build_snapshot_trades
from smcore.storage.trades_repo import get_trade_repository

router = APIRouter(prefix="/api/admin", tags=["admin"])


def _client_ip(request: Request) -> str:
    """取客户端 IP，兼容 Render / Nginx 反向代理。"""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return (request.client.host if request.client else "") or "unknown"


def _require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    """校验管理员令牌。管理端未启用时 403，令牌无效/过期时 401。"""
    if not admin_auth.is_admin_enabled():
        raise HTTPException(
            status_code=403,
            detail="管理端未启用：请先在环境变量设置 ADMIN_PASSWORD",
        )
    if not admin_auth.verify_token(x_admin_token):
        raise HTTPException(status_code=401, detail="管理员令牌无效或已过期，请重新登录")


def _trade_rows() -> list[dict[str, Any]]:
    """交易流水（带 id，供前端删除单条）。"""
    trades = load_trades()
    rows: list[dict[str, Any]] = []
    for idx, t in enumerate(trades):
        rows.append(
            {
                "id": t.get("id") if t.get("id") is not None else idx,
                "date": t.get("date", ""),
                "code": t.get("code", ""),
                "name": t.get("name", ""),
                "side": t.get("side", ""),
                "price": t.get("price", 0),
                "qty": t.get("qty", 0),
                "fee": t.get("fee", 0),
                "notes": t.get("notes", ""),
            }
        )
    return rows


# --------------------------------------------------------------------------
# 公开：状态
# --------------------------------------------------------------------------
@router.get("/status")
def admin_status() -> dict[str, Any]:
    """管理端是否可用（不泄露密码）。前端据此显示登录页或配置提示。"""
    return admin_auth.auth_status()


# --------------------------------------------------------------------------
# 登录
# --------------------------------------------------------------------------
@router.post("/login")
def admin_login(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    """用 ADMIN_PASSWORD 换一个短期令牌。连续失败会被按 IP 限流。"""
    if not admin_auth.is_admin_enabled():
        raise HTTPException(
            status_code=403,
            detail="管理端未启用：请先在环境变量设置 ADMIN_PASSWORD",
        )

    ip = _client_ip(request)
    if admin_auth.is_locked_out(ip):
        raise HTTPException(status_code=429, detail="尝试次数过多，请稍后再试（15 分钟）")

    password = str(payload.get("password", ""))
    if not admin_auth.check_password(password, client_ip=ip):
        # 不区分「密码错」与「已锁定」，避免信息泄露
        raise HTTPException(status_code=401, detail="密码错误")

    ttl = admin_auth.DEFAULT_TTL
    return {"token": admin_auth.issue_token(ttl), "expires_in": ttl}


# --------------------------------------------------------------------------
# 需要令牌
# --------------------------------------------------------------------------
@router.get("/holdings")
def get_holdings(_: None = Depends(_require_admin)) -> dict[str, Any]:
    """当前持仓 + 流水 + 盈亏汇总。"""
    repo = get_trade_repository()
    trades = load_trades()
    pos_df, _closed = compute_fifo_positions(trades)

    snapshot: dict[str, Any] = {}
    try:
        snapshot = portfolio_snapshot()
    except Exception as exc:  # 实时行情失败不应阻断持仓展示
        snapshot = {"pnl_summary": {}, "realtime_positions": [], "error": str(exc)}

    return {
        "backend": repo.backend_name,
        "trades_count": len(trades),
        "trades": _trade_rows(),
        "positions": pos_df.to_dict(orient="records") if not pos_df.empty else [],
        "realtime_positions": snapshot.get("realtime_positions", []),
        "pnl_summary": snapshot.get("pnl_summary", {}),
    }


@router.post("/holdings/snapshot")
def post_snapshot(payload: dict[str, Any], _: None = Depends(_require_admin)) -> dict[str, Any]:
    """快照录入：只需 code + qty，日期/成本价自动占位。

    body: ``{"items": [{"code": "600519", "qty": 100, "name": "贵州茅台"}], "replace": false}``
    """
    repo = get_trade_repository()
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="items 不能为空")

    trades, skipped = build_snapshot_trades(items, default_date=payload.get("default_date"))
    if not trades:
        return {"inserted": 0, "skipped": skipped, "backend": repo.backend_name}

    if payload.get("replace"):
        clear_trades()

    inserted = repo.append_many(trades)
    return {
        "inserted": inserted,
        "skipped": skipped,
        "backend": repo.backend_name,
        "preview": trades[:20],
    }


@router.post("/trades")
def post_trade(payload: dict[str, Any], _: None = Depends(_require_admin)) -> dict[str, Any]:
    """完整录入一笔交易（code/side/price/qty 必填；code 可用名称）。"""
    from smcore.holdings_snapshot import resolve_identifier

    code_raw = str(payload.get("code", "")).strip()
    if not code_raw:
        raise HTTPException(status_code=400, detail="股票代码不能为空")
    code, resolved_name, err = resolve_identifier(code_raw)
    if not code:
        raise HTTPException(status_code=400, detail=err or "股票代码或名称无效")

    try:
        price = float(payload.get("price", 0))
        qty = int(payload.get("qty", 0))
        fee = float(payload.get("fee", 0) or 0)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="价格/数量/手续费格式无效")
    if price <= 0 or qty <= 0 or fee < 0:
        raise HTTPException(status_code=400, detail="价格与数量必须 > 0，手续费不能为负")

    side = str(payload.get("side", "buy")).lower()
    if side not in ("buy", "sell"):
        side = "buy"

    from datetime import date as _date

    trade = {
        "date": str(payload.get("date") or _date.today().isoformat()),
        "code": code,
        "name": str(payload.get("name", "")).strip() or resolved_name or code,
        "side": side,
        "price": price,
        "qty": qty,
        "fee": fee,
        "notes": str(payload.get("notes", "")),
    }
    try:
        trades = add_trade(trade)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {"count": len(trades), "latest": trade}


@router.get("/resolve")
def resolve_stock(q: str, _: None = Depends(_require_admin)) -> dict[str, Any]:
    """按股票**名称或代码**查候选，供录入时即时校验/补全。

    ``?q=贵州茅台`` → ``{"ok": true, "code": "600519", "name": "贵州茅台", "candidates": [...]}``
    """
    from smcore.holdings_snapshot import resolve_identifier

    code, name, err = resolve_identifier(q)
    return {
        "query": q,
        "ok": bool(code),
        "code": code,
        "name": name,
        "error": err,
        "candidates": ([] if code else _lookup_candidates(q)),
    }


def _lookup_candidates(q: str, limit: int = 8) -> list[dict[str, Any]]:
    from smcore.stock_names import lookup_candidates

    try:
        return lookup_candidates(q, limit=limit)
    except Exception:
        return []


@router.put("/trades/{trade_id}")
def update_trade(
    trade_id: str,
    payload: dict[str, Any],
    _: None = Depends(_require_admin),
) -> dict[str, Any]:
    """修改一笔交易（用于「更改持仓」：改成本价 / 股数 / 日期等）。

    只更新**显式传入**的字段，可传：date / code / name / side / price / qty / fee / notes。
    """
    repo = get_trade_repository()

    allowed = ("date", "code", "name", "side", "price", "qty", "fee", "notes")
    updates: dict[str, Any] = {}
    for key in allowed:
        if key not in payload:
            continue
        value = payload[key]
        if key in ("price", "qty", "fee"):
            try:
                value = float(value)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail=f"{key} 必须是数字")
            if key == "price" and value <= 0:
                raise HTTPException(status_code=400, detail="价格必须 > 0")
            if key == "qty" and value <= 0:
                raise HTTPException(status_code=400, detail="数量必须 > 0")
            if key == "fee" and value < 0:
                raise HTTPException(status_code=400, detail="手续费不能为负")
        elif key == "side":
            if str(value).lower() not in ("buy", "sell"):
                raise HTTPException(status_code=400, detail="side 只能是 buy/sell")
            value = str(value).lower()
        elif key == "code":
            from smcore.holdings_snapshot import resolve_identifier

            code, name, err = resolve_identifier(str(value))
            if not code:
                raise HTTPException(status_code=400, detail=err or "股票代码或名称无效")
            updates["code"] = code
            if "name" not in payload and name:
                updates["name"] = name
            continue
        else:
            value = str(value)
        updates[key] = value

    if not updates:
        raise HTTPException(status_code=400, detail="没有需要更新的字段")

    if not repo.update_by_id(trade_id, updates):
        raise HTTPException(
            status_code=404,
            detail=f"未找到 id={trade_id} 的交易，或其后端不支持按 id 更新",
        )
    return {"status": "ok", "updated": trade_id, "fields": sorted(updates.keys())}


@router.delete("/trades/{trade_id}")
def delete_trade(trade_id: str, _: None = Depends(_require_admin)) -> dict[str, Any]:
    """删除单条交易。"""
    repo = get_trade_repository()
    ok = repo.delete_by_id(trade_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"未找到 id={trade_id} 的交易，或其后端不支持按 id 删除")
    return {"status": "ok", "deleted": trade_id}


@router.delete("/trades")
def delete_all_trades(_: None = Depends(_require_admin)) -> dict[str, Any]:
    """清空全部交易（不可撤销）。"""
    clear_trades()
    return {"status": "ok"}
