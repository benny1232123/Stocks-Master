"""Trade persistence — JSON (local) or Supabase (cloud) backends.

Environment variables:
- TRADES_BACKEND: ``json`` | ``supabase`` | ``auto`` (default ``auto``)
- SUPABASE_URL / SUPABASE_KEY: enable Supabase when set
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code

logger = logging.getLogger("smcore.storage.trades_repo")

TRADES_FILE = STOCK_DATA_DIR / "trades.json"

SUPABASE_SCHEMA_SQL = """\
-- Run in Supabase SQL Editor
CREATE TABLE IF NOT EXISTS trades (
    id          BIGSERIAL PRIMARY KEY,
    trade_date  TEXT NOT NULL,
    code        TEXT NOT NULL,
    name        TEXT DEFAULT '',
    side        TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    price       DOUBLE PRECISION NOT NULL,
    quantity    DOUBLE PRECISION NOT NULL,
    fee         DOUBLE PRECISION DEFAULT 0.0,
    notes       TEXT DEFAULT '',
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trades_code ON trades(code);
CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(trade_date);

ALTER TABLE trades ENABLE ROW LEVEL SECURITY;
CREATE POLICY "allow_all" ON trades FOR ALL USING (true) WITH CHECK (true);
"""


def _normalize_supabase_url(url: str) -> str:
    url = url.rstrip("/")
    for suffix in ("/rest/v1", "/rest/v1/", "/rest", "/auth/v1"):
        if url.endswith(suffix):
            url = url[: -len(suffix)].rstrip("/")
    return url


def _to_app_trade(row: dict[str, Any], fallback_id: Any = None) -> dict[str, Any]:
    """把后端行（Supabase / JSON）转成应用内部 trade 结构。

    ``id`` 用于管理端「删除单条」。Supabase 自带自增 id；JSON 没有，
    由调用方传入列表下标作为稳定标识（加载—修改—整体写回，下标在单次会话内有效）。
    """
    side_raw = str(row.get("side", "buy")).upper()
    qty = row.get("qty", row.get("quantity", 0))
    return {
        "id": row.get("id", fallback_id),
        "date": str(row.get("date") or row.get("trade_date") or ""),
        "code": format_stock_code(str(row.get("code", ""))) or str(row.get("code", "")).strip(),
        "name": str(row.get("name") or ""),
        "side": "buy" if side_raw == "BUY" else "sell",
        "price": float(row.get("price") or 0),
        "qty": int(float(qty or 0)),
        "fee": float(row.get("fee") or 0),
        "notes": str(row.get("notes") or ""),
    }


#: 应用内部键名 → Supabase 列名
_APP_TO_DB_KEYS = {
    "date": "trade_date",
    "code": "code",
    "name": "name",
    "side": "side",
    "price": "price",
    "qty": "quantity",
    "fee": "fee",
    "notes": "notes",
}


def _partial_to_db(updates: dict[str, Any]) -> dict[str, Any]:
    """把**局部**更新字段从应用键名映射到 DB 列名。

    注意：不能用 :func:`_to_db_trade` —— 它会把未提供的字段补成空串/0，
    用于 UPDATE 会把该行其余列清空。这里只转换**显式传入**的键。
    """
    row: dict[str, Any] = {}
    for key, value in (updates or {}).items():
        if key not in _APP_TO_DB_KEYS:
            continue
        col = _APP_TO_DB_KEYS[key]
        if key == "code":
            row[col] = format_stock_code(str(value)) or str(value).strip()
        elif key == "side":
            row[col] = "BUY" if str(value).lower() == "buy" else "SELL"
        elif key in ("price", "qty", "fee"):
            try:
                row[col] = float(value)
            except (TypeError, ValueError):
                continue
        else:
            row[col] = str(value)
    return row


def _to_db_trade(trade: dict[str, Any]) -> dict[str, Any]:
    """应用内 trade → Supabase 行。

    ⚠️ 必须同时兼容两种键名：
    - **应用格式**（前端 POST /api/trades 等）：``date`` / ``qty`` / ``side=buy|sell``
    - **快照格式**（:func:`smcore.holdings_snapshot.build_snapshot_trades` 产出）：
      ``trade_date`` / ``quantity`` / ``side=BUY|SELL``

    历史教训（2026-08-29）：原实现只认应用格式，快照导入经 Supabase 落库时
    ``trade_date`` 写成空串、``quantity`` 写成 0 —— 用户持仓全部变成 0 股。
    """
    side_raw = str(trade.get("side", "buy")).lower()
    date_val = trade.get("date") if trade.get("date") is not None else trade.get("trade_date")
    qty_val = trade.get("qty")
    if qty_val is None:
        qty_val = trade.get("quantity")
    return {
        "trade_date": str(date_val or ""),
        "code": format_stock_code(str(trade.get("code", ""))) or str(trade.get("code", "")).strip(),
        "name": str(trade.get("name") or ""),
        "side": "BUY" if side_raw == "buy" else "SELL",
        "price": float(trade.get("price") or 0),
        "quantity": float(qty_val or 0),
        "fee": float(trade.get("fee") or 0),
        "notes": str(trade.get("notes") or ""),
    }


class TradeBackend(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def load_all(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def append(self, trade: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def replace_all(self, trades: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_by_id(self, trade_id: Any) -> bool:
        """按 id 删除单条；删除成功返回 True，id 不存在返回 False。"""
        raise NotImplementedError

    @abstractmethod
    def append_many(self, trades: list[dict[str, Any]]) -> int:
        """批量追加（一次往返写多条），返回成功写入条数。"""
        raise NotImplementedError

    @abstractmethod
    def update_by_id(self, trade_id: Any, updates: dict[str, Any]) -> bool:
        """按 id 局部更新字段（用于管理端修改持仓）。"""
        raise NotImplementedError


class JsonTradeBackend(TradeBackend):
    @property
    def name(self) -> str:
        return "json"

    def load_all(self) -> list[dict[str, Any]]:
        if not TRADES_FILE.exists():
            return []
        try:
            with TRADES_FILE.open("r", encoding="utf-8") as file_handle:
                data = json.load(file_handle)
            if not isinstance(data, list):
                return []
            # JSON 无自增 id，用列表下标充当稳定标识，供管理端删除单条
            return [
                _to_app_trade(item, idx)
                for idx, item in enumerate(data)
                if isinstance(item, dict)
            ]
        except Exception as exc:
            logger.warning("读取 trades.json 失败: %s", exc)
            return []

    def delete_by_id(self, trade_id: Any) -> bool:
        """按列表下标删除单条（仅对当前文件快照有效）。"""
        trades = self.load_all()
        try:
            idx = int(trade_id)
        except (TypeError, ValueError):
            return False
        if idx < 0 or idx >= len(trades):
            return False
        remaining = [t for i, t in enumerate(trades) if i != idx]
        for t in remaining:  # 剔除 id 字段，保持落盘格式干净
            t.pop("id", None)
        self.replace_all(remaining)
        return True

    def append_many(self, trades: list[dict[str, Any]]) -> int:
        """JSON 后端：读—拼接—整体写回。"""
        if not trades:
            return 0
        existing = self.load_all()
        for t in existing:
            t.pop("id", None)
        merged = existing + [_to_app_trade(t) for t in trades]
        for t in merged:
            t.pop("id", None)
        self.replace_all(merged)
        return len(trades)

    def update_by_id(self, trade_id: Any, updates: dict[str, Any]) -> bool:
        """按下标局部更新（JSON 后端用列表下标当 id）。"""
        trades = self.load_all()
        try:
            idx = int(trade_id)
        except (TypeError, ValueError):
            return False
        if idx < 0 or idx >= len(trades):
            return False
        for key, value in (updates or {}).items():
            trades[idx][key] = value
        for t in trades:
            t.pop("id", None)
        self.replace_all(trades)
        return True

    def append(self, trade: dict[str, Any]) -> None:
        trades = self.load_all()
        trades.append(_to_app_trade(trade))
        self.replace_all(trades)

    def replace_all(self, trades: list[dict[str, Any]]) -> None:
        TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)
        normalized = [_to_app_trade(item) for item in trades]
        fd, tmp_path = tempfile.mkstemp(dir=TRADES_FILE.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file_handle:
                json.dump(normalized, file_handle, ensure_ascii=False, indent=2)
            os.replace(tmp_path, TRADES_FILE)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


class SupabaseTradeBackend(TradeBackend):
    def __init__(self) -> None:
        self._client = self._create_client()
        if self._client is None:
            raise RuntimeError("Supabase 未配置或客户端创建失败")

    @property
    def name(self) -> str:
        return "supabase"

    @staticmethod
    def _create_client():
        try:
            from supabase import create_client
        except ImportError:
            logger.warning("未安装 supabase 包，无法使用云端存储")
            return None

        url = os.getenv("SUPABASE_URL", "").strip()
        key = os.getenv("SUPABASE_KEY", "").strip()
        if not url or not key:
            return None

        try:
            return create_client(_normalize_supabase_url(url), key)
        except Exception as exc:
            logger.warning("Supabase 客户端创建失败: %s", exc)
            return None

    def load_all(self) -> list[dict[str, Any]]:
        resp = (
            self._client.table("trades")
            .select("*")
            .order("trade_date")
            .order("id")
            .limit(10000)
            .execute()
        )
        rows = resp.data or []
        return [_to_app_trade(row) for row in rows]

    def append(self, trade: dict[str, Any]) -> None:
        row = _to_db_trade(trade)
        self._client.table("trades").insert(row).execute()

    def replace_all(self, trades: list[dict[str, Any]]) -> None:
        self._client.table("trades").delete().neq("id", -1).execute()
        if not trades:
            return
        rows = [_to_db_trade(item) for item in trades]
        self._client.table("trades").insert(rows).execute()

    def delete_by_id(self, trade_id: Any) -> bool:
        """按自增 id 删除单条。"""
        try:
            tid = int(trade_id)
        except (TypeError, ValueError):
            return False
        resp = self._client.table("trades").delete().eq("id", tid).execute()
        return bool(resp.data)

    def append_many(self, trades: list[dict[str, Any]]) -> int:
        """Supabase 后端：单次 insert 写多条。"""
        if not trades:
            return 0
        rows = [_to_db_trade(t) for t in trades]
        resp = self._client.table("trades").insert(rows).execute()
        return len(resp.data or [])

    def update_by_id(self, trade_id: Any, updates: dict[str, Any]) -> bool:
        """按自增 id 局部更新字段。updates 用应用内部键名（date/price/qty/side...）。"""
        try:
            tid = int(trade_id)
        except (TypeError, ValueError):
            return False
        # 只更新显式传入的字段（_partial_to_db 不会补全其余列，避免清空数据）
        row = _partial_to_db(updates)
        if not row:
            return False
        resp = self._client.table("trades").update(row).eq("id", tid).execute()
        return bool(resp.data)


class TradeRepository:
    """Unified trade storage with optional Supabase + JSON fallback."""

    def __init__(self) -> None:
        self._backend: TradeBackend = self._resolve_backend()
        self._fallback = JsonTradeBackend()
        self._maybe_migrate_json_to_cloud()

    @property
    def backend_name(self) -> str:
        return self._backend.name

    @property
    def using_fallback(self) -> bool:
        return isinstance(self._backend, JsonTradeBackend) and self._supabase_requested()

    def _supabase_requested(self) -> bool:
        mode = os.getenv("TRADES_BACKEND", "auto").strip().lower()
        if mode == "supabase":
            return True
        if mode == "json":
            return False
        return bool(os.getenv("SUPABASE_URL", "").strip() and os.getenv("SUPABASE_KEY", "").strip())

    def _resolve_backend(self) -> TradeBackend:
        mode = os.getenv("TRADES_BACKEND", "auto").strip().lower()

        if mode == "json":
            return JsonTradeBackend()

        if mode in ("supabase", "auto"):
            try:
                return SupabaseTradeBackend()
            except RuntimeError:
                if mode == "supabase":
                    logger.warning("TRADES_BACKEND=supabase 但连接失败，回退到 trades.json")
                return JsonTradeBackend()

        logger.warning("未知 TRADES_BACKEND=%s，使用 json", mode)
        return JsonTradeBackend()

    def _maybe_migrate_json_to_cloud(self) -> None:
        if not isinstance(self._backend, SupabaseTradeBackend):
            return
        if os.getenv("TRADES_MIGRATE_JSON", "1").strip() == "0":
            return
        try:
            cloud_trades = self._backend.load_all()
            if cloud_trades:
                return
            local_trades = self._fallback.load_all()
            if not local_trades:
                return
            self._backend.replace_all(local_trades)
            logger.info("已将 %d 条本地交易迁移到 Supabase", len(local_trades))
        except Exception as exc:
            logger.warning("JSON → Supabase 迁移失败: %s", exc)

    def load_all(self) -> list[dict[str, Any]]:
        try:
            return self._backend.load_all()
        except Exception as exc:
            logger.warning("%s 读取失败，回退 trades.json: %s", self._backend.name, exc)
            return self._fallback.load_all()

    def append(self, trade: dict[str, Any]) -> None:
        try:
            self._backend.append(trade)
        except Exception as exc:
            logger.warning("%s 写入失败，回退 trades.json: %s", self._backend.name, exc)
            self._fallback.append(trade)

    def replace_all(self, trades: list[dict[str, Any]]) -> None:
        try:
            self._backend.replace_all(trades)
        except Exception as exc:
            logger.warning("%s 覆盖写入失败，回退 trades.json: %s", self._backend.name, exc)
            self._fallback.replace_all(trades)

    def delete_by_id(self, trade_id: Any) -> bool:
        """按 id 删除单条。Supabase 后端失败时不回退 JSON（避免误删本地数据）。"""
        try:
            return bool(self._backend.delete_by_id(trade_id))
        except Exception as exc:
            logger.warning("%s 删除单条失败: %s", self._backend.name, exc)
            return False

    def append_many(self, trades: list[dict[str, Any]]) -> int:
        """批量追加；云端失败时回退本地 JSON，保证录入不丢。"""
        if not trades:
            return 0
        try:
            return int(self._backend.append_many(trades))
        except Exception as exc:
            logger.warning("%s 批量写入失败，回退 trades.json: %s", self._backend.name, exc)
            return int(self._fallback.append_many(trades))

    def update_by_id(self, trade_id: Any, updates: dict[str, Any]) -> bool:
        """按 id 局部更新。云端失败不回退 JSON（避免改到本地副本造成两边不一致）。"""
        try:
            return bool(self._backend.update_by_id(trade_id, updates))
        except Exception as exc:
            logger.warning("%s 更新单条失败: %s", self._backend.name, exc)
            return False


_repo: TradeRepository | None = None


def get_trade_repository() -> TradeRepository:
    global _repo
    if _repo is None:
        _repo = TradeRepository()
    return _repo
