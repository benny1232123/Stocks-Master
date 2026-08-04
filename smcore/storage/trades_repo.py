"""Trade persistence — JSON (local) or Supabase (cloud) backends.

Environment variables:
- TRADES_BACKEND: ``json`` | ``supabase`` | ``auto`` (default ``auto``)
- SUPABASE_URL / SUPABASE_KEY: enable Supabase when set
"""
from __future__ import annotations

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


def _to_app_trade(row: dict[str, Any]) -> dict[str, Any]:
    side_raw = str(row.get("side", "buy")).upper()
    qty = row.get("qty", row.get("quantity", 0))
    return {
        "date": str(row.get("date") or row.get("trade_date") or ""),
        "code": format_stock_code(str(row.get("code", ""))) or str(row.get("code", "")).strip(),
        "name": str(row.get("name") or ""),
        "side": "buy" if side_raw == "BUY" else "sell",
        "price": float(row.get("price") or 0),
        "qty": int(float(qty or 0)),
        "fee": float(row.get("fee") or 0),
        "notes": str(row.get("notes") or ""),
    }


def _to_db_trade(trade: dict[str, Any]) -> dict[str, Any]:
    side = str(trade.get("side", "buy")).lower()
    return {
        "trade_date": str(trade.get("date") or ""),
        "code": format_stock_code(str(trade.get("code", ""))) or str(trade.get("code", "")).strip(),
        "name": str(trade.get("name") or ""),
        "side": "BUY" if side == "buy" else "SELL",
        "price": float(trade.get("price") or 0),
        "quantity": float(trade.get("qty") or 0),
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
            return [_to_app_trade(item) for item in data if isinstance(item, dict)]
        except Exception as exc:
            logger.warning("读取 trades.json 失败: %s", exc)
            return []

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


_repo: TradeRepository | None = None


def get_trade_repository() -> TradeRepository:
    global _repo
    if _repo is None:
        _repo = TradeRepository()
    return _repo
