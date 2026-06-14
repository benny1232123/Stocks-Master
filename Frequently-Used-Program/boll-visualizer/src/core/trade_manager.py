"""交易记录管理器 — SQLite CRUD + CSV 兼容。"""

from __future__ import annotations

import csv
import io
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

import pandas as pd

from utils.config import STOCK_DATA_DIR, CSV_ENCODING

_DB_PATH = STOCK_DATA_DIR / "trading.db"

_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date  TEXT    NOT NULL,
    code        TEXT    NOT NULL,
    name        TEXT    DEFAULT '',
    side        TEXT    NOT NULL CHECK (side IN ('BUY','SELL')),
    price       REAL    NOT NULL,
    quantity    REAL    NOT NULL,
    fee         REAL    DEFAULT 0.0,
    notes       TEXT    DEFAULT '',
    created_at  TEXT    DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_trades_code ON trades(code);
CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(trade_date);
"""


class TradeManager:
    """轻量 SQLite 交易记录管理。"""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or _DB_PATH
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ── connection ──────────────────────────────────────────────

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(_CREATE_TABLE)

    # ── CRUD ────────────────────────────────────────────────────

    def add_trade(
        self,
        trade_date: str | date,
        code: str,
        side: str,
        price: float,
        quantity: float,
        fee: float = 0.0,
        name: str = "",
        notes: str = "",
    ) -> int:
        """插入一条交易记录，返回新行 id。"""
        code = _normalize_code(code)
        side = side.upper().strip()
        if side not in ("BUY", "SELL"):
            raise ValueError(f"side 必须为 BUY 或 SELL，实际: {side}")
        if isinstance(trade_date, date):
            trade_date = trade_date.strftime("%Y-%m-%d")
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO trades (trade_date, code, name, side, price, quantity, fee, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (trade_date, code, name, side, price, quantity, fee, notes),
            )
            return cur.lastrowid  # type: ignore[return-value]

    def delete_trade(self, trade_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
            return cur.rowcount > 0

    def get_trades(
        self,
        code: str | None = None,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        """查询交易记录，返回与 backtest_tradebook 兼容的 DataFrame。"""
        clauses: list[str] = []
        params: list[object] = []

        if code:
            clauses.append("code = ?")
            params.append(_normalize_code(code))
        if start_date:
            if isinstance(start_date, date):
                start_date = start_date.strftime("%Y-%m-%d")
            clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            if isinstance(end_date, date):
                end_date = end_date.strftime("%Y-%m-%d")
            clauses.append("trade_date <= ?")
            params.append(end_date)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            f"SELECT id, trade_date, code, name, side, price, quantity, fee, notes, created_at "
            f"FROM trades {where} ORDER BY trade_date, code, id LIMIT ?"
        )
        params.append(limit)

        with self._conn() as conn:
            df = pd.read_sql_query(sql, conn, params=params)

        if not df.empty:
            df = df.rename(columns={"trade_date": "date", "name": "stock_name"})
        return df

    def get_all_codes(self) -> list[str]:
        """返回所有出现过的股票代码。"""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT code FROM trades ORDER BY code"
            ).fetchall()
        return [r["code"] for r in rows]

    def get_trades_for_fifo(self) -> pd.DataFrame:
        """返回 FIFO 匹配所需的标准化 DataFrame（与 backtest_tradebook 兼容）。"""
        df = self.get_trades(limit=999_999)
        if df.empty:
            return pd.DataFrame(columns=["date", "code", "side", "price", "quantity", "fee"])
        return df[["date", "code", "side", "price", "quantity", "fee"]].sort_values(
            ["date", "code"]
        ).reset_index(drop=True)

    # ── CSV 导入 / 导出 ─────────────────────────────────────────

    def import_csv(self, csv_bytes: bytes) -> tuple[int, int]:
        """从 CSV 批量导入交易。返回 (成功数, 跳过数)。兼容 backtest_tradebook 列名。"""
        buf = io.BytesIO(csv_bytes)
        df = pd.read_csv(buf, encoding=CSV_ENCODING)
        if df.empty:
            return 0, 0

        # 列名匹配（复用 backtest_tradebook 的候选列表）
        date_col = _pick_column(df, ["date", "trade_date", "日期", "成交日期", "交易日期"])
        code_col = _pick_column(df, ["code", "股票代码", "symbol", "证券代码"])
        side_col = _pick_column(df, ["side", "方向", "action", "买卖", "交易方向"])
        price_col = _pick_column(df, ["price", "成交价", "成交均价", "均价", "trade_price"])
        qty_col = _pick_column(df, ["quantity", "数量", "成交数量", "成交股数", "volume"], required=False)
        fee_col = _pick_column(df, ["fee", "手续费", "佣金", "费用", "cost"], required=False)
        name_col = _pick_column(df, ["name", "股票名称", "名称", "stock_name"], required=False)
        notes_col = _pick_column(df, ["notes", "备注", "note", "remark"], required=False)

        imported = 0
        skipped = 0
        for _, row in df.iterrows():
            try:
                d = pd.to_datetime(row[date_col], errors="coerce")
                if pd.isna(d):
                    skipped += 1
                    continue
                c = _normalize_code(row[code_col])
                s = _normalize_side(row[side_col])
                p = float(row[price_col])
                q = float(row[qty_col]) if qty_col and pd.notna(row[qty_col]) else 1.0
                f = float(row[fee_col]) if fee_col and pd.notna(row[fee_col]) else 0.0
                n = str(row[name_col]) if name_col and pd.notna(row[name_col]) else ""
                nt = str(row[notes_col]) if notes_col and pd.notna(row[notes_col]) else ""
                if s not in ("BUY", "SELL") or q <= 0:
                    skipped += 1
                    continue
                self.add_trade(
                    trade_date=d.strftime("%Y-%m-%d"),
                    code=c, side=s, price=p, quantity=q, fee=f, name=n, notes=nt,
                )
                imported += 1
            except Exception:
                skipped += 1
        return imported, skipped

    def export_csv(self, codes: list[str] | None = None) -> bytes:
        """导出为兼容 backtest_tradebook 的 CSV（UTF-8 BOM）。"""
        if codes:
            frames = [self.get_trades(code=c, limit=999_999) for c in codes]
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        else:
            df = self.get_trades(limit=999_999)
        if df.empty:
            buf = io.BytesIO()
            buf.write("date,code,side,price,quantity,fee,notes\n".encode(CSV_ENCODING))
            return buf.getvalue()

        export_cols = {
            "date": "日期",
            "code": "股票代码",
            "side": "方向",
            "price": "成交价",
            "quantity": "数量",
            "fee": "手续费",
            "notes": "备注",
        }
        out = df[list(export_cols.keys())].rename(columns=export_cols)
        return out.to_csv(index=False, encoding=CSV_ENCODING).encode(CSV_ENCODING)


# ── helpers ─────────────────────────────────────────────────────


def _normalize_code(value: object) -> str:
    text = str(value or "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text.strip()


def _normalize_side(value: object) -> str:
    txt = str(value or "").strip().lower()
    if txt in {"buy", "b", "long", "买", "买入"}:
        return "BUY"
    if txt in {"sell", "s", "short", "卖", "卖出"}:
        return "SELL"
    return ""


def _pick_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str:
    lower_map = {str(col).strip().lower(): col for col in df.columns}
    for c in candidates:
        key = c.strip().lower()
        if key in lower_map:
            return str(lower_map[key])
    if required:
        raise ValueError(f"缺少列，候选: {candidates}")
    return ""
