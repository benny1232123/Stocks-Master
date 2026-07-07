"""Strategy result cache backed by Supabase.

Usage:
    # Check cache before running a strategy
    python scripts/strategy_cache.py pull <strategy> <date>

    # Push result after running a strategy
    python scripts/strategy_cache.py push <strategy> <date> <csv_path>

Strategies: boll, theme, cctv, relativity
Date format: YYYYMMDD
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STOCK_DATA = ROOT / "stock_data"

STRATEGY_PATTERNS = {
    "boll": "Stock-Selection-Boll-{date}.csv",
    "theme": "Stock-Selection-Ashare-Theme-Turnover-{date}.csv",
    "cctv": "CCTV-Sector-Stock-Pool-{date}.csv",
    "relativity": "Stock-Selection-Relativity-{date}.csv",
}

CACHE_TABLE = "strategy_cache"

SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS strategy_cache (
    id          BIGSERIAL PRIMARY KEY,
    strategy    TEXT NOT NULL,
    trade_date  TEXT NOT NULL,
    csv_content TEXT NOT NULL,
    row_count   INTEGER DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE(strategy, trade_date)
);
ALTER TABLE strategy_cache ENABLE ROW LEVEL SECURITY;
CREATE POLICY "allow_all" ON strategy_cache FOR ALL USING (true) WITH CHECK (true);
"""


def _load_env_file():
    """手动加载项目根目录 .env 中的 SUPABASE 配置（无外部依赖）。
    仅在对应环境变量尚未设置时填充，避免覆盖 GitHub Actions 注入的 secrets。"""
    import os

    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    try:
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key, val = key.strip(), val.strip().strip('"').strip("'")
                if key in ("SUPABASE_URL", "SUPABASE_KEY") and not os.getenv(key):
                    os.environ[key] = val
    except Exception:
        pass


def _get_client():
    import os

    _load_env_file()
    try:
        from supabase import create_client
    except ImportError:
        return None
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()
    if not url or not key:
        return None
    try:
        url = url.rstrip("/")
        for suffix in ("/rest/v1", "/rest/v1/", "/rest", "/auth/v1"):
            if url.endswith(suffix):
                url = url[: -len(suffix)].rstrip("/")
        return create_client(url, key)
    except Exception:
        return None


def pull(strategy: str, date: str) -> bool:
    client = _get_client()
    if client is None:
        print(f"[cache] Supabase 未配置，跳过 pull {strategy}")
        return False

    try:
        resp = (
            client.table(CACHE_TABLE)
            .select("csv_content")
            .eq("strategy", strategy)
            .eq("trade_date", date)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            print(f"[cache] {strategy}/{date} 无缓存")
            return False

        pattern = STRATEGY_PATTERNS[strategy]
        filename = pattern.format(date=date)
        out_path = STOCK_DATA / filename
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rows[0]["csv_content"], encoding="utf-8-sig")
        print(f"[cache] {strategy}/{date} 已从云端恢复 -> {filename}")
        return True
    except Exception as exc:
        print(f"[cache] pull {strategy} 失败: {exc}")
        return False


def push(strategy: str, date: str, csv_path: str) -> bool:
    client = _get_client()
    if client is None:
        print(f"[cache] Supabase 未配置，跳过 push {strategy}")
        return False

    path = Path(csv_path)
    if not path.exists():
        print(f"[cache] {csv_path} 不存在，跳过 push")
        return False

    content = path.read_text(encoding="utf-8-sig")
    row_count = max(0, content.count("\n") - 1)

    try:
        client.table(CACHE_TABLE).upsert(
            {
                "strategy": strategy,
                "trade_date": date,
                "csv_content": content,
                "row_count": row_count,
            },
            on_conflict="strategy,trade_date",
        ).execute()
        print(f"[cache] {strategy}/{date} 已上传云端 ({row_count} rows)")
        return True
    except Exception as exc:
        print(f"[cache] push {strategy} 失败: {exc}")
        return False


def main():
    if len(sys.argv) < 3:
        print("Usage: strategy_cache.py <pull|push> <strategy> <date> [csv_path]")
        sys.exit(1)

    action = sys.argv[1]
    strategy = sys.argv[2]
    date = sys.argv[3] if len(sys.argv) > 3 else ""

    if strategy not in STRATEGY_PATTERNS:
        print(f"Unknown strategy: {strategy}. Choose from: {', '.join(STRATEGY_PATTERNS)}")
        sys.exit(1)

    if action == "pull":
        ok = pull(strategy, date)
        sys.exit(0 if ok else 1)
    elif action == "push":
        csv_path = sys.argv[4] if len(sys.argv) > 4 else str(STOCK_DATA / STRATEGY_PATTERNS[strategy].format(date=date))
        ok = push(strategy, date, csv_path)
        sys.exit(0 if ok else 1)
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)


if __name__ == "__main__":
    main()
