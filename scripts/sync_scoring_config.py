"""把后端 RECOMMENDATION_CONFIG 同步成前端兜底快照。

后端 ``smcore/config/defaults.py::RECOMMENDATION_CONFIG`` 是持仓建议三维评分的
**唯一真源**；前端 ``frontend/src/config/scoringConfig.js`` 只是后端不可达时的兜底
副本。改了后端配置后跑本脚本，避免前后端口径漂移。

用法::

    python scripts/sync_scoring_config.py            # 写入（有变更才写）
    python scripts/sync_scoring_config.py --check    # 只检查是否漂移（CI 用，漂移则退出码 1）
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OUT = ROOT / "frontend" / "src" / "config" / "scoringConfig.js"

HEADER = """// 持仓建议三维评分配置 —— 由 smcore/config/defaults.py 的 RECOMMENDATION_CONFIG 生成。
//
// ⚠️ 单一真源：评分阈值与权重的真源在**后端**（smcore/config/defaults.py）。
// 本文件只是「后端不可达时的兜底快照」，由 scripts/sync_scoring_config.py 同步生成，
// 请勿手工编辑。运行时 useScoringConfig() 会拉取 /api/config/recommendation 覆盖它。
//
// 背景（2026-09-09）：此前 App.jsx 里硬编码复刻了整套分段阈值与 0.40/0.35/0.25 权重，
// 与后端各写一份，只能靠 verify_panel_alignment.py 事后校验，改一处漏一处。

export const DEFAULT_SCORING_CONFIG = %s

export default DEFAULT_SCORING_CONFIG
"""


def render() -> str:
    from smcore.config.defaults import RECOMMENDATION_CONFIG

    body = json.dumps(RECOMMENDATION_CONFIG, ensure_ascii=False, indent=2)
    return HEADER % body


def main() -> int:
    ap = argparse.ArgumentParser(description="同步评分配置到前端")
    ap.add_argument("--check", action="store_true", help="只检查是否漂移，不写文件")
    args = ap.parse_args()

    want = render()
    have = OUT.read_text(encoding="utf-8") if OUT.exists() else None

    if have == want:
        print(f"✓ 前端评分配置与后端一致：{OUT.relative_to(ROOT).as_posix()}")
        return 0

    if args.check:
        print(
            "✗ 前端评分配置已漂移，请运行：python scripts/sync_scoring_config.py",
            file=sys.stderr,
        )
        return 1

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(want, encoding="utf-8")
    print(f"✓ 已同步 {OUT.relative_to(ROOT).as_posix()}（{len(want)} 字节）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
