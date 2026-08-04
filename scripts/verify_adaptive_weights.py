"""验证自适应权重：确认 shrinkage 修复后 CCTV（n=1）不再被放大到 76%。"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smcore.strategy.adaptive_weights import (
    compute_strategy_edge,
    compute_adaptive_allocation,
)

print("=" * 70)
print("STEP 1: 各策略近期 edge（归因到来源策略的前向已实现收益）")
print("=" * 70)
edge = compute_strategy_edge(30)
for s in ["boll", "theme", "relativity", "momentum", "cctv"]:
    e = edge.get(s, {})
    n = e.get("n", 0)
    avg = e.get("avg_return")
    win = e.get("win_rate")
    ed = e.get("edge", 0.0)
    print(f"  {s:10s} n={n:4d}  avg_ret={avg}  win%={win}  edge={round(ed,3)}")

print()
print("=" * 70)
print("STEP 2: 自适应权重（含经验贝叶斯收缩 pseudo=15）")
print("=" * 70)
edge, weights, cash, cold = compute_adaptive_allocation()
for s in ["boll", "theme", "relativity", "momentum", "cctv"]:
    print(f"  {s:10s} {weights.get(s, 0)}%")
print(f"  {'现金':10s} {cash}%")
print(f"  冷启动={cold}  合计={sum(weights.values())}%")

print()
cctv_w = weights.get("cctv", 0)
if cctv_w > 40:
    print(f"❌ 失败：CCTV 仍被放大到 {cctv_w}%（n 很少却主导）")
    sys.exit(1)
else:
    print(f"✅ 通过：CCTV 权重={cctv_w}%（不再主导），权重分布随业绩此消彼长")
