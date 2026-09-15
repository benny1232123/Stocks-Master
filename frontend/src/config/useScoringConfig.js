import { useEffect, useState } from 'react'
import { DEFAULT_SCORING_CONFIG } from './scoringConfig'

// 持仓建议三维评分配置的运行时获取。
//
// 真源在后端（/api/config/recommendation ← smcore/config/defaults.py）。
// 启动时拉一次；失败或后端未部署时回退到打包进来的 DEFAULT_SCORING_CONFIG 快照，
// 保证面板在任何情况下都能渲染（不会因为配置拉不到而白屏）。

let cached = null
let inflight = null

export function fetchScoringConfig() {
  if (cached) return Promise.resolve(cached)
  if (inflight) return inflight
  inflight = fetch('/api/config/recommendation')
    .then((r) => (r.ok ? r.json() : null))
    .then((j) => {
      // 只有结构完整的配置才覆盖默认值，避免后端返回半截配置把面板算崩
      cached = j && j.face_weights && j.technical && j.fundamental && j.capital ? j : DEFAULT_SCORING_CONFIG
      return cached
    })
    .catch(() => DEFAULT_SCORING_CONFIG)
    .finally(() => {
      inflight = null
    })
  return inflight
}

export function useScoringConfig() {
  const [cfg, setCfg] = useState(cached || DEFAULT_SCORING_CONFIG)
  useEffect(() => {
    let alive = true
    fetchScoringConfig().then((c) => {
      if (alive && c) setCfg(c)
    })
    return () => {
      alive = false
    }
  }, [])
  return cfg
}

// ── 配置驱动的打分工具（替代 App.jsx 里硬编码的分段阈值）──

/** 按有序分段表取值：支持 {gt}/{gte}/{lt}/{lte}，first-match-wins。 */
export function bandScore(table, value, missing) {
  if (value == null || Number.isNaN(Number(value))) return missing
  const v = Number(value)
  for (const row of table || []) {
    if (row.gt != null && !(v > row.gt)) continue
    if (row.gte != null && !(v >= row.gte)) continue
    if (row.lt != null && !(v < row.lt)) continue
    if (row.lte != null && !(v <= row.lte)) continue
    return row.score
  }
  return missing
}

export function bandLabel(table, value, fallback = '中性') {
  if (value == null || Number.isNaN(Number(value))) return fallback
  const v = Number(value)
  for (const row of table || []) {
    if (row.gt != null && !(v > row.gt)) continue
    if (row.gte != null && !(v >= row.gte)) continue
    if (row.lt != null && !(v < row.lt)) continue
    if (row.lte != null && !(v <= row.lte)) continue
    return row.label || fallback
  }
  return fallback
}

// ── ROE 年化（累计口径 → 年度可比）──
// 后端 fundamental.roe 的分段阈值（0.10/0.15/0.20）是按**年度** ROE 设的，而 THS/baostock
// 财报披露的是**年初至今累计**（Q1/Q2/Q3 累计，单调递增）→ 直接比阈值会让面分随报告日历漂移
// （同一只票 Q1 落「偏低」、年报落「良好」）。
// 与 smcore/strategy/fundamental.annualize_roe 严格同构：Q1×4 / Q2×2 / Q3×4/3 / Q4×1。
// period 缺失（旧 v1 扁平缓存，其值本就是年度）→ 原样返回，不猜。
const ROE_ANNUALIZE_MULT = { '03-31': 4, '06-30': 2, '09-30': 4 / 3, '12-31': 1 }

export function annualizeRoe(roe, period) {
  if (roe == null || !period) return roe
  const mult = ROE_ANNUALIZE_MULT[String(period).slice(-5)]
  return mult == null ? roe : Number(roe) * mult
}

export default useScoringConfig
