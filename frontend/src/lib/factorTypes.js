// 策略 → 因子类型 映射（前端展示副本）
//
// ⚠️ 单一真源在后端：smcore/strategy/factor_types.py。
// 改这里前请先改后端并同步——两处必须一致，否则报告/监控层与前端分类会漂移。
//
// 设计原则（与后端一致，用户 2026-09-15 要求「按因子类型分类」）：
// - 每策略归一个主因子类型（1:1），保证归并桶清晰、无歧义；
// - 一个标的可命中多策略 → 可映射到多个因子类型；
// - 因子类型顺序 / 配色与既有策略认知一致，便于阅读。
//
// 2026-09-15 修订：原「题材·事件」拆为「题材」(Theme) 与「事件·舆情」(CCTV) 两类，
// 去掉合并掩盖（Theme 实为拖累、CCTV 才是正贡献）。

export const STRATEGY_FACTOR_TYPE = {
  boll: '反转·均值回归',
  momentum: '动量',
  relativity: '相对强度·资金流',
  theme: '题材',
  cctv: '事件·舆情',
}

export const FACTOR_TYPE_ORDER = [
  '动量',
  '反转·均值回归',
  '相对强度·资金流',
  '题材',
  '事件·舆情',
  '其他',
]

const _DEFAULT_TYPE = '其他'

function _normalize(name) {
  return String(name ?? '').trim().toLowerCase()
}

// 来源策略串（如 'Boll/Momentum' / 'cctv' / 'Boll、Momentum'）→ 去重后的因子类型列表。
// 用于明细展示与贡献度统计：一个 pick 可能命中多个策略，故可能返回多个类型。
export function factorTypesOfSource(source) {
  if (!source) return [_DEFAULT_TYPE]
  const out = []
  for (const part of String(source).replace(/[、,]/g, '/').split('/')) {
    const k = _normalize(part)
    if (!k) continue
    const t = STRATEGY_FACTOR_TYPE[k] || _DEFAULT_TYPE
    if (!out.includes(t)) out.push(t)
  }
  return out.length ? out : [_DEFAULT_TYPE]
}

// 因子类型配色（与策略配色风格一致，用于前端彩色标签 / 左边框 / 分布条）
export const FACTOR_TYPE_COLORS = {
  '动量': { bg: 'hsla(217, 91%, 60%, 0.10)', text: '#3B82F6', border: 'hsla(217, 91%, 60%, 0.35)' },
  '反转·均值回归': { bg: 'hsla(229, 87%, 56%, 0.10)', text: '#6366F1', border: 'hsla(229, 87%, 56%, 0.35)' },
  '相对强度·资金流': { bg: 'hsla(157, 81%, 37%, 0.09)', text: '#30A46C', border: 'hsla(157, 81%, 37%, 0.35)' },
  '题材': { bg: 'hsla(3, 80%, 50%, 0.08)', text: '#E5484D', border: 'hsla(3, 80%, 50%, 0.35)' },
  '事件·舆情': { bg: 'hsla(28, 90%, 50%, 0.10)', text: '#F5861F', border: 'hsla(28, 90%, 50%, 0.35)' },
  '其他': { bg: 'hsl(var(--surface-2))', text: 'hsl(var(--muted))', border: 'hsl(var(--border))' },
}

export function getFactorTypeColor(t) {
  return FACTOR_TYPE_COLORS[t] || FACTOR_TYPE_COLORS['其他']
}
