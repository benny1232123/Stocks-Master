// 策略 → 因子类型 映射（前端展示副本）
//
// ⚠️ 单一真源在后端：smcore/strategy/factor_types.py。
// 改这里前请先改后端并同步——两处必须一致，否则报告/监控层与前端分类会漂移。
// 一致性由 tests/test_frontend_registry_parity.py 守卫（键集合 + 类型串逐一比对），
// 漂移会直接红灯，不再靠人眼发现。
//
// 设计原则（与后端一致，用户 2026-09-15 要求「按因子类型分类」）：
// - 每策略归一个主因子类型（1:1），保证归并桶清晰、无歧义；
// - 一个标的可命中多策略 → 可映射到多个因子类型；
// - 因子类型顺序 / 配色与既有策略认知一致，便于阅读。
//
// 2026-09-15 修订：原「题材·事件」拆为「题材」(Theme) 与「事件·舆情」(CCTV) 两类，
// 去掉合并掩盖（Theme 实为拖累、CCTV 才是正贡献）。
// 2026-09-17 修订：补上漂移两代的 9 个策略 —— 基本面三因子（质量/估值/规模）与
// boll/relativity 沿价格轴拆出的 6 个原子（超卖/近下轨/中轨回踩/带宽收口、
// 上涨满足率/抗跌满足率）。此前它们全部落到「其他」灰色兜底。

export const STRATEGY_FACTOR_TYPE = {
  boll: '反转·均值回归',
  momentum: '动量',
  relativity: '相对强度·资金流',
  theme: '题材',
  cctv: '事件·舆情',
  quality: '基本面·质量',
  value: '基本面·估值',
  size: '基本面·规模',
  boll_oversold: '反转·超卖',
  boll_near_lower: '反转·近下轨',
  boll_mid_pullback: '反转·中轨回踩',
  boll_squeeze: '反转·带宽收口',
  rel_up: '相对强度·上涨满足率',
  rel_down: '相对强度·抗跌满足率',
}

export const FACTOR_TYPE_ORDER = [
  '动量',
  '反转·均值回归',
  '反转·超卖',
  '反转·近下轨',
  '反转·中轨回踩',
  '反转·带宽收口',
  '相对强度·资金流',
  '相对强度·上涨满足率',
  '相对强度·抗跌满足率',
  '题材',
  '事件·舆情',
  '基本面·质量',
  '基本面·估值',
  '基本面·规模',
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

// 因子「家族」= 类型串中「·」之前的部分（如 '相对强度·抗跌满足率' → '相对强度'）。
// 拆分后同一族会有多个类型串，调用方**不要**再按完整串精确匹配。
export function typeFamilyOf(type) {
  const t = String(type ?? '')
  const i = t.indexOf('·')
  return i >= 0 ? t.slice(0, i) : t
}

// 来源策略串是否命中某家族（'相对强度' / '反转' / '基本面' …）。
// 后端 fusion 趋势闸门用的是同一套前缀归并，前后端口径保持一致。
export function hasTypeFamily(source, family) {
  const f = String(family ?? '')
  if (!f) return false
  return factorTypesOfSource(source).some((t) => t === f || t.startsWith(`${f}·`))
}

// 因子类型配色（与策略配色风格一致，用于前端彩色标签 / 左边框 / 分布条）
// 同族用同色系的不同明度，便于一眼区分「复合体」与「其原子」。
export const FACTOR_TYPE_COLORS = {
  '动量': { bg: 'hsla(217, 91%, 60%, 0.10)', text: '#3B82F6', border: 'hsla(217, 91%, 60%, 0.35)' },
  '反转·均值回归': { bg: 'hsla(229, 87%, 56%, 0.10)', text: '#6366F1', border: 'hsla(229, 87%, 56%, 0.35)' },
  '反转·超卖': { bg: 'hsla(258, 90%, 66%, 0.10)', text: '#8B5CF6', border: 'hsla(258, 90%, 66%, 0.35)' },
  '反转·近下轨': { bg: 'hsla(262, 83%, 58%, 0.10)', text: '#7C3AED', border: 'hsla(262, 83%, 58%, 0.35)' },
  '反转·中轨回踩': { bg: 'hsla(255, 92%, 76%, 0.10)', text: '#A78BFA', border: 'hsla(255, 92%, 76%, 0.35)' },
  '反转·带宽收口': { bg: 'hsla(244, 75%, 57%, 0.10)', text: '#4F46E5', border: 'hsla(244, 75%, 57%, 0.35)' },
  '相对强度·资金流': { bg: 'hsla(157, 81%, 37%, 0.09)', text: '#30A46C', border: 'hsla(157, 81%, 37%, 0.35)' },
  '相对强度·上涨满足率': { bg: 'hsla(160, 84%, 39%, 0.09)', text: '#10B981', border: 'hsla(160, 84%, 39%, 0.35)' },
  '相对强度·抗跌满足率': { bg: 'hsla(161, 94%, 30%, 0.09)', text: '#059669', border: 'hsla(161, 94%, 30%, 0.35)' },
  '题材': { bg: 'hsla(3, 80%, 50%, 0.08)', text: '#E5484D', border: 'hsla(3, 80%, 50%, 0.35)' },
  '事件·舆情': { bg: 'hsla(28, 90%, 50%, 0.10)', text: '#F5861F', border: 'hsla(28, 90%, 50%, 0.35)' },
  '基本面·质量': { bg: 'hsla(199, 89%, 48%, 0.10)', text: '#0EA5E9', border: 'hsla(199, 89%, 48%, 0.35)' },
  '基本面·估值': { bg: 'hsla(173, 80%, 40%, 0.10)', text: '#14B8A6', border: 'hsla(173, 80%, 40%, 0.35)' },
  '基本面·规模': { bg: 'hsla(215, 16%, 47%, 0.10)', text: '#64748B', border: 'hsla(215, 16%, 47%, 0.35)' },
  '其他': { bg: 'hsl(var(--surface-2))', text: 'hsl(var(--muted))', border: 'hsl(var(--border))' },
}

export function getFactorTypeColor(t) {
  return FACTOR_TYPE_COLORS[t] || FACTOR_TYPE_COLORS['其他']
}
