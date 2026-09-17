// 策略 → 因子类型 映射（前端展示副本）
//
// ⚠️ 单一真源在后端：smcore/strategy/factor_types.py。
// 改这里前请先改后端并同步——两处必须一致，否则报告/监控层与前端分类会漂移。
// 一致性由 tests/test_frontend_registry_parity.py 守卫（键集合 + 类型串 + 顺序 +
// 配色覆盖逐一比对），漂移会直接红灯，不再靠人眼发现。
//
// 设计原则（与后端一致，用户 2026-09-15 要求「按因子类型分类」）：
// - 每策略归一个主因子类型（1:1），保证归并桶清晰、无歧义；
// - 一个标的可命中多策略 → 可映射到多个因子类型；
// - 因子类型顺序 / 配色与既有策略认知一致，便于阅读。
//
// 2026-09-15 修订：原「题材·事件」拆为「题材」(Theme) 与「事件·舆情」(CCTV) 两类，
// 去掉合并掩盖（Theme 实为拖累、CCTV 才是正贡献）。
// 2026-09-17 第一批：补上漂移两代的 9 个策略 —— 基本面三因子（质量/估值/规模）与
// boll/relativity 沿价格轴拆出的 6 个原子（超卖/近下轨/中轨回踩/带宽收口、
// 上涨满足率/抗跌满足率）。此前它们全部落到「其他」灰色兜底。
// 2026-09-17 第三批（本次）：因子池（factor_zoo 预注册文法 v1）存活价格因子 12 个
// （量价相关/成交稳定/收益偏度/波动比/距低点/波动/非流动性）+ 基本面单指标原子 4 个
// （ROE/毛利率/EP/BP）+ 反向动量 1 个（Boll/Rel 原子的做法同一：买 + 前劣后）。
//
// 配色约定：**同族用同色系不同明度**，便于一眼区分「复合体」与其原子
// （如 基本面·质量 与 ·ROE/·毛利率 同属天蓝；反转·前期弱势 与其余反转原子同属紫）。

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
  // A 批：因子池存活价格因子
  pvcorr20: '量价相关',
  pvcorr60: '量价相关',
  cvamt20: '成交稳定',
  cvamt60: '成交稳定',
  skew20: '收益偏度',
  skew60: '收益偏度',
  vratio20_120: '波动比',
  vratio10_60: '波动比',
  distlo10: '位置·距低点',
  distlo60: '位置·距低点',
  vol20: '波动',
  illiq20: '非流动性',
  // C 批：基本面单指标原子 + 反向动量
  roe: '基本面·质量·ROE',
  gross_margin: '基本面·质量·毛利率',
  ep: '基本面·估值·EP',
  bp: '基本面·估值·BP',
  lowmom20: '反转·前期弱势',
}

export const FACTOR_TYPE_ORDER = [
  '动量',
  '反转·均值回归',
  '反转·超卖',
  '反转·近下轨',
  '反转·中轨回踩',
  '反转·带宽收口',
  '反转·前期弱势',
  '相对强度·资金流',
  '相对强度·上涨满足率',
  '相对强度·抗跌满足率',
  '题材',
  '事件·舆情',
  '基本面·质量',
  '基本面·质量·ROE',
  '基本面·质量·毛利率',
  '基本面·估值',
  '基本面·估值·EP',
  '基本面·估值·BP',
  '基本面·规模',
  '量价相关',
  '成交稳定',
  '波动比',
  '波动',
  '非流动性',
  '收益偏度',
  '位置·距低点',
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
export const FACTOR_TYPE_COLORS = {
  '动量': { bg: 'hsla(217, 91%, 60%, 0.10)', text: '#3B82F6', border: 'hsla(217, 91%, 60%, 0.35)' },
  '反转·均值回归': { bg: 'hsla(229, 87%, 56%, 0.10)', text: '#6366F1', border: 'hsla(229, 87%, 56%, 0.35)' },
  '反转·超卖': { bg: 'hsla(258, 90%, 66%, 0.10)', text: '#8B5CF6', border: 'hsla(258, 90%, 66%, 0.35)' },
  '反转·近下轨': { bg: 'hsla(262, 83%, 58%, 0.10)', text: '#7C3AED', border: 'hsla(262, 83%, 58%, 0.35)' },
  '反转·中轨回踩': { bg: 'hsla(255, 92%, 76%, 0.10)', text: '#A78BFA', border: 'hsla(255, 92%, 76%, 0.35)' },
  '反转·带宽收口': { bg: 'hsla(244, 75%, 57%, 0.10)', text: '#4F46E5', border: 'hsla(244, 75%, 57%, 0.35)' },
  '反转·前期弱势': { bg: 'hsla(271, 91%, 75%, 0.10)', text: '#C084FC', border: 'hsla(271, 91%, 75%, 0.35)' },
  '相对强度·资金流': { bg: 'hsla(157, 81%, 37%, 0.09)', text: '#30A46C', border: 'hsla(157, 81%, 37%, 0.35)' },
  '相对强度·上涨满足率': { bg: 'hsla(160, 84%, 39%, 0.09)', text: '#10B981', border: 'hsla(160, 84%, 39%, 0.35)' },
  '相对强度·抗跌满足率': { bg: 'hsla(161, 94%, 30%, 0.09)', text: '#059669', border: 'hsla(161, 94%, 30%, 0.35)' },
  '题材': { bg: 'hsla(3, 80%, 50%, 0.08)', text: '#E5484D', border: 'hsla(3, 80%, 50%, 0.35)' },
  '事件·舆情': { bg: 'hsla(28, 90%, 50%, 0.10)', text: '#F5861F', border: 'hsla(28, 90%, 50%, 0.35)' },
  '基本面·质量': { bg: 'hsla(199, 89%, 48%, 0.10)', text: '#0EA5E9', border: 'hsla(199, 89%, 48%, 0.35)' },
  '基本面·质量·ROE': { bg: 'hsla(199, 93%, 60%, 0.10)', text: '#38BDF8', border: 'hsla(199, 93%, 60%, 0.35)' },
  '基本面·质量·毛利率': { bg: 'hsla(201, 96%, 32%, 0.10)', text: '#0369A1', border: 'hsla(201, 96%, 32%, 0.35)' },
  '基本面·估值': { bg: 'hsla(173, 80%, 40%, 0.10)', text: '#14B8A6', border: 'hsla(173, 80%, 40%, 0.35)' },
  '基本面·估值·EP': { bg: 'hsla(172, 66%, 50%, 0.10)', text: '#2DD4BF', border: 'hsla(172, 66%, 50%, 0.35)' },
  '基本面·估值·BP': { bg: 'hsla(175, 77%, 26%, 0.10)', text: '#0F766E', border: 'hsla(175, 77%, 26%, 0.35)' },
  '基本面·规模': { bg: 'hsla(215, 16%, 47%, 0.10)', text: '#64748B', border: 'hsla(215, 16%, 47%, 0.35)' },
  // A 批：量价 / 微观结构族（色相彼此拉开，避免与上面各族撞色）
  '量价相关': { bg: 'hsla(330, 81%, 45%, 0.10)', text: '#DB2777', border: 'hsla(330, 81%, 45%, 0.35)' },
  '成交稳定': { bg: 'hsla(292, 84%, 41%, 0.10)', text: '#A21CAF', border: 'hsla(292, 84%, 41%, 0.35)' },
  '波动比': { bg: 'hsla(189, 94%, 35%, 0.10)', text: '#0891B2', border: 'hsla(189, 94%, 35%, 0.35)' },
  '波动': { bg: 'hsla(43, 96%, 40%, 0.10)', text: '#CA8A04', border: 'hsla(43, 96%, 40%, 0.35)' },
  '非流动性': { bg: 'hsla(80, 79%, 27%, 0.10)', text: '#4D7C0F', border: 'hsla(80, 79%, 27%, 0.35)' },
  '收益偏度': { bg: 'hsla(224, 76%, 40%, 0.10)', text: '#1E40AF', border: 'hsla(224, 76%, 40%, 0.35)' },
  '位置·距低点': { bg: 'hsla(30, 6%, 32%, 0.10)', text: '#57534E', border: 'hsla(30, 6%, 32%, 0.35)' },
  '其他': { bg: 'hsl(var(--surface-2))', text: 'hsl(var(--muted))', border: 'hsl(var(--border))' },
}

export function getFactorTypeColor(t) {
  return FACTOR_TYPE_COLORS[t] || FACTOR_TYPE_COLORS['其他']
}
