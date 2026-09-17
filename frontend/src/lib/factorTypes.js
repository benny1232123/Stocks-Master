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
// 2026-09-17 重构：用户要求「完全去掉之前的策略，重新整一套因子候选」。原 boll/relativity/
// theme/cctv/momentum 复合策略、基本面 quality/value/size 复合体、boll/relativity 的 6 个原子、
// C 批基本面单指标原子（ROE/毛利率/EP/BP）与反向动量 lowmom20 全部移除，菜单收缩为
// **A 批 12 个因子池（factor_zoo 预注册文法 v1）存活价格因子**：
//   量价相关(pvcorr20/60) / 成交稳定(cvamt20/60) / 收益偏度(skew20/60) /
//   波动比(vratio20_120/vratio10_60) / 位置·距低点(distlo10/60) / 波动(vol20) / 非流动性(illiq20)。
// 这些因子公式 1:1 复用 factor_zoo.compute_factor，已验证存活。新因子候选（来自因子挖掘系统 /
// 开源 Alpha101 等）后续按同一注册表协议接入：在 factor_types 注册 + 产出同名
// Stock-Selection-<Label>-<date>.csv 即可，前端三处副本（本文件 / styles.css / App.jsx）同步更新，
// tests/test_frontend_registry_parity.py 守卫一致性。
//
// 配色约定：**同族用同色系不同明度**（如 pvcorr20/60 同属品红、cvamt20/60 同属紫、skew20/60 同属蓝），
// 便于一眼区分窗口变体。

export const STRATEGY_FACTOR_TYPE = {
  // A 批：因子池存活价格因子（菜单当前全集）
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
}

export const FACTOR_TYPE_ORDER = [
  '量价相关',
  '成交稳定',
  '收益偏度',
  '波动比',
  '位置·距低点',
  '波动',
  '非流动性',
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
  // A 批：量价 / 微观结构族（菜单当前全集；与后端 factor_types.FACTOR_TYPE_ORDER 一致）
  '量价相关': { bg: 'hsla(330, 81%, 45%, 0.10)', text: '#DB2777', border: 'hsla(330, 81%, 45%, 0.35)' },
  '成交稳定': { bg: 'hsla(292, 84%, 41%, 0.10)', text: '#A21CAF', border: 'hsla(292, 84%, 41%, 0.35)' },
  '收益偏度': { bg: 'hsla(224, 76%, 40%, 0.10)', text: '#1E40AF', border: 'hsla(224, 76%, 40%, 0.35)' },
  '波动比': { bg: 'hsla(189, 94%, 35%, 0.10)', text: '#0891B2', border: 'hsla(189, 94%, 35%, 0.35)' },
  '位置·距低点': { bg: 'hsla(30, 6%, 32%, 0.10)', text: '#57534E', border: 'hsla(30, 6%, 32%, 0.35)' },
  '波动': { bg: 'hsla(43, 96%, 40%, 0.10)', text: '#CA8A04', border: 'hsla(43, 96%, 40%, 0.35)' },
  '非流动性': { bg: 'hsla(80, 79%, 27%, 0.10)', text: '#4D7C0F', border: 'hsla(80, 79%, 27%, 0.35)' },
  '其他': { bg: 'hsl(var(--surface-2))', text: 'hsl(var(--muted))', border: 'hsl(var(--border))' },
}

export function getFactorTypeColor(t) {
  return FACTOR_TYPE_COLORS[t] || FACTOR_TYPE_COLORS['其他']
}
