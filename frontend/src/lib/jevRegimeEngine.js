// JEV 实时市场时期判定与全站动态权重联动引擎
// 数据源：腾讯财经 sh000300（沪深300）65日K线 + 实时盘口（原生 CORS）

export const REGIME_STRATEGIES = [
  {
    id: 'bear_rescue',
    label: '📉 弱势阴跌 / 套牢自救期',
    shortLabel: '弱势自救期',
    condition: '沪深300 低于 MA20 与 MA60，上方抛压较重',
    primaryModel: 'gueant_grid',
    primaryName: 'Guéant 非对称网格做 T（交易管理）',
    action: '不盲目补仓、不割在地板，留足核心底仓，用手机云端非对称网格吃日内波动降成本。',
    tradeAdvice: '当前处于弱势期：强烈建议保持开启手机 Guéant 非对称网格做 T 抽水降本。',
    color: '#3b82f6',
    baseWeights: {
      gueant_grid: 0.26,
      cash_defense: 0.18,
      statistical_arbitrage: 0.14,
      world_model: 0.11,
      microstructure_ofi: 0.08,
      adaptive_regime: 0.07,
      causal_event: 0.06,
      diffusion_stress: 0.05,
      supply_chain_gnn: 0.03,
      multi_agent_rl: 0.02,
    },
  },
  {
    id: 'range_grind',
    label: '〰️ 宽幅震荡 / 磨底轮动期',
    shortLabel: '震荡磨底期',
    condition: '沪深300 位于 MA20 与 MA60 之间拉锯震荡',
    primaryModel: 'statistical_arbitrage',
    primaryName: 'Statistical Arbitrage (统计套利) + Guéant 网格',
    action: '高抛低吸不追涨，靠高低切换与红利底仓做波段差价。',
    tradeAdvice: '当前处于震荡拉锯期：最适合 Guéant 网格来回高抛低吸吃震荡差价。',
    color: '#10b981',
    baseWeights: {
      statistical_arbitrage: 0.21,
      gueant_grid: 0.18,
      microstructure_ofi: 0.14,
      world_model: 0.12,
      adaptive_regime: 0.10,
      causal_event: 0.08,
      cash_defense: 0.06,
      supply_chain_gnn: 0.05,
      diffusion_stress: 0.04,
      multi_agent_rl: 0.02,
    },
  },
  {
    id: 'bull_trend',
    label: '📈 多头趋势 / 主升浪确立期',
    shortLabel: '多头主升期',
    condition: '沪深300 放量站稳 MA20 & MA60，均线多头排列',
    primaryModel: 'world_model',
    primaryName: 'World Model (世界模型) + Macro Cycles',
    action: '减少高频卖出做 T，顺势拿住核心底仓与 510300 宽基 ETF 吃完整波段。',
    tradeAdvice: '⚠️ 注意：大盘已进入多头主升浪，建议放宽或暂停网格卖出，防止底仓卖飞踏空！',
    color: '#a855f7',
    baseWeights: {
      world_model: 0.24,
      supply_chain_gnn: 0.18,
      gueant_grid: 0.10,
      causal_event: 0.14,
      diffusion_stress: 0.11,
      adaptive_regime: 0.09,
      microstructure_ofi: 0.06,
      statistical_arbitrage: 0.04,
      multi_agent_rl: 0.02,
      cash_defense: 0.02,
    },
  },
  {
    id: 'mania_catalyst',
    label: '🔥 情绪狂热 / 题材主线爆发期',
    shortLabel: '情绪爆发期',
    condition: '指数强势突破且 20 日涨幅 > 6% 或单日急涨 > 1.5%',
    primaryModel: 'gnn',
    primaryName: 'GNN (产业链图谱) + Diffusion (扩散模型)',
    action: '聚焦主线核心龙头，同时挂载 5% 移动跟踪止盈防范高波动急跌。',
    tradeAdvice: '🔥 市场情绪爆发：暂停网格逢高卖出，拿住核心底仓并挂载 5% 移动止盈。',
    color: '#f59e0b',
    baseWeights: {
      supply_chain_gnn: 0.26,
      diffusion_stress: 0.20,
      world_model: 0.16,
      causal_event: 0.12,
      gueant_grid: 0.06,
      microstructure_ofi: 0.07,
      multi_agent_rl: 0.05,
      adaptive_regime: 0.04,
      statistical_arbitrage: 0.03,
      cash_defense: 0.01,
    },
  },
]

export function computeDynamicWeights(regimeId, distMa20 = 0, vol20 = 0.015) {
  const reg = REGIME_STRATEGIES.find((r) => r.id === regimeId) || REGIME_STRATEGIES[0]
  const w = { ...reg.baseWeights }
  // 根据实时偏离 MA20 程度与波动率做连续微调，确保权重随实时行情跳动
  const volBoost = Math.max(-0.03, Math.min(0.03, (vol20 - 0.015) * 1.5))
  const trendShift = Math.max(-0.03, Math.min(0.03, distMa20 * 0.5))

  w.cash_defense = Math.max(0.01, (w.cash_defense || 0.05) + volBoost - trendShift)
  w.world_model = Math.max(0.05, (w.world_model || 0.12) + trendShift)
  w.statistical_arbitrage = Math.max(0.02, (w.statistical_arbitrage || 0.08) + Math.abs(volBoost) * 0.5)

  const sum = Object.values(w).reduce((a, b) => a + b, 0) || 1
  const norm = {}
  for (const [k, v] of Object.entries(w)) {
    norm[k] = Number((v / sum).toFixed(4))
  }
  return norm
}

export async function fetchLiveJevRegime() {
  try {
    const [kRes, qRes] = await Promise.all([
      fetch('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh000300,day,,,65,qfq'),
      fetch('https://qt.gtimg.cn/q=sh000300'),
    ])
    const kJson = await kRes.json()
    const qBuf = await qRes.arrayBuffer()
    const qText = new TextDecoder('gbk').decode(qBuf)

    const klines = kJson?.data?.sh000300?.day || kJson?.data?.sh000300?.qfqday || []
    const closes = klines.map((r) => parseFloat(r[2])).filter((v) => v > 0)

    let livePrice = closes[closes.length - 1] || 0
    let dayChg = 0
    if (qText.includes('~')) {
      const f = qText.split('~')
      if (f.length > 32) {
        livePrice = parseFloat(f[3]) || livePrice
        dayChg = parseFloat(f[32]) || 0
      }
    }

    if (closes.length >= 20 && livePrice > 0) {
      // 用最新实时价替换或追加到均线计算
      const recent = [...closes.slice(0, -1), livePrice]
      const ma20 = recent.slice(-20).reduce((a, b) => a + b, 0) / 20
      const ma60 = recent.slice(-60).reduce((a, b) => a + b, 0) / Math.min(60, recent.length)
      const c20Ago = recent[Math.max(0, recent.length - 21)] || livePrice
      const chg20 = (livePrice - c20Ago) / c20Ago

      // 20日日收益率标准差近似波动率
      const rets = []
      for (let i = Math.max(1, recent.length - 20); i < recent.length; i++) {
        rets.push((recent[i] - recent[i - 1]) / recent[i - 1])
      }
      const meanR = rets.reduce((a, b) => a + b, 0) / (rets.length || 1)
      const vol20 = Math.sqrt(rets.reduce((s, r) => s + (r - meanR) ** 2, 0) / (rets.length || 1))

      let regimeId = 'range_grind'
      if (livePrice < ma20 && livePrice < ma60) {
        regimeId = 'bear_rescue'
      } else if (livePrice >= ma20 && livePrice >= ma60) {
        if (chg20 > 0.06 || dayChg > 1.5) {
          regimeId = 'mania_catalyst'
        } else {
          regimeId = 'bull_trend'
        }
      }

      const distMa20 = (livePrice - ma20) / ma20
      const weights = computeDynamicWeights(regimeId, distMa20, vol20)
      const regimeObj = REGIME_STRATEGIES.find((r) => r.id === regimeId) || REGIME_STRATEGIES[0]

      return {
        regimeId,
        regimeObj,
        livePrice: Number(livePrice.toFixed(2)),
        dayChg: Number(dayChg.toFixed(2)),
        ma20: Number(ma20.toFixed(2)),
        ma60: Number(ma60.toFixed(2)),
        vol20: Number((vol20 * 100).toFixed(2)),
        weights,
        updatedAt: new Date().toLocaleTimeString('zh-CN', { hour12: false }),
      }
    }
  } catch (e) {
    // 网络异常兜底
  }
  const fallbackObj = REGIME_STRATEGIES[0]
  return {
    regimeId: 'bear_rescue',
    regimeObj: fallbackObj,
    livePrice: 0,
    dayChg: 0,
    ma20: 0,
    ma60: 0,
    vol20: 1.5,
    weights: computeDynamicWeights('bear_rescue', 0, 0.015),
    updatedAt: '',
  }
}
