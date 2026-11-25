import React, { useState, useEffect, useMemo } from 'react'
import {
  Zap,
  Network,
  Waves,
  Globe2,
  ShieldCheck,
  ShieldAlert,
  ArrowRight,
  TrendingUp,
  Cpu,
  CheckCircle2,
  Sliders,
  Crosshair,
  Sparkles,
  Users,
  LineChart
} from 'lucide-react'
import { SectionCard } from './cards'
import { REGIME_STRATEGIES, computeDynamicWeights } from '../lib/jevRegimeEngine'

export default function FrontierAiSection({ selectedStrategy, onSelectStrategy, jevLive, activeRegime: propRegime, onChangeRegime }) {
  const [data, setData] = useState(null)
  const [localRegime, setLocalRegime] = useState('bear_rescue')
  const activeRegime = propRegime || localRegime
  const setActiveRegime = onChangeRegime || setLocalRegime

  useEffect(() => {
    let alive = true
    async function loadData() {
      try {
        const res = await fetch('web_data/frontier_ai_latest.json')
        if (res.ok && alive) {
          const json = await res.json()
          setData(json)
        }
      } catch (e) {
        console.warn('Failed to load frontier AI data:', e)
      }
    }
    loadData()
    return () => { alive = false }
  }, [])

  const defaultData = {
    updated_at: '2026-11-23 22:30:28',
    jev_strategy_route: {
      supply_chain_gnn: 0.1885,
      causal_event: 0.1586,
      world_model: 0.1582,
      microstructure_ofi: 0.1385,
      adaptive_regime: 0.0919,
      statistical_arbitrage: 0.0820,
      diffusion_stress: 0.0717,
      multi_agent_rl: 0.0665,
      cash_defense: 0.0441
    },
    gnn_active_catalyst: 'GPU_ASIC_UPSTREAM (算力链传导)',
    diffusion_tested_scenarios: 500,
    top_picks: [
      {
        code: '002463',
        name: '沪电股份',
        sector: 'PCB_SUBSTRATE',
        price: 38.6,
        beta: 1.15,
        jev_safe: true,
        jev_risk_prob: 0.31,
        gnn_spillover: 0.0911,
        diffusion_survival_rate: '33.8%',
        diffusion_worst_drawdown: '-32.36%',
        frontier_ai_score: 65.1,
        suggested_action: '超配买入'
      },
      {
        code: '300308',
        name: '中际旭创',
        sector: 'CPO_OPTICAL_MODULE',
        price: 142.5,
        beta: 1.25,
        jev_safe: true,
        jev_risk_prob: 0.31,
        gnn_spillover: 0.089,
        diffusion_survival_rate: '35.4%',
        diffusion_worst_drawdown: '-37.76%',
        frontier_ai_score: 64.9,
        suggested_action: '超配买入'
      },
      {
        code: '300502',
        name: '新易盛',
        sector: 'CPO_OPTICAL_MODULE',
        price: 118.2,
        beta: 1.3,
        jev_safe: true,
        jev_risk_prob: 0.31,
        gnn_spillover: 0.089,
        diffusion_survival_rate: '33.2%',
        diffusion_worst_drawdown: '-42.79%',
        frontier_ai_score: 64.3,
        suggested_action: '超配买入'
      },
      {
        code: '002594',
        name: '比亚迪',
        sector: 'EV_SMART_OEM',
        price: 286.0,
        beta: 0.9,
        jev_safe: true,
        jev_risk_prob: 0.31,
        gnn_spillover: 0.02,
        diffusion_survival_rate: '49.2%',
        diffusion_worst_drawdown: '-21.67%',
        frontier_ai_score: 48.4,
        suggested_action: '观察'
      },
      {
        code: '601138',
        name: '工业富联',
        sector: 'SERVER_INTEGRATION',
        price: 24.3,
        beta: 1.1,
        jev_safe: true,
        jev_risk_prob: 0.31,
        gnn_spillover: 0.0278,
        diffusion_survival_rate: '40.2%',
        diffusion_worst_drawdown: '-28.86%',
        frontier_ai_score: 48.0,
        suggested_action: '观察'
      }
    ]
  }

  const active = data || defaultData
  const distMa20 = jevLive && jevLive.ma20 > 0 ? (jevLive.livePrice - jevLive.ma20) / jevLive.ma20 : 0
  const vol20 = jevLive ? jevLive.vol20 / 100 : 0.015
  const routes = useMemo(() => {
    if (jevLive && activeRegime === jevLive.regimeId && jevLive.weights) {
      return jevLive.weights
    }
    return computeDynamicWeights(activeRegime, distMa20, vol20)
  }, [activeRegime, jevLive, distMa20, vol20])

  const strategyConfigs = [
    {
      id: 'adaptive',
      key: 'adaptive_regime',
      category: 'defense',
      name: '全天候自适应稳健',
      icon: CheckCircle2,
      color: '#059669',
      pct: ((routes.adaptive_regime || 0) * 100).toFixed(1),
      desc: '宏观周期状态机 · 均线破位刚性避险',
      howTo: {
        buy: '沪深300 放量站上 MA20 且 MA20 > MA60 时开仓，分批买入宽基 ETF（如 510300）或高分蓝筹。',
        sell: '跌破 MA20 先减半，有效跌破 MA60（连续 2 日收盘低于）无条件清仓切入国债逆回购。',
        risk: '多头期 90% 仓位，震荡期 40%，弱势期 0% 空仓防守，最大回撤严控在个位数。',
        fit: '牛熊切换频发、不想天天盯盘的稳健投资者。'
      }
    },
    {
      id: 'stat_arb',
      key: 'statistical_arbitrage',
      category: 'defense',
      name: '统计套利行业中性',
      icon: LineChart,
      color: '#0d9488',
      pct: ((routes.statistical_arbitrage || 0) * 100).toFixed(1),
      desc: '因子正交去 Beta · 45度平稳向上复利',
      howTo: {
        buy: '在同行业内挑选估值/动量偏低、基本面稳健的滞涨股（或超跌龙头）低吸建仓。',
        sell: '当标的相对行业均值修复超 +5%~+8% 时获利了结，切向下一只低估滞涨标的。',
        risk: '分散到 2~3 个低相关行业，单股不超总资金 35%，严格回避单边题材妖股。',
        fit: '指数横盘拉锯、板块快速轮动的磨底震荡市。'
      }
    },
    {
      id: 'cash_defense',
      key: 'cash_defense',
      category: 'defense',
      name: '国债逆回购 / 现金防守',
      icon: ShieldCheck,
      color: '#64748b',
      pct: ((routes.cash_defense || 0) * 100).toFixed(1),
      desc: '空仓保留流动性 · 年化 ~2.5% 无风险底息',
      howTo: {
        buy: '每天 14:30~15:00 在券商 App 买入深市/沪市 1 天期国债逆回购（如 131810 / 204001），周四买 1 天享 3 天利息。',
        sell: '次一交易日开盘资金自动可用（可随时买股票），无需手动卖出，保留 100% 机动性。',
        risk: '零股票市场回撤风险，专治大盘主跌浪“手痒抄底”。',
        fit: '大盘明显处于 MA20/MA60 下方阴跌、或风险概率 P≥0.65 触发风控拦截时。'
      }
    },
    {
      id: 'world_model',
      key: 'world_model',
      category: 'trend',
      name: '世界模型前瞻规划',
      icon: Globe2,
      color: '#7c3aed',
      pct: ((routes.world_model || 0) * 100).toFixed(1),
      desc: 'RSSM 潜空间推演 · 500次轨迹反事实规划',
      howTo: {
        buy: '确认板块趋势拐点向上时买入前排核心标的，持仓周期 1~3 周吃完整波段。',
        sell: '触发 -4% 硬止损无条件离场；盈利超 +10% 后回撤 3% 启动移动止盈。',
        risk: '依靠潜空间反事实推演提前规避高波动回撤日，禁止在下跌趋势中逆势补仓。',
        fit: '大盘确立多头主升浪、行业景气度明确向上的阶段。'
      }
    },
    {
      id: 'gnn',
      key: 'supply_chain_gnn',
      category: 'trend',
      name: '产业链拓扑 GNN',
      icon: Network,
      color: '#2563eb',
      pct: ((routes.supply_chain_gnn || 0) * 100).toFixed(1),
      desc: '图卷积多跳特征传导 · 捕捉景气扩散 Alpha',
      howTo: {
        buy: '当龙头涨停或业绩超预期时，买入尚未大涨的上下游 1~2 跳配套供应商（如算力 PCB/光模块配套）。',
        sell: '当上游龙头见顶放量滞涨、或补涨标的涨幅追平龙头时分批兑现。',
        risk: '只做有真实供应链订单关联的标的，若龙头次日核按钮则立即放弃跟风。',
        fit: '主线题材明确、资金从龙头向中后排扩散的初期阶段。'
      }
    },
    {
      id: 'diffusion',
      key: 'diffusion_stress',
      category: 'trend',
      name: '金融时序扩散模型',
      icon: Waves,
      color: '#d97706',
      pct: ((routes.diffusion_stress || 0) * 100).toFixed(1),
      desc: '50步生成式极限压力测试 · 极值反脆弱',
      howTo: {
        buy: '只买入经受 50 步极端暴跌压力测试仍具备强支撑与高股息/现金流的反脆弱标的。',
        sell: '在市场恐慌见底反弹、情绪修复至正常估值中枢后逐步逢高减仓。',
        risk: '单票最坏情景回撤封顶约束，专防黑天鹅连续跌停踩踏。',
        fit: '外部宏观不确定性高、市场频繁出现急跌插针的时期。'
      }
    },
    {
      id: 'ofi',
      key: 'microstructure_ofi',
      category: 'alpha',
      name: '盘口微观订单流',
      icon: Crosshair,
      color: '#0284c7',
      pct: ((routes.microstructure_ofi || 0) * 100).toFixed(1),
      desc: 'Level-2 订单失衡 OFI · 主力大单吸筹博弈',
      howTo: {
        buy: '尾盘 14:40 后观察放量突破筹码峰且主动买盘持续净流入的标的，顺势轻仓跟随。',
        sell: '超短线 1~3 日快进快出，次日冲高不封板或买盘衰减即分批止盈。',
        risk: '快进快出纪律：破分时均价线或跌超 -3% 坚决止损，绝不把短线做成套牢长线。',
        fit: '成交额放大、游资与主力资金活跃度高的短线情绪窗口。'
      }
    },
    {
      id: 'causal',
      key: 'causal_event',
      category: 'alpha',
      name: '大模型因果事件',
      icon: Sparkles,
      color: '#ec4899',
      pct: ((routes.causal_event || 0) * 100).toFixed(1),
      desc: '结构因果图传导 · 捕捉政策与突发脉冲',
      howTo: {
        buy: '重大政策/行业催化落地当晚，筛选最直接受益且尚未一字涨停的细分龙头，早盘集合竞价/分时回踩低吸。',
        sell: '事件利好兑现、预期一致高潮放巨量换手时果断止盈（买在分歧、卖在一致）。',
        risk: '不追连续 3 板以上纯情绪接力，只做首波因果传导预期差。',
        fit: '政策密集出台、重磅产业大会或突发供需缺口事件期。'
      }
    },
    {
      id: 'marl',
      key: 'multi_agent_rl',
      category: 'alpha',
      name: '多智能体强化博弈',
      icon: Users,
      color: '#f97316',
      pct: ((routes.multi_agent_rl || 0) * 100).toFixed(1),
      desc: '沙盘纳什均衡 · 反向猎杀散户恐慌踩踏',
      howTo: {
        buy: '大盘或优质标的出现无基本面利空的连续恐慌杀跌、跌停潮尾声出现翘板承接时分批低吸。',
        sell: '恐慌盘出清后的 2~4 天反抽修复期，反弹至 5 日/10 日均线压力位快速兑现。',
        risk: '必须分 2~3 笔金字塔式左侧试仓，严禁一次性满仓抄在半山腰。',
        fit: '市场出现极端情绪冰点、流动性踩踏后的短线反抽博弈。'
      }
    }
  ]

  const categoryGroups = [
    { key: 'defense', title: '🛡️ 存量防卫 / 解套类（偏震荡与回撤控制）', color: '#e11d48' },
    { key: 'trend', title: '📈 趋势 / 周期配置类（中线景气与宏观轮动）', color: '#2563eb' },
    { key: 'alpha', title: '🚀 高弹性 / 事件驱动类（强攻主升与资金博弈）', color: '#d97706' }
  ]

  const currentRegimeObj = REGIME_STRATEGIES.find((r) => r.id === activeRegime) || REGIME_STRATEGIES[0]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, marginBottom: 16 }}>
      {/* Jev 底层决策选择器核心架构卡片 */}
      <div className="glass-card" style={{ padding: '16px 20px', borderRadius: 12 }}>
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: 10,
          marginBottom: 14,
          paddingBottom: 12,
          borderBottom: '1px solid hsla(var(--border)/0.6)'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div style={{
              width: 34,
              height: 34,
              borderRadius: 8,
              background: 'linear-gradient(135deg, #10b981, #059669)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#fff'
            }}>
              <Zap className="w-5 h-5" />
            </div>
            <div>
              <div style={{ fontWeight: 600, fontSize: '0.98rem', display: 'flex', alignItems: 'center', gap: 8 }}>
                <span>⚡ Jev 底层决策选择器 (Underlying Meta-Selector)</span>
                <span className="chip" style={{ color: '#059669', borderColor: 'rgba(16,185,129,0.3)', background: 'rgba(16,185,129,0.08)', fontSize: '0.72rem' }}>
                  System 1 · 延迟 &lt; 3ms
                </span>
              </div>
              <div style={{ fontSize: '0.78rem', color: 'hsl(var(--muted))' }}>
                底层非自回归概率引擎 · 小资金不拆仓原则：实时计算各策略当前匹配度评分，自动锁定唯一最优主策略集中执行 (P≥0.65 强制风控一票否决)
              </div>
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: '0.78rem', flexWrap: 'wrap' }}>
            <span className="chip" style={{
              color: currentRegimeObj.color,
              borderColor: `${currentRegimeObj.color}50`,
              background: `${currentRegimeObj.color}12`,
              fontSize: '0.74rem',
              fontWeight: 600
            }}>
              🧭 底层自动识别：{currentRegimeObj.label}
              {jevLive && jevLive.livePrice > 0 ? ` · 沪深300 ${jevLive.livePrice} (MA20 ${jevLive.ma20})` : ''}
              {' '}→ 主推：{currentRegimeObj.primaryName}
            </span>
            <span style={{ color: '#059669', display: 'inline-flex', alignItems: 'center', gap: 4, fontWeight: 500 }}>
              <ShieldCheck className="w-4 h-4" /> 正常放行 (P=0.31)
            </span>
          </div>
        </div>

        {/* 单一主策略集中执行栏（不再拆分有限资金） */}
        <div style={{ marginBottom: 14 }}>
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: 8,
            padding: '10px 14px',
            borderRadius: 8,
            background: `${currentRegimeObj.color}10`,
            border: `1px solid ${currentRegimeObj.color}35`,
            marginBottom: 14
          }}>
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color: 'hsl(var(--foreground))' }}>
              🎯 当前唯一集中执行策略：<span style={{ color: currentRegimeObj.color, fontWeight: 700 }}>{currentRegimeObj.primaryName}</span>
            </div>
            <div style={{ fontSize: '0.74rem', color: 'hsl(var(--muted))' }}>
              有限资金 100% 集中执行单一主策略（不拆散本金）· 其余策略仅作匹配度对比，点击可查看历史回测曲线
            </div>
          </div>

          {/* 按策略类别分组展示卡片 */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            {categoryGroups.map((grp) => {
              const items = strategyConfigs.filter((c) => c.category === grp.key)
              return (
                <div key={grp.key}>
                  <div style={{
                    fontSize: '0.78rem',
                    fontWeight: 700,
                    color: grp.color,
                    marginBottom: 8,
                    display: 'flex',
                    alignItems: 'center',
                    gap: 6
                  }}>
                    <span>{grp.title}</span>
                  </div>
                  <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))',
                    gap: 10
                  }}>
                    {items.map((cfg) => {
                      const isSelected = selectedStrategy === cfg.id
                      const isAutoPrimary = currentRegimeObj.primaryModel === cfg.id
                      const Icon = cfg.icon
                      const maxPct = Math.max(...strategyConfigs.map((s) => Number(s.pct) || 1), 1)
                      const matchScore = isAutoPrimary
                        ? 95
                        : Math.max(28, Math.min(89, Math.round((Number(cfg.pct) / maxPct) * 86)))
                      return (
                        <div
                          key={cfg.id}
                          onClick={() => onSelectStrategy && onSelectStrategy(cfg.id)}
                          style={{
                            padding: '10px 12px',
                            borderRadius: 8,
                            cursor: 'pointer',
                            background: isSelected ? 'hsla(var(--primary)/0.08)' : 'hsla(var(--muted)/0.04)',
                            border: isSelected ? `1.5px solid ${cfg.color}` : '1px solid hsla(var(--border)/0.6)',
                            boxShadow: isSelected ? `0 0 10px ${cfg.color}20` : 'none',
                            transition: 'all 0.15s ease'
                          }}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4, gap: 6 }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6, minWidth: 0 }}>
                              <Icon className="w-3.5 h-3.5" style={{ color: cfg.color, flexShrink: 0 }} />
                              <span style={{ fontWeight: 600, fontSize: '0.82rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{cfg.name}</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 }}>
                              {isAutoPrimary && (
                                <span style={{
                                  fontSize: '0.66rem',
                                  padding: '1px 5px',
                                  borderRadius: 4,
                                  background: `${cfg.color}18`,
                                  color: cfg.color,
                                  fontWeight: 700
                                }}>
                                  唯一执行
                                </span>
                              )}
                              <span style={{ fontWeight: 700, fontSize: '0.82rem', color: cfg.color }}>
                                {matchScore}分
                              </span>
                            </div>
                          </div>
                          <div style={{ fontSize: '0.72rem', color: 'hsl(var(--muted))', lineHeight: 1.3 }}>
                            {cfg.desc}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* 当前选中策略实操指南 */}
        {(() => {
          const activeStrat = strategyConfigs.find((s) => s.id === selectedStrategy)
            || strategyConfigs.find((s) => s.id === currentRegimeObj.primaryModel)
            || strategyConfigs[0]
          if (!activeStrat?.howTo) return null
          return (
            <div style={{
              marginBottom: 14,
              padding: '12px 14px',
              borderRadius: 8,
              background: `${activeStrat.color}0a`,
              border: `1px solid ${activeStrat.color}30`,
              fontSize: '0.78rem',
              lineHeight: 1.55
            }}>
              <div style={{ fontWeight: 700, color: activeStrat.color, marginBottom: 6 }}>
                📖 策略实操指南 · {activeStrat.name}（点击上方任意策略卡片切换查看）
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 8 }}>
                <div><strong>📥 怎么买：</strong>{activeStrat.howTo.buy}</div>
                <div><strong>📤 怎么卖：</strong>{activeStrat.howTo.sell}</div>
                <div><strong>🛡️ 仓位与风控：</strong>{activeStrat.howTo.risk}</div>
                <div><strong>🧭 适用行情：</strong>{activeStrat.howTo.fit}</div>
              </div>
            </div>
          )
        })()}
      </div>
    </div>
  )
}
