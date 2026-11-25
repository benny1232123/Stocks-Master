import React, { useState, useEffect } from 'react'
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

const REGIME_STRATEGIES = [
  {
    id: 'bear_rescue',
    label: '📉 弱势阴跌 / 套牢自救期',
    condition: '大盘或个股跌破 MA20/MA60，上方抛压重',
    primaryModel: 'jev_meta',
    primaryName: 'Guéant 非对称网格做 T + 现金防御',
    action: '不盲目补仓、不割在地板，留足核心底仓，用手机云端非对称网格吃日内波动降成本。',
    color: '#3b82f6',
  },
  {
    id: 'range_grind',
    label: '〰️ 宽幅震荡 / 磨底轮动期',
    condition: '指数箱体拉锯，成交量温和，无明确主线',
    primaryModel: 'statistical_arbitrage',
    primaryName: 'Statistical Arbitrage (统计套利) + OFI',
    action: '高抛低吸不追涨，靠高低切换与红利低波底仓吃差价。',
    color: '#10b981',
  },
  {
    id: 'bull_trend',
    label: '📈 多头趋势 / 主升浪确立期',
    condition: '大盘放量站稳 MA20 & MA60，均线多头排列',
    primaryModel: 'world_model',
    primaryName: 'World Model (世界模型) + Macro Cycles',
    action: '停止高频卖出做 T，顺势拿住核心趋势标的或 510300 宽基 ETF 吃完整波段。',
    color: '#a855f7',
  },
  {
    id: 'mania_catalyst',
    label: '🔥 情绪狂热 / 题材主线爆发期',
    condition: '成交额破万亿，主线产业链出现强催化扩散',
    primaryModel: 'gnn',
    primaryName: 'GNN (产业链图谱) + Diffusion (扩散模型)',
    action: '聚焦核心主线龙头，同时挂载 5% 移动跟踪止盈防范高波动急跌。',
    color: '#f59e0b',
  },
]

export default function FrontierAiSection({ selectedStrategy, onSelectStrategy }) {
  const [data, setData] = useState(null)
  const [activeRegime, setActiveRegime] = useState('bear_rescue')

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
  const routes = active.jev_strategy_route || defaultData.jev_strategy_route

  const strategyConfigs = [
    {
      id: 'gnn',
      key: 'supply_chain_gnn',
      name: '产业链拓扑 GNN',
      icon: Network,
      color: '#2563eb',
      pct: (routes.supply_chain_gnn * 100).toFixed(1),
      desc: '图卷积多跳特征传导 · 捕捉景气扩散 Alpha'
    },
    {
      id: 'causal',
      key: 'causal_event',
      name: '大模型因果事件',
      icon: Sparkles,
      color: '#ec4899',
      pct: (routes.causal_event * 100).toFixed(1),
      desc: '结构因果图传导 · 捕捉政策与突发脉冲'
    },
    {
      id: 'world_model',
      key: 'world_model',
      name: '世界模型前瞻规划',
      icon: Globe2,
      color: '#7c3aed',
      pct: (routes.world_model * 100).toFixed(1),
      desc: 'RSSM 潜空间推演 · 500次轨迹反事实规划'
    },
    {
      id: 'ofi',
      key: 'microstructure_ofi',
      name: '盘口微观订单流',
      icon: Crosshair,
      color: '#0284c7',
      pct: (routes.microstructure_ofi * 100).toFixed(1),
      desc: 'Level-2 订单失衡 OFI · 主力大单吸筹博弈'
    },
    {
      id: 'adaptive',
      key: 'adaptive_regime',
      name: '全天候自适应稳健',
      icon: CheckCircle2,
      color: '#059669',
      pct: (routes.adaptive_regime * 100).toFixed(1),
      desc: '宏观周期状态机 · 均线破位避险'
    },
    {
      id: 'stat_arb',
      key: 'statistical_arbitrage',
      name: '统计套利行业中性',
      icon: LineChart,
      color: '#0d9488',
      pct: (routes.statistical_arbitrage * 100).toFixed(1),
      desc: '因子正交去 Beta · 45度平稳向上复利'
    },
    {
      id: 'diffusion',
      key: 'diffusion_stress',
      name: '金融时序扩散模型',
      icon: Waves,
      color: '#d97706',
      pct: (routes.diffusion_stress * 100).toFixed(1),
      desc: '50步生成式极限压力测试 · 极值反脆弱'
    },
    {
      id: 'marl',
      key: 'multi_agent_rl',
      name: '多智能体强化博弈',
      icon: Users,
      color: '#f97316',
      pct: (routes.multi_agent_rl * 100).toFixed(1),
      desc: '沙盘纳什均衡 · 反向猎杀散户恐慌踩踏'
    }
  ]

  const currentRegimeObj = REGIME_STRATEGIES.find((r) => r.id === activeRegime) || REGIME_STRATEGIES[0]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, marginBottom: 16 }}>
      {/* JEV 分时期自适应策略路由器 */}
      <div className="glass-card" style={{ padding: '16px 20px', borderRadius: 12, border: `1px solid ${currentRegimeObj.color}40` }}>
        <div style={{ fontWeight: 600, fontSize: '0.98rem', marginBottom: 4 }}>
          🧭 JEV 分时期策略路由器 (不同市场时期选用不同主策略)
        </div>
        <div style={{ fontSize: '0.78rem', color: 'hsl(var(--muted))', marginBottom: 12 }}>
          不再每天高频换股——点击切换当前市场时期，JEV 自动匹配最适合该阶段的主力策略并联动下方净值曲线：
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: 10, marginBottom: 12 }}>
          {REGIME_STRATEGIES.map((r) => {
            const isSel = activeRegime === r.id
            return (
              <button
                key={r.id}
                type="button"
                onClick={() => {
                  setActiveRegime(r.id)
                  if (onSelectStrategy) onSelectStrategy(r.primaryModel)
                }}
                style={{
                  textAlign: 'left',
                  padding: '10px 12px',
                  borderRadius: 8,
                  border: isSel ? `1.5px solid ${r.color}` : '1px solid hsl(var(--border))',
                  background: isSel ? `${r.color}14` : 'hsl(var(--secondary) / 0.35)',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                }}
              >
                <div style={{ fontSize: 13, fontWeight: 600, color: isSel ? r.color : 'hsl(var(--foreground))' }}>{r.label}</div>
                <div style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))', marginTop: 4 }}>{r.condition}</div>
              </button>
            )
          })}
        </div>
        <div style={{
          padding: '10px 14px',
          borderRadius: 8,
          background: `${currentRegimeObj.color}10`,
          borderLeft: `4px solid ${currentRegimeObj.color}`,
          fontSize: 13,
          lineHeight: 1.6
        }}>
          <div>
            <strong>当前时期推荐主策略：</strong>
            <span style={{ color: currentRegimeObj.color, fontWeight: 700 }}>{currentRegimeObj.primaryName}</span>
          </div>
          <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>
            实操指令：{currentRegimeObj.action}
          </div>
        </div>
      </div>

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
                底层非自回归概率引擎 · 统领八大并排策略动态权重路由，全局统一风控拦截 (P≥0.65 强制一票否决)
              </div>
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: '0.78rem' }}>
            <span style={{ color: 'hsl(var(--muted))' }}>统一风控状态:</span>
            <span style={{ color: '#059669', display: 'inline-flex', alignItems: 'center', gap: 4, fontWeight: 500 }}>
              <ShieldCheck className="w-4 h-4" /> 正常放行 (P=0.31)
            </span>
          </div>
        </div>

        {/* 八大并排策略动态权重分配进度条 */}
        <div style={{ marginBottom: 14 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.78rem', color: 'hsl(var(--muted))', marginBottom: 6 }}>
            <span>Jev 底层当前实时权重路由 (点击策略卡片可直达下方曲线)：</span>
            <span>现金/国债防守保留: {(routes.cash_defense * 100).toFixed(1)}%</span>
          </div>

          <div style={{
            display: 'flex',
            height: 10,
            borderRadius: 6,
            overflow: 'hidden',
            background: 'hsla(var(--muted)/0.15)',
            marginBottom: 10
          }}>
            {strategyConfigs.map((cfg) => (
              <div
                key={cfg.id}
                style={{
                  width: `${cfg.pct}%`,
                  background: cfg.color,
                  transition: 'width 0.3s ease'
                }}
                title={`${cfg.name}: ${cfg.pct}%`}
              />
            ))}
            <div
              style={{
                width: `${(routes.cash_defense * 100).toFixed(1)}%`,
                background: 'hsl(var(--muted))',
                opacity: 0.4
              }}
              title={`现金防守: ${(routes.cash_defense * 100).toFixed(1)}%`}
            />
          </div>

          {/* 8大并排策略卡片网格 */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
            gap: 10
          }}>
            {strategyConfigs.map((cfg) => {
              const isSelected = selectedStrategy === cfg.id
              const Icon = cfg.icon
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
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <Icon className="w-3.5 h-3.5" style={{ color: cfg.color }} />
                      <span style={{ fontWeight: 600, fontSize: '0.82rem' }}>{cfg.name}</span>
                    </div>
                    <span style={{ fontWeight: 700, fontSize: '0.84rem', color: cfg.color }}>
                      {cfg.pct}%
                    </span>
                  </div>
                  <div style={{ fontSize: '0.72rem', color: 'hsl(var(--muted))', lineHeight: 1.3 }}>
                    {cfg.desc}
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* Jev 底层优选标的池展示区 */}
        <div style={{
          background: 'hsla(var(--muted)/0.04)',
          borderRadius: 8,
          border: '1px solid hsla(var(--border)/0.5)',
          padding: '12px 14px',
          display: 'flex',
          flexDirection: 'column',
          gap: 8,
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 6 }}>
            <span style={{ fontWeight: 600, fontSize: '0.84rem', color: 'hsl(var(--foreground))', whiteSpace: 'nowrap', flexShrink: 0 }}>
              🎯 Jev 底层实时协同打分优选
            </span>
            <span style={{ fontSize: '0.76rem', color: 'hsl(var(--muted))', whiteSpace: 'nowrap', flexShrink: 0 }}>
              催化主线: <strong style={{ color: '#2563eb' }}>{active.gnn_active_catalyst}</strong>
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            {active.top_picks.map((p) => (
              <span
                key={p.code}
                className="chip"
                style={{
                  background: 'rgba(37, 99, 235, 0.08)',
                  borderColor: 'rgba(37, 99, 235, 0.25)',
                  color: '#2563eb',
                  fontSize: '0.76rem',
                  fontWeight: 500,
                  whiteSpace: 'nowrap'
                }}
              >
                {p.name} ({p.code}) · 综合 {p.frontier_ai_score}分 [{p.suggested_action}]
              </span>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
