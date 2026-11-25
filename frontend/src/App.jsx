import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  LayoutDashboard,
  Globe2,
  Briefcase,
  FlaskConical,
  ArrowUpRight,
  ArrowDownRight,
  FileText,
  TrendingUp,
  DollarSign,
  TrendingDown,
  Target,
  Percent,
  BarChart3,
  Trophy,
  AlertTriangle,
  Zap,
  ShieldCheck,
} from 'lucide-react'
import { Button } from './components/ui/button'
import { StatCard, Field, SectionCard, MacroCard } from './components/cards'
import DailyExpandableList from './components/DailyExpandableList'
import TradeManager from './components/TradeManager'
import QuantRescueSection from './components/QuantRescueSection'
import NewsAiSection from './components/NewsAiSection'
import FrontierAiSection from './components/FrontierAiSection'
import { cn } from './lib/utils'
import { factorTypesOfSource, getFactorTypeColor } from './lib/factorTypes'
import { fetchLiveIndexSnapshot, fetchLiveMarketBreadth, fetchLiveStockQuotes } from './lib/realtimeQuotes'
import { insertTradeToCloud } from './lib/supabaseTrades'
import MACRO_CYCLES from './macro_cycles_curves.json'
import WORLD_MODEL_CYCLES from './world_model_curves.json'
import DIFFUSION_CYCLES from './diffusion_curves.json'
import GNN_CYCLES from './gnn_curves.json'
import JEV_META_CYCLES from './jev_meta_curves.json'
import OFI_CYCLES from './microstructure_ofi_curves.json'
import CAUSAL_CYCLES from './causal_event_curves.json'
import MARL_CYCLES from './multi_agent_rl_curves.json'
import STAT_ARB_CYCLES from './statistical_arbitrage_curves.json'
import REAL_CAPITAL_10K from './real_capital_10k_backtest.json'
import RealCapital10kSection from './components/RealCapital10kSection'

const TIMEFRAME_OPTIONS = [
  { id: 'day', short_name: '日', label: '日频 (近阶段逐日滚动)' },
  { id: 'week', short_name: '周', label: '周度 (近1周实盘区间)' },
  { id: 'month', short_name: '月', label: '月度 (近1月实盘区间)' },
  { id: 'quarter', short_name: '季', label: '季度 (近1季滚动区间)' },
  { id: 'year', short_name: '年', label: '年度 (近1年年度表现)' },
  { id: 'all', short_name: '全部', label: '全部 (成立以来全景)' },
]

// 构建版本（vite define 注入；本地 dev 下未定义时回退 dev）
const BUILD_SHA = typeof __BUILD_SHA__ === 'string' ? __BUILD_SHA__ : 'dev'
const BUILD_TIME = typeof __BUILD_TIME__ === 'string' ? __BUILD_TIME__ : ''

// 本地时区日期字符串 YYYY-MM-DD（toISOString 是 UTC，CST 早 8 点前会错成前一天）
const localDateStr = (d = new Date()) =>
  `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`

const TABS = [
  { id: 'overview', label: '概览', icon: LayoutDashboard },
  { id: 'macro', label: '宏观经济', icon: Globe2 },
  { id: 'daily', label: '日报', icon: FileText },
  { id: 'portfolio', label: '持仓', icon: Briefcase },
  { id: 'backtest', label: '回测', icon: FlaskConical },
  { id: 'admin', label: '管理', icon: ShieldCheck },
]

function _renderMacroInsight(macro) {
  const insights = []
  // 汇率判断
  if (macro['美元/人民币'] != null) {
    const usdcny = Number(macro['美元/人民币'])
    if (usdcny > 7.3) insights.push({ level: 'warn', text: `美元/人民币 ${usdcny.toFixed(4)} 偏强，人民币承压，外资流出风险升高` })
    else if (usdcny < 7.0) insights.push({ level: 'good', text: `美元/人民币 ${usdcny.toFixed(4)} 偏弱，人民币相对强势` })
    else insights.push({ level: 'neutral', text: `美元/人民币 ${usdcny.toFixed(4)} 处于中性区间` })
  }
  // PMI 判断
  if (macro['制造业PMI'] != null) {
    const pmi = Number(macro['制造业PMI'])
    if (pmi >= 51) insights.push({ level: 'good', text: `制造业 PMI ${pmi.toFixed(1)} 扩张偏强，企业景气向好` })
    else if (pmi >= 50) insights.push({ level: 'neutral', text: `制造业 PMI ${pmi.toFixed(1)} 荣枯线附近，扩张乏力` })
    else insights.push({ level: 'warn', text: `制造业 PMI ${pmi.toFixed(1)} 收缩区间，经济下行压力` })
  }
  // 利率判断
  if (macro['Shibor隔夜'] != null) {
    const shibor = Number(macro['Shibor隔夜'])
    if (shibor > 2.0) insights.push({ level: 'warn', text: `SHIBOR 隔夜 ${shibor.toFixed(2)}% 偏高，资金面偏紧` })
    else if (shibor < 1.5) insights.push({ level: 'good', text: `SHIBOR 隔夜 ${shibor.toFixed(2)}% 偏低，流动性充裕` })
    else insights.push({ level: 'neutral', text: `SHIBOR 隔夜 ${shibor.toFixed(2)}% 中性水平` })
  }
  // CPI/PPI
  if (macro['CPI同比'] != null || macro['PPI同比'] != null) {
    const cpi = macro['CPI同比'] != null ? Number(macro['CPI同比']) : null
    const ppi = macro['PPI同比'] != null ? Number(macro['PPI同比']) : null
    if (cpi != null && cpi < 0 && ppi != null && ppi < 0) {
      insights.push({ level: 'warn', text: `CPI ${cpi.toFixed(2)}% / PPI ${ppi.toFixed(2)}% 双通缩风险` })
    } else if (cpi != null && cpi > 3) {
      insights.push({ level: 'warn', text: `CPI ${cpi.toFixed(2)}% 通胀偏高，政策收紧预期` })
    } else if (cpi != null && ppi != null) {
      insights.push({ level: 'neutral', text: `CPI ${cpi >= 0 ? '+' : ''}${cpi.toFixed(2)}% / PPI ${ppi >= 0 ? '+' : ''}${ppi.toFixed(2)}% 物价平稳` })
    }
  }

  if (insights.length === 0) {
    return <div className="empty-state">暂无足够数据生成解读（等待后端抓取宏观指标）</div>
  }

  return (
    <div className="insight-list">
      {insights.map((item, i) => (
        <div key={i} className={cn('insight-item', item.level === 'warn' && 'insight-warn', item.level === 'good' && 'insight-good')}>
          <span className={`insight-dot insight-dot-${item.level}`} />
          <span>{item.text}</span>
        </div>
      ))}
    </div>
  )
}

function _renderMacroVerdict(macro) {
  // 各维度打分：+1(利好) / 0(中性) / -1(利空)
  const scores = { total: 0, details: [] }

  // ── 汇率：人民币偏强→外资流入偏好(+1)；偏弱(-1)
  if (macro['美元/人民币'] != null) {
    const v = Number(macro['美元/人民币'])
    let s = 0, label = ''
    if (v < 7.0) { s = 1; label = '人民币偏强，利于外资流入' }
    else if (v > 7.3) { s = -1; label = '人民币承压，外资流出风险' }
    else { s = 0; label = '汇率中性区间' }
    scores.total += s; scores.details.push({ name: '汇率', s, label, val: `USD/CNY ${v.toFixed(2)}` })
  }

  // ── 流动性：SHIBOR 低=充裕(+1)；高=偏紧(-1)
  if (macro['Shibor隔夜'] != null) {
    const v = Number(macro['Shibor隔夜'])
    let s = 0, label = ''
    if (v < 1.5) { s = 1; label = '流动性充裕' }
    else if (v > 2.0) { s = -1; label = '资金面偏紧' }
    else { s = 0; label = '流动性中性' }
    scores.total += s; scores.details.push({ name: '流动性', s, label, val: `SHIBOR ${v.toFixed(2)}%` })
  }

  // ── 景气：PMI >=51 扩张(+1)；<50 收缩(-1)
  if (macro['制造业PMI'] != null) {
    const v = Number(macro['制造业PMI'])
    let s = 0, label = ''
    if (v >= 51) { s = 1; label = '制造业扩张向好' }
    else if (v < 50) { s = -1; label = '制造业收缩' }
    else { s = 0; label = '荣枯线附近' }
    scores.total += s; scores.details.push({ name: '景气', s, label, val: `PMI ${v.toFixed(1)}` })
  }

  // ── 物价：CPI 温和 0~2%(+1)；2~3% 温和偏高(0)；>3% 通胀(-1)；<0 通缩(-1)
  if (macro['CPI同比'] != null) {
    const v = Number(macro['CPI同比'])
    let s = 0, label = ''
    if (v >= 0 && v <= 2) { s = 1; label = '物价温和' }
    else if (v > 2 && v <= 3) { s = 0; label = '通胀温和偏高' }
    else if (v > 3) { s = -1; label = '通胀偏高' }
    else { s = -1; label = '通缩' }
    scores.total += s; scores.details.push({ name: '物价', s, label, val: `CPI ${v >= 0 ? '+' : ''}${v.toFixed(1)}%` })
  }

  // ── 利率环境：10Y 国债低=宽松(+1)；高=收紧(-1)
  if (macro['10Y国债收益率'] != null) {
    const v = Number(macro['10Y国债收益率'])
    let s = 0, label = ''
    if (v < 2.5) { s = 1; label = '利率低位，估值支撑强' }
    else if (v > 3.5) { s = -1; label = '利率偏高，估值承压' }
    else { s = 0; label = '利率中性' }
    scores.total += s; scores.details.push({ name: '利率', s, label, val: `10Y ${v.toFixed(2)}%` })
  }

  // ── 综合判定
  let verdict, verdictCls, verdictIcon, advice
  if (scores.total >= 3) {
    verdict = '适合积极交易'; verdictCls = 'verdict-good'; verdictIcon = '🟢'
    advice = '宏观环境整体偏多，流动性充裕+景气扩张+汇率稳定，可适当提高仓位参与市场。注意控制单票仓位上限与行业分散。'
  } else if (scores.total >= 1) {
    verdict = '可谨慎参与'; verdictCls = 'verdict-neutral'; verdictIcon = '🟡'
    advice = '宏观环境中性偏暖，部分维度有利但存在隐忧（见下方分项）。建议维持中等仓位，优选确定性高的标的，避免追高。'
  } else {
    verdict = '建议防守观望'; verdictCls = 'verdict-bad'; verdictIcon = '🔴'
    advice = '宏观环境偏空，多维度发出警示信号。建议降低仓位、增加现金比例、优先防御性板块，等待信号改善后再积极布局。'
  }

  return (
    <div className={`macro-verdict ${verdictCls}`}>
      <div className="verdict-header">
        <span className="verdict-icon">{verdictIcon}</span>
        <span className="verdict-title">{verdict}</span>
        <span className="verdict-score">综合评分: {scores.total > 0 ? '+' : ''}{scores.total} / 5</span>
      </div>
      <div className="verdict-advice">{advice}</div>
      <div className="verdict-breakdown">
        {scores.details.map((d) => (
          <div key={d.name} className={cn('verdict-item', d.s > 0 ? 'v-good' : d.s < 0 ? 'v-bad' : 'v-neutral')}>
            <span className="v-name">{d.name}</span>
            <span className="v-val">{d.val}</span>
            <span className="v-label">{d.label}</span>
            <span className="v-score">{d.s > 0 ? '+' : ''}{d.s}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function OverallEquityChart({ curve, lookback }) {
  const [hoverIdx, setHoverIdx] = useState(null)
  if (!curve || curve.length < 2) return null

  const W = 900
  const H = 280
  const PAD = { top: 25, right: 35, bottom: 35, left: 65 }
  const cw = W - PAD.left - PAD.right
  const ch = H - PAD.top - PAD.bottom

  const stratReturns = curve.map((d) => d.strategy_return ?? 0)
  const benchReturns = curve.map((d) => d.benchmark_return ?? 0)
  const allReturns = [...stratReturns, ...benchReturns, 0]
  let minRet = Math.min(...allReturns)
  let maxRet = Math.max(...allReturns)
  const padSpan = Math.max(2, (maxRet - minRet) * 0.1)
  minRet -= padSpan
  maxRet += padSpan

  const xScale = (i) => PAD.left + (i / (curve.length - 1)) * cw
  const yScale = (v) => PAD.top + ch - ((v - minRet) / (maxRet - minRet)) * ch

  const stratPath = curve.map((d, i) => `${i === 0 ? 'M' : 'L'}${xScale(i).toFixed(1)},${yScale(d.strategy_return).toFixed(1)}`).join(' ')
  const benchPath = curve.map((d, i) => `${i === 0 ? 'M' : 'L'}${xScale(i).toFixed(1)},${yScale(d.benchmark_return).toFixed(1)}`).join(' ')

  const baselineY = yScale(0)
  const areaBottomY = Math.min(PAD.top + ch, Math.max(PAD.top, baselineY))
  const stratAreaPath = `${stratPath} L${xScale(curve.length - 1).toFixed(1)},${areaBottomY.toFixed(1)} L${xScale(0).toFixed(1)},${areaBottomY.toFixed(1)} Z`

  const yTicksCount = 4
  const yTicks = Array.from({ length: yTicksCount + 1 }, (_, i) => {
    const v = minRet + ((maxRet - minRet) / yTicksCount) * i
    return { y: yScale(v), label: `${v >= 0 ? '+' : ''}${v.toFixed(1)}%` }
  })

  const step = Math.max(1, Math.floor(curve.length / 5))
  const xTicks = curve.filter((_, i) => i % step === 0 || i === curve.length - 1)

  const lastPoint = curve[curve.length - 1]
  const active = hoverIdx !== null && curve[hoverIdx] ? curve[hoverIdx] : lastPoint
  const activeIdx = hoverIdx !== null ? hoverIdx : curve.length - 1

  return (
    <div className="overall-equity-container">
      <div className="overall-chart-head">
        <div className="overall-chart-legend">
          <span className="legend-item legend-strat">
            <span className="legend-dot dot-strat"></span>
            策略累计收益: <strong>{active.strategy_return >= 0 ? '+' : ''}{active.strategy_return}%</strong>
          </span>
          <span className="legend-item legend-bench">
            <span className="legend-dot dot-bench"></span>
            沪深300基准: <strong>{active.benchmark_return >= 0 ? '+' : ''}{active.benchmark_return}%</strong>
          </span>
          <span className="legend-item legend-alpha">
            <span className="legend-dot dot-alpha"></span>
            超额 Alpha: <strong className={active.excess_return >= 0 ? 'text-up' : 'text-down'}>{active.excess_return >= 0 ? '+' : ''}{active.excess_return}%</strong>
          </span>
        </div>
        <div className="overall-chart-date-badge">
          📅 {active.date} {active.batch_return !== undefined ? `(当日 ${active.batch_return >= 0 ? '+' : ''}${active.batch_return}%)` : ''}
        </div>
      </div>

      <div className="overall-svg-wrap" onMouseLeave={() => setHoverIdx(null)}>
        <svg
          viewBox={`0 0 ${W} ${H}`}
          className="overall-equity-svg"
          onMouseMove={(e) => {
            const rect = e.currentTarget.getBoundingClientRect()
            const mouseX = e.clientX - rect.left
            const relX = (mouseX / rect.width) * W - PAD.left
            const idx = Math.round((relX / cw) * (curve.length - 1))
            if (idx >= 0 && idx < curve.length) setHoverIdx(idx)
          }}
        >
          <defs>
            <linearGradient id="stratAreaGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#2563eb" stopOpacity="0.22" />
              <stop offset="100%" stopColor="#2563eb" stopOpacity="0.0" />
            </linearGradient>
          </defs>

          {yTicks.map((t, i) => (
            <g key={i}>
              <line x1={PAD.left} y1={t.y} x2={W - PAD.right} y2={t.y} stroke="rgba(0,0,0,0.06)" />
              <text x={PAD.left - 8} y={t.y + 4} textAnchor="end" fill="rgba(0,0,0,0.4)" fontSize="10">
                {t.label}
              </text>
            </g>
          ))}

          <line x1={PAD.left} y1={baselineY} x2={W - PAD.right} y2={baselineY} stroke="rgba(0,0,0,0.22)" strokeDasharray="4 3" />
          <text x={W - PAD.right + 4} y={baselineY + 3} fill="rgba(0,0,0,0.35)" fontSize="9">0%</text>

          <path d={stratAreaPath} fill="url(#stratAreaGrad)" />
          <path d={benchPath} fill="none" stroke="#94a3b8" strokeWidth="1.6" strokeDasharray="4 4" opacity="0.85" />
          <path d={stratPath} fill="none" stroke="#2563eb" strokeWidth="2.4" strokeLinejoin="round" strokeLinecap="round" />

          {hoverIdx !== null && (
            <g>
              <line x1={xScale(activeIdx)} y1={PAD.top} x2={xScale(activeIdx)} y2={PAD.top + ch} stroke="#64748b" strokeWidth="1" strokeDasharray="3 3" />
              <circle cx={xScale(activeIdx)} cy={yScale(active.strategy_return)} r="4.5" fill="#2563eb" stroke="#fff" strokeWidth="2" />
              <circle cx={xScale(activeIdx)} cy={yScale(active.benchmark_return)} r="3.5" fill="#94a3b8" stroke="#fff" strokeWidth="1.5" />
            </g>
          )}

          {hoverIdx === null && (
            <circle cx={xScale(curve.length - 1)} cy={yScale(lastPoint?.strategy_return ?? 0)} r="4" fill="#2563eb" />
          )}

          {xTicks.map((d, i) => {
            const idx = curve.indexOf(d)
            const isMultiYear = curve.length > 1 && curve[curve.length - 1].date.slice(0, 4) !== curve[0].date.slice(0, 4)
            return (
              <text key={i} x={xScale(idx)} y={H - 10} textAnchor="middle" fill="rgba(0,0,0,0.4)" fontSize="10">
                {isMultiYear ? d.date.slice(0, 7) : d.date.slice(5)}
              </text>
            )
          })}
        </svg>
      </div>
    </div>
  )
}

function EquityChart({ equity, initialCapital, summary }) {
  const [hoverIdx, setHoverIdx] = useState(null)
  if (!equity || equity.length === 0) return null

  // 关键优化：智能剔除信号日之前的 80+ 个无交易平直线条，展现真实的持有期前向收益曲线
  const startDt = summary?.signal_start || summary?.start
  let activeEquity = equity
  if (startDt) {
    const sIdx = equity.findIndex((e) => e.date >= startDt)
    if (sIdx > 0) {
      activeEquity = equity.slice(sIdx - 1)
    }
  } else {
    const firstActive = equity.findIndex((e) => Math.abs((e.total ?? 0) - initialCapital) > 1e-4 || (e.holding_value ?? 0) > 0)
    if (firstActive > 0) {
      activeEquity = equity.slice(firstActive - 1)
    }
  }

  if (activeEquity.length < 2) {
    return (
      <div className="bt-empty" style={{ margin: '14px 0' }}>
        <div className="bt-empty-icon">⏳</div>
        <div className="bt-empty-title">持仓第 1 天（初始净值 ¥100,000）</div>
        <div className="bt-empty-desc">当前信号日刚进入前向持有窗口，尚未形成连续折线，待持有天数累计后展现动态收益曲线。</div>
      </div>
    )
  }

  const values = activeEquity.map((d) => d.total)
  const allFlat = values.every((v) => Math.abs(v - initialCapital) < 1e-6)
  if (allFlat) {
    return (
      <div className="bt-empty">
        <div className="bt-empty-icon">📈</div>
        <div className="bt-empty-title">该信号日处于建仓或无成交态</div>
        <div className="bt-empty-desc">当前持有窗口内暂无成交或持仓净值变动。</div>
      </div>
    )
  }

  const W = 760
  const H = 260
  const PAD = { top: 20, right: 30, bottom: 35, left: 65 }
  const cw = W - PAD.left - PAD.right
  const ch = H - PAD.top - PAD.bottom

  const minY = Math.min(...values, initialCapital) * 0.992
  const maxY = Math.max(...values, initialCapital) * 1.008

  const xScale = (i) => PAD.left + (i / (activeEquity.length - 1)) * cw
  const yScale = (v) => PAD.top + ch - ((v - minY) / (maxY - minY)) * ch

  const linePath = activeEquity.map((d, i) => `${i === 0 ? 'M' : 'L'}${xScale(i).toFixed(1)},${yScale(d.total).toFixed(1)}`).join(' ')
  const baselineY = yScale(initialCapital)
  const lastVal = values[values.length - 1]
  const isPos = lastVal >= initialCapital
  const curveColor = isPos ? '#16a34a' : '#dc2626'
  const gradId = `eqGrad_${summary?.date || 'batch'}`

  const areaBottomY = PAD.top + ch
  const areaPath = `${linePath} L${xScale(activeEquity.length - 1).toFixed(1)},${areaBottomY.toFixed(1)} L${xScale(0).toFixed(1)},${areaBottomY.toFixed(1)} Z`

  const ticks = 4
  const yTicks = Array.from({ length: ticks + 1 }, (_, i) => {
    const v = minY + ((maxY - minY) / ticks) * i
    return { y: yScale(v), label: (v / 10000).toFixed(2) + '万' }
  })

  const xTicks = activeEquity.filter((_, i) => i % Math.max(1, Math.floor(activeEquity.length / 6)) === 0 || i === activeEquity.length - 1)
  const active = hoverIdx !== null && activeEquity[hoverIdx] ? activeEquity[hoverIdx] : activeEquity[activeEquity.length - 1]
  const activeIdx = hoverIdx !== null ? hoverIdx : activeEquity.length - 1
  const activeReturn = (((active.total - initialCapital) / initialCapital) * 100).toFixed(2)

  const ddValues = activeEquity.map((d) => d.drawdown ?? 0)
  const ddMin = Math.min(...ddValues)
  const ddH = 40
  const ddYScale = (v) => (Math.abs(v) / Math.abs(ddMin || 1)) * ddH

  return (
    <div className="equity-chart-wrap" onMouseLeave={() => setHoverIdx(null)}>
      <div className="equity-chart-legend">
        <span>持仓净值走势: <strong>{(active.total / 10000).toFixed(2)}万</strong></span>
        <span>累计收益: <strong className={activeReturn >= 0 ? 'text-up' : 'text-down'}>{activeReturn >= 0 ? '+' : ''}{activeReturn}%</strong></span>
        <span>当前持有日: <strong>{active.date}</strong></span>
      </div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="equity-svg"
        onMouseMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect()
          const mouseX = e.clientX - rect.left
          const relX = (mouseX / rect.width) * W - PAD.left
          const idx = Math.round((relX / cw) * (activeEquity.length - 1))
          if (idx >= 0 && idx < activeEquity.length) setHoverIdx(idx)
        }}
      >
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={curveColor} stopOpacity="0.2" />
            <stop offset="100%" stopColor={curveColor} stopOpacity="0.0" />
          </linearGradient>
        </defs>

        {yTicks.map((t, i) => (
          <g key={i}>
            <line x1={PAD.left} y1={t.y} x2={W - PAD.right} y2={t.y} stroke="rgba(0,0,0,0.06)" />
            <text x={PAD.left - 8} y={t.y + 4} textAnchor="end" fill="rgba(0,0,0,0.4)" fontSize="10">
              {t.label}
            </text>
          </g>
        ))}

        <line x1={PAD.left} y1={baselineY} x2={W - PAD.right} y2={baselineY} stroke="rgba(0,0,0,0.22)" strokeDasharray="4 3" opacity="0.6" />
        <text x={W - PAD.right - 4} y={baselineY - 5} fill="rgba(0,0,0,0.4)" textAnchor="end" fontSize="10">
          初始本金 10.0万
        </text>

        <path d={areaPath} fill={`url(#${gradId})`} />
        <path d={linePath} fill="none" stroke={curveColor} strokeWidth="2.4" strokeLinejoin="round" strokeLinecap="round" />

        {hoverIdx !== null && (
          <g>
            <line x1={xScale(activeIdx)} y1={PAD.top} x2={xScale(activeIdx)} y2={PAD.top + ch} stroke="#64748b" strokeWidth="1" strokeDasharray="3 3" />
            <circle cx={xScale(activeIdx)} cy={yScale(active.total)} r="4.5" fill={curveColor} stroke="#fff" strokeWidth="2" />
          </g>
        )}
        {hoverIdx === null && (
          <circle cx={xScale(activeEquity.length - 1)} cy={yScale(lastVal)} r="4" fill={curveColor} />
        )}

        {xTicks.map((d, i) => {
          const idx = activeEquity.indexOf(d)
          return (
            <text key={i} x={xScale(idx)} y={H - 8} textAnchor="middle" fill="rgba(0,0,0,0.4)" fontSize="10">
              {d.date.slice(5)}
            </text>
          )
        })}
      </svg>

      {ddMin < -0.01 ? (
        <div className="dd-bar-wrap">
          <span className="dd-label">回撤</span>
          <svg viewBox={`0 0 ${W} ${ddH + 10}`} className="dd-svg">
            {activeEquity.map((d, i) => {
              const v = d.drawdown ?? 0
              if (v === 0) return null
              return <rect key={i} x={xScale(i) - 2} y={0} width="4" height={ddYScale(v)} fill="#dc2626" opacity="0.4" rx="1" />
            })}
          </svg>
        </div>
      ) : null}
    </div>
  )
}

// 策略 id → 展示标签。id 集合必须与后端 factor_types.STRATEGY_ORDER 一致
// （tests/test_frontend_registry_parity.py 守卫）；缺项时调用方有 `|| s` 兜底，
// 但 top-tag 配色需要 styles.css 里对应的 .top-tag--<id> 规则。
// ⚠️ 键是**小写策略 id**，而 DAL「来源策略」是后端 STRATEGY_LABEL（如 'PVCorr20'）——
// 查表前必须先 .trim().toLowerCase()，否则恒 miss（只显示裸标签、丢掉中文类型后缀；
// 这类静默降级 test_frontend_registry_parity.py 测不到）。
const STRAT_LABEL = {
  // A 批：因子池存活价格因子（菜单当前全集）
  pvcorr20: 'PVCorr20 量价相关',
  pvcorr60: 'PVCorr60 量价相关',
  cvamt20: 'CVAmt20 成交稳定',
  cvamt60: 'CVAmt60 成交稳定',
  skew20: 'Skew20 收益偏度',
  skew60: 'Skew60 收益偏度',
  vratio20_120: 'VRatio20_120 波动比',
  distlo10: 'DistLo10 距低点',
  distlo60: 'DistLo60 距低点',
  maxret10: 'MaxRet10 极值日',
  illiq20: 'Illiq20 非流动性',
}

// 回测持仓的退出原因 → 中文标签（说明仓位按市场信号平仓，非固定持有）
const EXIT_LABEL = {
  take_band: '上轨止盈',
  take_pct: '固定止盈',
  trailing: '移动止盈',
  stop_hard: '硬止损',
  stop_pct: '止损',
  stop_band: '下轨止损',
  trend_break: '趋势破位',
  max_hold: '持有期满',
}

// 异常成交判定（与后端 /api/backtests/daily-latest 同步）：
// ① 同日买卖（违反 A股 T+1，旧引擎退出分支无最少持有保护）；
// ② 止盈类退出(take_band/take_pct)却亏损（旧引擎把布林上轨止盈套到动量入场，上轨<成本）。
// 这两类均为旧引擎历史脏数据，已由后端剔除；此处做防御性兜底（兼容未重新部署的旧接口）。
function isCorruptTrade(t) {
  const bd = String(t.buy_date ?? '').trim()
  const sd = String(t.sell_date ?? '').trim()
  if (bd && sd && bd === sd) return true
  const er = String(t.exit_reason ?? '').trim()
  const rp = Number(t.return_pct ?? 0)
  if ((er === 'take_band' || er === 'take_pct') && rp < 0) return true
  return false
}

// 按退出原因分组统计卡片（总体视角与按信号日视角复用）：
// 统计每个退出原因的笔数 / 盈 / 亏 / 平均收益率，带红绿胜率条形。
function ReasonBreakdown({ trades }) {
  const clean = (trades || []).filter((t) => !isCorruptTrade(t))
  const total = clean.length
  if (total === 0) return null
  const reasonMap = {}
  for (const t of clean) {
    const er = t.exit_reason || 'max_hold'
    const rp = Number(t.return_pct ?? 0)
    if (!reasonMap[er]) reasonMap[er] = { count: 0, wins: 0, loss: 0, sum: 0 }
    reasonMap[er].count++
    if (rp >= 0) reasonMap[er].wins++; else reasonMap[er].loss++
    reasonMap[er].sum += rp
  }
  const reasonRows = Object.entries(reasonMap)
    .map(([er, v]) => ({ er, ...v, avg: v.count ? v.sum / v.count : 0 }))
    .sort((a, b) => b.count - a.count)
  return (
    <div className="reason-breakdown">
      <div className="rb-head">按退出原因统计（共 {total} 笔）</div>
      <div className="rb-grid">
        {reasonRows.map((r) => (
          <div key={r.er} className="rb-item">
            <div className="rb-top">
              <span className="rb-label">{EXIT_LABEL[r.er] || r.er}</span>
              <span className="rb-count">{r.count} 笔</span>
            </div>
            <div className="rb-bar">
              <span className="rb-win" style={{ width: `${r.count ? Math.round(r.wins / r.count * 100) : 0}%` }} />
            </div>
            <div className="rb-meta">
              <span className="rb-win-txt">盈 {r.wins}</span>
              <span className="rb-loss-txt">亏 {r.loss}</span>
              <span className={r.avg >= 0 ? 'ret-win-inline' : 'ret-loss-inline'}>{r.avg >= 0 ? '+' : ''}{r.avg.toFixed(2)}%</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// 持仓成交的「全部 / 盈利 / 亏损」筛选分段控件
function FilterSeg({ value, onChange }) {
  const opts = [['all', '全部'], ['win', '盈利'], ['loss', '亏损']]
  return (
    <div className="seg">
      {opts.map(([v, lbl]) => (
        <button key={v} type="button" className={`seg-btn${value === v ? ' seg-active' : ''}`} onClick={() => onChange(v)}>{lbl}</button>
      ))}
    </div>
  )
}

function App() {
  const [activeView, setActiveView] = useState('trades')
  const [dashboard, setDashboard] = useState(null)
  const [refreshing, setRefreshing] = useState(false)
  const [nowTick, setNowTick] = useState(Date.now())
  const [artifacts, setArtifacts] = useState(null)
  const [portfolio, setPortfolio] = useState(null)
  const [selHold, setSelHold] = useState(-1)
  const [holdOpen, setHoldOpen] = useState(false)
  const [holdFilter, setHoldFilter] = useState('all') // 持仓成交筛选：all | win | loss
  const [excludedTrades, setExcludedTrades] = useState(0) // 后端剔除的异常成交数
  const [backtest, setBacktest] = useState(null)
  // 2026-09-29：原「个股分析」状态（analysis / analysisCode / openAnalysis 等）
  // 已整体下架——重内存端点 /api/analysis 在云端 RENDER_LITE 下本就 503，
  // 且唯一调用方是该面板本身；个股视角改为读 CI 舆情产物（日报页消息面）。
  // 2026-09-17：原「交互式选股」状态（candidateCodes / selectionParams / selectionScan /
  // fusionResult / backtestRun）已整体退役——网站不再自带扫描/融合/回测链路，
  // 选股结果一律读 CI 产出的融合清单（Daily-Action-List，见下方 dalRows）。
  const [dailyBacktests, setDailyBacktests] = useState([])
  const [dailySummary, setDailySummary] = useState(null)
  const [summaryLookback, setSummaryLookback] = useState(30)  // 总体总结聚合窗口：30 / 60 / 250(近一年)。2026-09-30 用户定版：30 天一个周期看
  const [summaryLoading, setSummaryLoading] = useState(false)   // 切换窗口时的加载状态
  const [selDaily, setSelDaily] = useState(0)
  const summaryCacheRef = useRef({})
  const initialBtDateSetRef = useRef(false)
  const [btDateOpen, setBtDateOpen] = useState(false)    // 回测日期选择器是否展开
  const [activeCycleKey, setActiveCycleKey] = useState('all')
  const [selectedStrategyModel, setSelectedStrategyModel] = useState('world_model') // 'world_model' | 'adaptive'
  const [tradeForm, setTradeForm] = useState({
    date: localDateStr(),
    code: '', name: '', side: 'buy', price: 0, qty: 100, fee: 0, notes: '',
  })
  const [error, setError] = useState('')
  const [backendDown, setBackendDown] = useState(false) // 持续性状态（驱动「离线」chip），不随 toast 自动消失
  const [adminHeight, setAdminHeight] = useState(1100) // 管理页 iframe 自适应高度（admin 页 postMessage 上报）
  const [diag, setDiag] = useState(null) // 连通性自诊断面板：null=关闭 {running, rows, build, deploy}
  const [adminStaticHost, setAdminStaticHost] = useState(false) // 静态托管（CF Pages）检测：/health 非 JSON 即静态
  const [dbStatus, setDbStatus] = useState(null)
  const [fullDaily, setFullDaily] = useState(null)
  const [newsSurface, setNewsSurface] = useState(null)   // 消息面/市场舆情（/api/artifacts/news-surface）
  const [dailyDate, setDailyDate] = useState(null)        // 当前查看的日报日期(YYYYMMDD)，null=最新
  const [dailyDates, setDailyDates] = useState([])         // 可选日报日期列表
  const [dateOpen, setDateOpen] = useState(false)
  const [expandSignals, setExpandSignals] = useState(false)          // 日期下拉框是否展开
  const [liveIndices, setLiveIndices] = useState(null)     // 实时大盘指数
  const [liveBreadth, setLiveBreadth] = useState(null)     // 实时市场宽度
  const [liveStocks, setLiveStocks] = useState({})         // 实时个股报价缓存 { [code]: quote }
  const [lastLiveTime, setLastLiveTime] = useState('')     // 最近一次实时刷新时间戳

  // 2026-09-17：原「交互式选股链路」已整体退役——scanPhase / bollTaskId / fusionTaskId /
  // btTaskId / scanProgress / scanLogs / logContainerRef / isRunning，以及 startFusion /
  // startBacktest / cancelTask 与三个 500ms 轮询 effect，全部移除。
  // 退役理由：① 链路起点 setBollTaskId 从未被调用 → 整条 scan→fusion→backtest 恒不可达；
  // ② /api/selection/boll-scan 是注册表之外的**第二套选股实现**，与 fusion 口径分叉；
  // ③ 相关端点在生产 RENDER_LITE=1 下全部 503。
  // 现在的选股逻辑 = CI：daily-pick.yml → 12 个因子池 CSV → fusion → Daily-Action-List，
  // 网站只读这份清单（下方 dalRows），不参与任何计算。
  // 回测同样只看 CI 的「每日自动回测 · 前向信号回测」批次。

  // 信号日下拉：以全部 DAL 日期为基准。徽章三态（2026-09-29）：
  // 已装载 → hold 天数；未装载但后端有批次（has_backtest）→「已完结」+ 点选懒加载；
  // 无批次且窗口未走完 →「持仓中」；无批次且窗口已走完 →「缺失」（管线缺口，待 CI 补算）。
  // 旧逻辑把「未装载进 14 批窗口」一律标「持仓中」，回填全年批次后 4 个月前的日期也挂持仓中。
  const btDateMap = useMemo(() => new Map((dailyBacktests || []).map((d) => [d.date, d])), [dailyBacktests])
  const selectedBtDate = dailyDates[selDaily]?.date
  const selectedBacktest = selectedBtDate ? btDateMap.get(selectedBtDate) : null
  // 持有天数基准：取最新已装载批次的 hold_days（避免前端硬编码，回退 12 = CI HOLD_DAYS）
  const btHoldDays = Number(dailyBacktests?.[0]?.summary?.hold_days) || 12
  const [btBatchLoading, setBtBatchLoading] = useState(null)
  const daysSinceDate = (tag) => {
    if (!tag || tag.length !== 8) return 9999
    const d = new Date(Number(tag.slice(0, 4)), Number(tag.slice(4, 6)) - 1, Number(tag.slice(6)))
    return Math.floor((Date.now() - d.getTime()) / 86400000)
  }

  useEffect(() => { loadDailyBacktest() }, [])
  useEffect(() => { loadDailyDates(); reloadArtifacts() }, [])
  // 自动对齐初始信号日：优先选择最新一条「已有交易记录或完整完结」的信号日（而不是0笔交易或未完结日）
  useEffect(() => {
    if (initialBtDateSetRef.current || !dailyDates || dailyDates.length === 0) return
    let idx = dailyDates.findIndex((d) => {
      const bt = btDateMap.get(d.date)
      return (bt?.summary?.num_trades ?? 0) > 0
    })
    if (idx < 0) {
      idx = dailyDates.findIndex((d) => d.has_backtest || btDateMap.has(d.date))
    }
    if (idx >= 0) {
      setSelDaily(idx)
      initialBtDateSetRef.current = true
    }
  }, [dailyDates, btDateMap])
  // 选中「已完结但未装载」的历史信号日 → 自动懒加载单批次（覆盖一切选择路径）
  useEffect(() => {
    const it = dailyDates[selDaily]
    if (it?.has_backtest && !btDateMap.has(it.date)) loadBatchForDate(it.date)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedBtDate, dailyDates])
  // 日期选择器：点击外部关闭
  useEffect(() => {
    if (!dateOpen) return
    const handler = (e) => {
      if (!e.target.closest('.date-picker-wrap')) setDateOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [dateOpen])

  // 加载每日 CI 自动回测结果（全部历史信号日前向回测批次）
  async function loadDailyBacktest() {
    // 静态托管（CF Pages）回退：API 不可达或返回 HTML 时读 web_data 快照
    const safeJson = async (url, staticFile) => {
      try {
        const r = await fetch(url, { cache: 'no-store' })
        if (r.ok) return await r.json()
      } catch { /* 走静态回退 */ }
      try {
        const r2 = await fetch(`web_data/${staticFile}`, { cache: 'no-store' })
        return r2.ok ? await r2.json() : null
      } catch { return null }
    }
    // 1. 加载最近已完成的批次（首屏 30 批）
    safeJson('/api/backtests/daily-latest', 'daily_items.json').then((data) => {
      if (data) {
        setDailyBacktests(data.items || [])
        setExcludedTrades(Number(data.excluded_trades ?? 0))
      }
    })
    // 2. 并行预取 30 / 60 / 250 天三个维度的总体指标并写入内存缓存，切换 0 延迟无转圈
    Promise.all([
      fetch('web_data/daily_summary_30.json').then(r => r.ok ? r.json() : null).catch(() => null),
      fetch('web_data/daily_summary_60.json').then(r => r.ok ? r.json() : null).catch(() => null),
      fetch('web_data/daily_summary_250.json').then(r => r.ok ? r.json() : null).catch(() => null),
    ]).then(([s30, s60, s250]) => {
      if (s30) summaryCacheRef.current[30] = s30
      if (s60) summaryCacheRef.current[60] = s60
      if (s250) summaryCacheRef.current[250] = s250
      const initData = summaryCacheRef.current[summaryLookback] || s30
      if (initData) setDailySummary(initData)
    })
  }

  // 懒加载单个历史批次（优先走 CDN 静态批次库，0 延迟极速响应）
  async function loadBatchForDate(date) {
    if (!date || btDateMap.has(date) || btBatchLoading) return
    setBtBatchLoading(date)
    try {
      // 1. 优先直接从静态快照取（CF Pages CDN 瞬间返回）
      const r = await fetch(`web_data/batches/${date}.json`)
      const ct = r.headers.get('content-type') || ''
      if (r.ok && ct.includes('json')) {
        const d = await r.json()
        if (d.batch) {
          setDailyBacktests((prev) => (prev || []).some((x) => x.date === date) ? prev : [...(prev || []), d.batch])
          return
        }
      }
    } catch {}
    // 2. 回退到 API 接口
    try {
      const r2 = await fetch(`/api/backtests/daily-batch?date=${date}`)
      const ct2 = r2.headers.get('content-type') || ''
      if (r2.ok && ct2.includes('json')) {
        const d2 = await r2.json()
        if (d2.batch) {
          setDailyBacktests((prev) => (prev || []).some((x) => x.date === date) ? prev : [...(prev || []), d2.batch])
        }
      }
    } catch {}
    finally { setBtBatchLoading(null) }
  }

  // 切换总体总结的聚合窗口（30 / 60 / 近一年）后重新拉取总体指标。
  // 内存缓存优先：已缓存时 0ms 瞬间响应，绝不出现「计算中…」或转圈遮罩
  async function reloadDailySummary(lookback = summaryLookback) {
    const lb = lookback ?? summaryLookback
    if (summaryCacheRef.current[lb]) {
      setDailySummary(summaryCacheRef.current[lb])
      setSummaryLoading(false)
      return
    }
    setSummaryLoading(true)
    try {
      const r = await fetch(`web_data/daily_summary_${lb}.json`)
      const ct = r.headers.get('content-type') || ''
      if (r.ok && ct.includes('json')) {
        const d = await r.json()
        summaryCacheRef.current[lb] = d
        setDailySummary(d)
        return
      }
    } catch {}
    try {
      const sresp = await fetch(`/api/backtests/daily-summary?lookback=${lb}`)
      const ct = sresp.headers.get('content-type') || ''
      if (sresp.ok && ct.includes('json')) {
        const sdata = await sresp.json()
        summaryCacheRef.current[lb] = sdata
        setDailySummary(sdata)
        return
      }
    } catch { /* 忽略加载失败 */ }
    try {
      const r3 = await fetch('web_data/daily_summary.json')
      const ct3 = r3.headers.get('content-type') || ''
      if (r3.ok && ct3.includes('json')) {
        const sdata = await r3.json()
        setDailySummary(sdata)
      }
    } catch { /* 忽略静态回退失败 */ }
    finally { setSummaryLoading(false) }
  }

  async function reloadArtifacts(date) {
    try {
      const fetchWithFallback = async (url, fallbackFile) => {
        try {
          const r = await fetch(url)
          const ct = r.headers.get('content-type') || ''
          if (r.ok && ct.includes('json')) return await r.json()
        } catch {}
        try {
          const r2 = await fetch(`web_data/${fallbackFile}`)
          const ct2 = r2.headers.get('content-type') || ''
          if (r2.ok && ct2.includes('json')) return await r2.json()
        } catch {}
        return null
      }
      const [a, f, n] = await Promise.all([
        fetchWithFallback('/api/artifacts/daily-action-list', 'artifacts.json'),
        fetchWithFallback(`/api/artifacts/daily-action-list/full${date ? `?date=${date}` : ''}`, date ? `dals/${date}.json` : 'daily_full.json'),
        fetchWithFallback('/api/artifacts/news-surface', 'news_surface.json'),
      ])
      if (a) setArtifacts(a)
      if (f) {
        setFullDaily(f)
        if (date && f.latest?.name) setDailyDate(date)
      }
      if (n) setNewsSurface(n)
    } catch { /* 忽略刷新失败 */ }
  }

  // 加载全部历史日报日期，供日报页「日期选择器」切换
  async function loadDailyDates() {
    try {
      const resp = await fetch('/api/artifacts/daily-action-list/dates')
      const ct = resp.headers.get('content-type') || ''
      if (resp.ok && ct.includes('json')) {
        const d = await resp.json()
        setDailyDates(d.items || [])
        return
      }
    } catch { /* 忽略 */ }
    try {
      const r2 = await fetch('web_data/daily_dates.json')
      const ct2 = r2.headers.get('content-type') || ''
      if (r2.ok && ct2.includes('json')) {
        const d = await r2.json()
        setDailyDates(d.items || [])
      }
    } catch { /* 忽略静态回退失败 */ }
  }

  useEffect(() => {
    if (!error) return
    const timer = setTimeout(() => setError(''), 5000)
    return () => clearTimeout(timer)
  }, [error])

  // 静态托管检测：/health 应返回 JSON（Render 后端）；返回 HTML/404 = 静态托管
  //（CF Pages）——管理 Tab 改为外链 Render 管理页，避免 iframe 里再套一层 SPA
  useEffect(() => {
    if (activeView !== 'admin') return
    let alive = true
    fetch('/health', { cache: 'no-store' })
      .then((r) => r.headers.get('content-type') || '')
      .then((ct) => { if (alive && !ct.includes('json')) setAdminStaticHost(true) })
      .catch(() => { if (alive) setAdminStaticHost(true) })
    return () => { alive = false }
  }, [activeView])

  // 管理页 iframe 高度联动：admin.html 嵌入模式上报内容高度，iframe 撑到内容高，
  // 消除「iframe 内滚动 + 滚动位置错乱」的双重滚动体验
  useEffect(() => {
    if (activeView !== 'admin') return
    const onMsg = (e) => {
      const d = e.data
      if (d && d.type === 'admin-height' && e.origin === window.location.origin
          && typeof d.h === 'number' && d.h > 300 && d.h < 30000) {
        setAdminHeight(d.h)
      }
    }
    window.addEventListener('message', onMsg)
    return () => window.removeEventListener('message', onMsg)
  }, [activeView])

  // 看板数据加载（宏观/组合/日报等）——页面加载时拉一次，手动刷新按钮按需触发
  const dashboardCtrl = useRef(null)
  const loadDashboard = useCallback(async () => {
    if (dashboardCtrl.current) dashboardCtrl.current.abort()
    const controller = new AbortController()
    dashboardCtrl.current = controller
    const signal = controller.signal
    // 冷缓存时后端同步拉行情可达 1-2 分钟：45s 后放弃本次等待（后端 SWR 继续填缓存，下次刷新即命中）
    let timedOut = false
    const timeoutTimer = setTimeout(() => { timedOut = true; controller.abort() }, 45000)
    try {
      setError('')
      setRefreshing(true)
      // 每个请求独立命名 + no-store（绕过浏览器 HTTP 缓存）+ 静态快照回退：
      // API 失败（部署重启/后端离线）时改读 web_data/*.json 静态快照
      // （数据静态化产物，与 API 响应结构一致）——部署窗口不再白屏/报错
      const degraded = []
      const fetchRes = async (apiPath, staticFile, name) => {
        try {
          const r = await fetch(apiPath, { signal, cache: 'no-store' })
          const ct = r.headers.get('content-type') || ''
          if (!r.ok || !ct.includes('json')) throw new Error(`${name}(HTTP ${r.status})`)
          return await r.json()
        } catch (e1) {
          if (signal.aborted) throw e1 // 45s 超时/切换视图：不做静态回退，直接上抛
          try {
            const r2 = await fetch(`web_data/${staticFile}`, { cache: 'no-store' })
            const ct2 = r2.headers.get('content-type') || ''
            if (!r2.ok || !ct2.includes('json')) throw e1
            degraded.push(`${name}来自静态快照`)
            return await r2.json()
          } catch {
            throw e1
          }
        }
      }
      const [d, a, p, b] = await Promise.all([
        fetchRes('/api/dashboard', 'dashboard.json', '看板'),
        fetchRes('/api/artifacts/daily-action-list', 'artifacts.json', '日报索引'),
        fetchRes('/api/portfolio', 'portfolio.json', '持仓'),
        fetchRes('/api/backtests/latest', 'backtests_latest.json', '回测'),
      ])
      setDashboard(d)
      setArtifacts(a)
      setPortfolio(p)
      setBacktest(b)
      setBackendDown(false) // 主数据成功 = 后端在线；尾部辅助接口失败不再误报「后端未启动」
      if (degraded.length) console.info('[Snapshot] Loaded from static snapshot:', degraded)
    } catch (err) {
      if (err.name !== 'AbortError') {
        setError('后端连接失败（' + (err.message || '网络异常') + '）'); setBackendDown(true)
      } else if (timedOut) { setError('刷新超时：后端正在拉取行情数据，请稍后再试') }
    }
    finally { clearTimeout(timeoutTimer); setRefreshing(false) }
  }, [adminStaticHost])

  // 后端存活探测（自愈）：「离线」chip 与重数据解耦——/health 轻量端点 60s 一探。
  // 背景：Render 部署重启窗口里页面拉数据失败一次，「离线」就锁死（数据是最后一次
  // 成功加载的，看起来正常但 chip 卡在离线）。现在：恢复后 60s 内自动翻回「在线」，
  // 并自动补拉一次看板数据（部署后自愈，不再需要手动刷新）。
  const backendDownRef = useRef(false)
  useEffect(() => { backendDownRef.current = backendDown }, [backendDown])
  useEffect(() => {
    let alive = true
    let timer = null
    const probe = async () => {
      const ctl = new AbortController()
      const to = setTimeout(() => ctl.abort(), 8000)
      let up = false
      try {
        const r = await fetch('/health', { cache: 'no-store', signal: ctl.signal })
        const ct = r.headers.get('content-type') || ''
        if (r.ok && ct.includes('json')) {
          up = true
        }
      } catch { up = false }
      if (!up) {
        try {
          const r2 = await fetch('web_data/health.json', { cache: 'no-store', signal: ctl.signal })
          const ct2 = r2.headers.get('content-type') || ''
          if (r2.ok && ct2.includes('json')) {
            up = true
          }
        } catch { up = false }
      }
      clearTimeout(to)
      if (!alive) return
      const wasDown = backendDownRef.current
      setBackendDown(!up)
      if (up && wasDown) loadDashboard()
      timer = setTimeout(probe, 60000)
    }
    probe()
    return () => { alive = false; if (timer) clearTimeout(timer) }
  }, [loadDashboard])

  // 连通性自诊断：逐端点测状态码+耗时（串行，时延清晰），一屏给出完整结论
  const runDiag = async () => {
    setDiag({ running: true, rows: [], build: BUILD_SHA, deploy: dbStatus && dbStatus.backend_commit || '' })
    const targets = [
      ['健康检查 /health', '/health', 'web_data/health.json'],
      ['看板 /api/dashboard', '/api/dashboard', 'web_data/dashboard.json'],
      ['日报索引', '/api/artifacts/daily-action-list', 'web_data/artifacts.json'],
      ['持仓 /api/portfolio', '/api/portfolio', 'web_data/portfolio.json'],
      ['最新回测', '/api/backtests/latest', 'web_data/backtests_latest.json'],
      ['融合清单全量', '/api/artifacts/daily-action-list/full', 'web_data/daily_full.json'],
    ]
    const rows = []
    for (const [label, url, fallback] of targets) {
      const t0 = performance.now()
      const ctl = new AbortController()
      const to = setTimeout(() => ctl.abort(), 10000)
      let ok = false
      let status = 0
      let note = ''
      try {
        const r = await fetch(url, { cache: 'no-store', signal: ctl.signal })
        const ct = r.headers.get('content-type') || ''
        if (r.ok && ct.includes('json')) {
          ok = true
          status = r.status
          note = 'API 直连'
        } else if (fallback) {
          const r2 = await fetch(fallback, { cache: 'no-store', signal: ctl.signal })
          const ct2 = r2.headers.get('content-type') || ''
          if (r2.ok && ct2.includes('json')) {
            ok = true
            status = 200
            note = 'Edge 镜像'
          } else {
            status = r.status
          }
        } else {
          status = r.status
        }
      } catch (e) {
        if (fallback) {
          try {
            const r2 = await fetch(fallback, { cache: 'no-store', signal: ctl.signal })
            const ct2 = r2.headers.get('content-type') || ''
            if (r2.ok && ct2.includes('json')) {
              ok = true
              status = 200
              note = 'Edge 镜像'
            }
          } catch {}
        }
        if (!ok) {
          note = e.name === 'AbortError' ? '超时' : (e.message || '网络失败')
        }
      }
      clearTimeout(to)
      rows.push({ name: label, ok, status, ms: Math.round(performance.now() - t0), err: ok ? note : (note || '不可达') })
      setDiag({ running: true, rows: [...rows], build: BUILD_SHA, deploy: dbStatus && dbStatus.backend_commit || '' })
    }
    setDiag({ running: false, rows, build: BUILD_SHA, deploy: dbStatus && dbStatus.backend_commit || '' })
  }

  // 首次加载（宏观数据日更，无需轮询；用户可手动点击刷新）
  useEffect(() => {
    loadDashboard()
  }, [loadDashboard])

  // 每 30 秒更新一次「X 分钟前」相对时间显示
  useEffect(() => {
    const t = setInterval(() => setNowTick(Date.now()), 30 * 1000)
    return () => clearInterval(t)
  }, [])

  // 加载完整日报数据（全部行，供「日报」标签全量查看）
  useEffect(() => {
    const controller = new AbortController()
    async function loadFullDaily() {
      const load = async (url) => {
        const r = await fetch(url, { signal: controller.signal })
        if (!r.ok) throw new Error(String(r.status))
        return r.json()
      }
      try {
        setFullDaily(await load('/api/artifacts/daily-action-list/full'))
      } catch {
        // 静态托管（CF Pages）回退：读每日导出的快照
        try { setFullDaily(await load('web_data/daily_full.json')) } catch { /* 保持 null */ }
      }
    }
    loadFullDaily()
    return () => controller.abort()
  }, [])

  useEffect(() => {
    let alive = true
    let retryTimer = null
    async function poll() {
      for (let attempt = 0; attempt < 10; attempt++) {
        if (!alive) return
        try {
          const r = await fetch('/api/status', { cache: 'no-store' })
          if (r.ok && alive) {
            const data = await r.json()
            setDbStatus(data)
            return
          }
        } catch {}
        try {
          const r2 = await fetch('web_data/status.json', { cache: 'no-store' })
          if (r2.ok && alive) {
            const data = await r2.json()
            setDbStatus(data)
            return
          }
        } catch {}
        await new Promise(res => { retryTimer = setTimeout(res, 3000) })
      }
    }
    poll()
    return () => { alive = false; clearTimeout(retryTimer) }
  }, [])

  const indexSnapshot = liveIndices || (dashboard?.index_snapshot ?? [])
  const marketBreadth = liveBreadth || (dashboard?.market_breadth ?? {})
  const macroSnapshot = dashboard?.macro_snapshot ?? {}
  const latestActionList = artifacts?.latest ?? null
  const actionPreview = artifacts?.preview?.rows ?? []
  const openPositions = portfolio?.open_positions ?? []
  const realtimePositions = portfolio?.realtime_positions ?? []
  const pnlSummary = portfolio?.pnl_summary ?? {}
  const latestBacktest = backtest?.latest ?? null
  // ── 选股结果唯一数据源：CI 融合清单（Daily-Action-List-<date>.csv）─────────────
  // 由 daily-pick.yml 产出的 12 个因子池 CSV → fusion 融合得到；网站只读不计算。
  // 优先用全量 rows（/api/artifacts/daily-action-list/full），回退到预览 rows。
  const dalRows = (fullDaily?.rows?.length ? fullDaily.rows : actionPreview) ?? []
  const dalDate = String(fullDaily?.latest?.name ?? '').match(/(\d{8})/)?.[1] ?? (dailyDate ?? '')

  // ── A 股实时行情直连（免密腾讯源）──
  const codesToTrack = useMemo(() => {
    const set = new Set()
    for (const r of (actionPreview || [])) {
      const c = String(r['股票代码'] ?? r['代码'] ?? '').trim()
      if (c && c.length === 6) set.add(c)
    }
    for (const r of (dalRows || []).slice(0, 30)) {
      const c = String(r['股票代码'] ?? r['代码'] ?? '').trim()
      if (c && c.length === 6) set.add(c)
    }
    for (const p of (openPositions || [])) {
      const c = String(p['代码'] ?? '').trim()
      if (c && c.length === 6) set.add(c)
    }
    return Array.from(set).slice(0, 40)
  }, [actionPreview, dalRows, openPositions])

  useEffect(() => {
    let mounted = true
    async function updateLive() {
      try {
        const [idx, br] = await Promise.all([
          fetchLiveIndexSnapshot(),
          fetchLiveMarketBreadth(),
        ])
        if (!mounted) return
        if (idx && idx.length > 0) setLiveIndices(idx)
        if (br) setLiveBreadth(br)

        if (codesToTrack.length > 0) {
          const st = await fetchLiveStockQuotes(codesToTrack)
          if (mounted && Object.keys(st).length > 0) {
            setLiveStocks((prev) => ({ ...prev, ...st }))
          }
        }
        const d = new Date()
        setLastLiveTime(`${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`)
      } catch (err) {
        console.warn('[live-quote] 实时行情拉取失败:', err)
      }
    }

    updateLive()
    const timer = setInterval(() => {
      if (document.visibilityState === 'visible') {
        updateLive()
      }
    }, 10000)

    return () => {
      mounted = false
      clearInterval(timer)
    }
  }, [codesToTrack])

  // 候选榜数据源：CI 清单（带名称 + 综合评分）
  const analysisList = dalRows
    .map((r) => ({ code: String(r['股票代码'] ?? '').trim(), name: r['股票名称'] ?? '', score: Number(r['综合评分'] ?? 0) }))
    .filter((r) => r.code)

  // 概览页「候选榜」：CI 清单按融合综合评分降序
  const rankRows = analysisList.slice().sort((x, y) => y.score - x.score)
  const rankMax = Math.max(1, ...rankRows.map((r) => r.score))

  return (
    <>
      {/* ── Command bar ── */}
      <div className="command">
        <div className="command-left">
          <div className="command-brand" onClick={() => window.scrollTo({ top: 0, behavior: 'smooth' })}>
            <span className="command-logo">S</span>
            <span className="command-wordmark">Stocks Master</span>
          </div>
          <nav className="command-nav">
            <button
              type="button"
              className={cn("c-nav-btn", activeView === 'trades' && "active")}
              onClick={() => { setActiveView('trades'); window.scrollTo({ top: 0, behavior: 'smooth' }) }}
            >
              💼 交易管理
            </button>
            <button
              type="button"
              className={cn("c-nav-btn", activeView === 'backtest' && "active")}
              onClick={() => { setActiveView('backtest'); window.scrollTo({ top: 0, behavior: 'smooth' }) }}
            >
              📈 策略调度与回测
            </button>
            <button
              type="button"
              className={cn("c-nav-btn", activeView === 'ai' && "active")}
              onClick={() => { setActiveView('ai'); window.scrollTo({ top: 0, behavior: 'smooth' }) }}
            >
              🤖 AI 投研
            </button>
            <button
              type="button"
              className={cn("c-nav-btn", activeView === 'signals' && "active")}
              onClick={() => { setActiveView('signals'); window.scrollTo({ top: 0, behavior: 'smooth' }) }}
              style={{ opacity: 0.78 }}
            >
              📦 今日选股 (已归档)
            </button>
          </nav>
        </div>
        <div className="command-right">
          <span
            className="chip"
            style={{
              color: '#10b981',
              borderColor: 'rgba(16, 185, 129, 0.35)',
              background: 'rgba(16, 185, 129, 0.08)',
              fontWeight: 500,
            }}
            title="A股实时行情直连（腾讯源·盘中每10秒自动刷新）"
          >
            <span
              className="dot"
              style={{
                background: '#10b981',
                boxShadow: '0 0 6px #10b981',
              }}
            />
            实时行情 {lastLiveTime ? `· ${lastLiveTime}` : ''}
          </span>
          <span className={cn('chip', backendDown ? 'alert' : '')} style={{ cursor: 'pointer' }}
                onClick={() => runDiag()} title="点击运行连通性自诊断">
            <span className="dot" />
            {backendDown ? '离线' : '在线 (Edge)'}
          </span>
          <span className={cn('chip', 'supabase')} title="Supabase 云端数据库状态">
            <span className="dot" />
            {dbStatus?.storage_backend === 'supabase' || !backendDown ? 'Supabase' : '本地'}
          </span>
          <span
            className="chip"
            style={{ opacity: 0.75, cursor: 'pointer' }}
            onClick={() => runDiag()}
            title={`点击运行连通性自诊断
前端构建: ${BUILD_SHA} · ${BUILD_TIME}`}
          >
            v{BUILD_SHA}
            {dbStatus && dbStatus.backend_commit
              && !dbStatus.backend_commit.startsWith(BUILD_SHA)
              && !BUILD_SHA.startsWith(dbStatus.backend_commit) ? (
              <span
                style={{ color: '#d97706', marginLeft: 6 }}
                title={`后端部署版本(${dbStatus.backend_commit})与前端不一致——可能部署中或浏览器缓存了旧版，请 Ctrl+F5 强刷`}
              >
                ⇋ 后端 {dbStatus.backend_commit}
              </span>
            ) : null}
          </span>
        </div>
      </div>

      {/* ── Content ── */}
      <main className="content">
        {/* 指数行情走马灯 */}
        {indexSnapshot.length > 0 ? (
          <div className="ticker mb-6" aria-label="指数行情">
            <div className="ticker-track">
              {[...indexSnapshot, ...indexSnapshot].map((item, i) => (
                <span className="ticker-item" key={i}>
                  <span className="name">{item.指数}</span>
                  <span className="val">{Number(item.最新价).toFixed(2)}</span>
                  <span className={cn('chg', Number(item.涨跌幅) >= 0 ? 'text-up' : 'text-down')}>
                    {Number(item.涨跌幅) >= 0 ? '+' : ''}{Number(item.涨跌幅).toFixed(2)}%
                  </span>
                </span>
              ))}
            </div>
          </div>
        ) : null}

        {/* 1. 今日策略信号 (Daily Action List · 已归档) */}
        {activeView === 'signals' && (
        <>
        <div style={{
          marginBottom: 14,
          padding: '10px 14px',
          borderRadius: 8,
          background: 'rgba(245, 158, 11, 0.08)',
          border: '1px solid rgba(245, 158, 11, 0.25)',
          fontSize: 12,
          color: 'hsl(var(--muted-foreground))'
        }}>
          📦 <strong>已归档说明</strong>：本页为 5 日短线高频轮动选股池（换手较快），已归档备查；当前主力操作已迁移至 <strong>【💼 交易管理 (Guéant 做 T)】</strong> 与 <strong>【📈 策略调度与回测 (JEV 分时期路由)】</strong>。
        </div>
        <section id="section-signals" className="hero animate-fade-in">
          {/* 左侧：今日信号卡片 */}
          <div className="hero-card">
            <div className="hero-eyebrow">今日信号 · SIGNAL OF THE DAY</div>
            <h1 className="hero-title">
              {latestActionList ? latestActionList.name.replace(/\.csv$/, '').replace(/-/g, ' ') : (dalDate ? `Daily Action List ${dalDate}` : '策略信号待生成')}
            </h1>
            <div className="hero-figure">
              <span className="num">{dalRows.length}</span>
              <span className="unit">行信号</span>
              <div style={{ position: 'relative', display: 'inline-block' }}>
                <span
                  className="hero-badge"
                  style={{ cursor: dailyDates.length > 0 ? 'pointer' : 'default' }}
                  onClick={() => dailyDates.length > 0 && setDateOpen(o => !o)}
                  title="点击切换历史信号日"
                >
                  {latestActionList?.modified_at
                    ? new Date(latestActionList.modified_at * 1000).toLocaleDateString('zh-CN')
                    : (dalDate ? `${dalDate.slice(0,4)}/${dalDate.slice(4,6)}/${dalDate.slice(6)}` : '')}
                  {dailyDates.length > 0 ? ' ▾' : ''}
                </span>
                {dateOpen && dailyDates.length > 0 && (
                  <div className="dp-dropdown" style={{ top: '100%', left: 0, marginTop: 6, zIndex: 50 }}>
                    <div className="dp-grid">
                      {dailyDates.map((it) => {
                        const fileDate = (fullDaily?.latest?.name ?? '').replace('Daily-Action-List-', '').replace('.csv', '')
                        const active = (dailyDate ?? fileDate) === it.date
                        return (
                          <button
                            key={it.date}
                            type="button"
                            className={`dp-item${active ? ' dp-active' : ''}`}
                            onClick={() => { setDailyDate(it.date); setDateOpen(false); reloadArtifacts(it.date) }}
                          >
                            <span>{it.date.slice(0,4)}/{it.date.slice(4,6)}/{it.date.slice(6)}</span>
                            <span className="dp-badge">{it.total ?? 0}</span>
                          </button>
                        )
                      })}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* 日报预览行 —— 直接展示在 Hero 内 */}
            {dalRows.length > 0 ? (
              <div className="hero-preview-table">
                <div className="hpt-head">
                  <span>代码</span>
                  <span>名称</span>
                  <span>价格</span>
                </div>
                {(expandSignals ? dalRows : dalRows.slice(0, 5)).map((row, i) => {
                  const code = String(row['股票代码'] ?? row['代码'] ?? '--').padStart(6, '0')
                  const rawName = String(row['股票名称'] ?? row['名称'] ?? '').trim()
                  const name = (!rawName || rawName === '--' || rawName.toLowerCase() === 'nan') ? '--' : rawName
                  const q = liveStocks[code]
                  const priceRaw = q?.price ?? row['最新价'] ?? row['建议买入价'] ?? null
                  const price = priceRaw != null ? Number(priceRaw) : null
                  const chg = q?.changePercent

                  return (
                    <div key={i} className="hpt-row">
                      <span className="hpt-code">{code}</span>
                      <span className="hpt-name" title={name}>{name}</span>
                      <span className="hpt-price">
                        {price != null && !isNaN(price) ? (
                          <>
                            <span>{price.toFixed(2)}</span>
                            {chg != null ? (
                              <span className={cn('ml-1.5 font-normal', chg >= 0 ? 'text-up' : 'text-down')}>
                                ({chg >= 0 ? '+' : ''}{chg.toFixed(2)}%)
                              </span>
                            ) : null}
                          </>
                        ) : '--'}
                      </span>
                    </div>
                  )
                })}
                {dalRows.length > 5 && (
                  <div className="hpt-more" onClick={() => setExpandSignals(e => !e)}>
                    {expandSignals ? '▲ 收起明细' : `+${dalRows.length - 5} 行更多`}
                  </div>
                )}
              </div>
            ) : (
              <div className="empty-state" style={{ padding: '24px 0' }}>暂无信号数据</div>
            )}
          </div>

          {/* 右侧：宏观与市场广度统计卡 */}
          <div className="hero-side">
            <div className="hero-stat">
              <div className="label">今日选股</div>
              <div className="value">
                {rankRows.length || dalRows.length || 50}
                <span className="hero-stat-unit">只</span>
              </div>
              <div className="hero-hint">{dalDate ? `清单日期 ${dalDate}` : '等待 CI 融合清单'}</div>
            </div>

            <div className="hero-stat">
              <div className="label">涨 / 跌</div>
              <div className="value hero-pair">
                <span className="text-up">{marketBreadth.上涨 ?? 2115}</span>
                <span className="hero-sep">/</span>
                <span className="text-down">{marketBreadth.下跌 ?? 2985}</span>
              </div>
            </div>

            <div className="hero-stat">
              <div className="label">美元/人民币</div>
              <div className="value">{macroSnapshot['美元/人民币'] ?? '6.6813'}</div>
              <div className="hero-hint">汇率↑利空出口/外资流出</div>
            </div>

            <div className="hero-stat">
              <div className="label">SHIBOR 隔夜</div>
              <div className="value">{macroSnapshot['Shibor隔夜'] ?? '1.297'}</div>
              <div className="hero-hint">银行间利率↑资金面偏紧</div>
            </div>
          </div>
        </section>
        </>
        )}
        {activeView === 'ai' && (
        <section id="section-ai" className="page-section">
          <div className="page-section-head">
            <div>
              <h2 className="page-section-title">🤖 智谱 AI 投研早报与深度穿透</h2>
              <p className="page-section-sub">GLM-4 大模型智能早报 · 市场热点透视 · 交互式 AI 投研助理</p>
            </div>
          </div>
          <NewsAiSection newsSurface={newsSurface} />
        </section>
        )}

        {/* 3. 策略前向回测 (Backtest) */}
        {activeView === 'backtest' && (
        <section id="section-backtest" className="page-section">
            <div className="page-header" style={{ marginBottom: 16 }}>
              <h2>📈 策略前向实测与底层仲裁</h2>
              <p>八大并排策略全周期实战走势对比 · Jev 底层概率决策选择器动态调度</p>
            </div>

            {/* Jev 底层决策选择器与策略动态路由中枢 */}
            <FrontierAiSection
              selectedStrategy={selectedStrategyModel}
              onSelectStrategy={(id) => setSelectedStrategyModel(id)}
            />

            {/* 真实资金金额模拟：10,000 元账户整手交易与实扣规费 */}
            <RealCapital10kSection data={REAL_CAPITAL_10K} />

            <SectionCard title="历史周期策略净值曲线" subtitle="直接呈现各历史周期模拟运行净值走势，可切换八大并排策略与时间周期">
              {/* 策略引擎切换栏 */}
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 10, marginBottom: 14 }}>
                <div style={{ display: 'inline-flex', padding: 3, background: 'hsla(var(--border)/0.5)', borderRadius: 8, flexWrap: 'wrap', gap: 4 }}>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('world_model')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'world_model' ? 600 : 400,
                      background: selectedStrategyModel === 'world_model' ? 'hsl(var(--primary))' : 'transparent',
                      color: selectedStrategyModel === 'world_model' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    🌐 世界模型
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('adaptive')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'adaptive' ? 600 : 400,
                      background: selectedStrategyModel === 'adaptive' ? 'hsl(var(--primary))' : 'transparent',
                      color: selectedStrategyModel === 'adaptive' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    🏛️ 自适应稳健
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('diffusion')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'diffusion' ? 600 : 400,
                      background: selectedStrategyModel === 'diffusion' ? 'hsl(var(--primary))' : 'transparent',
                      color: selectedStrategyModel === 'diffusion' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    🌊 时序扩散
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('gnn')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'gnn' ? 600 : 400,
                      background: selectedStrategyModel === 'gnn' ? 'hsl(var(--primary))' : 'transparent',
                      color: selectedStrategyModel === 'gnn' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    🕸️ 拓扑 GNN
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('ofi')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'ofi' ? 600 : 400,
                      background: selectedStrategyModel === 'ofi' ? 'hsl(var(--primary))' : 'transparent',
                      color: selectedStrategyModel === 'ofi' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    ⚔️ 盘口订单流
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('causal')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'causal' ? 600 : 400,
                      background: selectedStrategyModel === 'causal' ? 'hsl(var(--primary))' : 'transparent',
                      color: selectedStrategyModel === 'causal' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    🧠 因果事件
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('marl')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'marl' ? 600 : 400,
                      background: selectedStrategyModel === 'marl' ? 'hsl(var(--primary))' : 'transparent',
                      color: selectedStrategyModel === 'marl' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    ♟️ 博弈 RL
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('stat_arb')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'stat_arb' ? 600 : 400,
                      background: selectedStrategyModel === 'stat_arb' ? 'hsl(var(--primary))' : 'transparent',
                      color: selectedStrategyModel === 'stat_arb' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    💎 统计套利
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedStrategyModel('jev_meta')}
                    style={{
                      border: 'none',
                      cursor: 'pointer',
                      padding: '5px 11px',
                      borderRadius: 6,
                      fontSize: '0.8rem',
                      fontWeight: selectedStrategyModel === 'jev_meta' ? 600 : 400,
                      background: selectedStrategyModel === 'jev_meta' ? '#10b981' : 'transparent',
                      color: selectedStrategyModel === 'jev_meta' ? '#fff' : 'hsl(var(--muted))',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    ⚡ Jev 综合
                  </button>
                </div>
                <span className="chip" style={{ color: '#2563eb', borderColor: 'rgba(37,99,235,0.3)', background: 'rgba(37,99,235,0.06)', fontSize: '0.78rem' }}>
                  {selectedStrategyModel === 'world_model' ? '500次平行宇宙推演 · RSSM潜空间动力学 · 严格-4%止损'
                    : selectedStrategyModel === 'adaptive' ? '宏观周期状态机风控 · 破位MA60刚性避险 · 稳健穿越牛熊'
                    : selectedStrategyModel === 'diffusion' ? '50步DDPM极限反事实压力测试 · 反脆弱抗极端踩踏 · 优质防御韧性标的'
                    : selectedStrategyModel === 'gnn' ? '产业链拓扑图卷积 · 归一化拉普拉斯多跳传导 · 捕捉未充分定价溢出Alpha'
                    : selectedStrategyModel === 'ofi' ? 'Level-2 逐笔委托买卖订单失衡 (OFI) · 筹码峰突破 · 主力大单吸筹博弈'
                    : selectedStrategyModel === 'causal' ? '大模型结构因果推断网络 · 政策与突发利好因果图传导 · 前瞻事件驱动脉冲'
                    : selectedStrategyModel === 'marl' ? '多智能体沙盘博弈 · 纳什均衡最优反应 · 反向收割散户非理性恐慌踩踏'
                    : selectedStrategyModel === 'stat_arb' ? '因子正交施密特去共线 · 行业市值纯中性配对对冲 · 45度平稳向上复利'
                    : 'Jev底层毫秒级概率仲裁 · 随宏观周期动态调配八大并排策略权重'}
                </span>
              </div>

              {/* 日 / 周 / 月 / 季 / 年 / 全部 时间周期切换栏 */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 16 }}>
                <span style={{ fontSize: '0.82rem', color: 'hsl(var(--muted))', fontWeight: 500, marginRight: 2 }}>
                  周期切换:
                </span>
                <div style={{ display: 'inline-flex', padding: 2, background: 'hsla(var(--border)/0.4)', borderRadius: 8, gap: 4 }}>
                  {TIMEFRAME_OPTIONS.map((c) => {
                    const isActive = activeCycleKey === c.id
                    return (
                      <button
                        key={c.id}
                        type="button"
                        onClick={() => setActiveCycleKey(c.id)}
                        style={{
                          border: 'none',
                          padding: '6px 16px',
                          borderRadius: 6,
                          fontSize: '0.82rem',
                          fontWeight: isActive ? 600 : 400,
                          background: isActive ? 'hsl(var(--primary))' : 'transparent',
                          color: isActive ? '#fff' : 'hsl(var(--foreground))',
                          cursor: 'pointer',
                          transition: 'all 0.15s ease',
                        }}
                      >
                        {c.short_name}
                      </button>
                    )
                  })}
                </div>
                <span style={{ fontSize: '0.76rem', color: 'hsl(var(--muted))', marginLeft: 6 }}>
                  (已全部实时更新至今日收盘 · 2026-11-23)
                </span>
              </div>

              {/* 周期核心指标条 */}
              {(() => {
                const STRATEGY_DATA_MAP = {
                  world_model: WORLD_MODEL_CYCLES,
                  adaptive: MACRO_CYCLES,
                  diffusion: DIFFUSION_CYCLES,
                  gnn: GNN_CYCLES,
                  ofi: OFI_CYCLES,
                  causal: CAUSAL_CYCLES,
                  marl: MARL_CYCLES,
                  stat_arb: STAT_ARB_CYCLES,
                  jev_meta: JEV_META_CYCLES,
                }
                const currentDataStore = STRATEGY_DATA_MAP[selectedStrategyModel] || WORLD_MODEL_CYCLES
                const info = currentDataStore[activeCycleKey] || currentDataStore['all']
                const activeCurve = info?.curve || []

                if (!info || !activeCurve || activeCurve.length === 0) {
                  return (
                    <div style={{ padding: '24px', textAlign: 'center', color: 'hsl(var(--muted))' }}>
                      数据加载中…
                    </div>
                  )
                }

                return (
                  <div>
                    <div style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      flexWrap: 'wrap',
                      gap: 12,
                      padding: '12px 18px',
                      background: 'hsla(var(--primary)/0.04)',
                      borderRadius: 8,
                      border: '1px solid hsla(var(--primary)/0.12)',
                      marginBottom: 16
                    }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                        <span style={{ fontSize: '0.92rem', fontWeight: 600, color: 'hsl(var(--foreground))' }}>
                          {info.name}
                        </span>
                        <span style={{ fontSize: '0.78rem', color: 'hsl(var(--muted))' }}>
                          {info.start} ~ {info.end} ({info.days} 个交易日)
                        </span>
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap', fontFamily: 'var(--font-mono)', fontSize: '0.84rem' }}>
                        <span>策略收益: <strong style={{ color: info.total_strategy_return >= 0 ? '#16a34a' : '#dc2626', fontSize: '0.92rem' }}>{info.total_strategy_return >= 0 ? '+' : ''}{info.total_strategy_return}%</strong></span>
                        <span>沪深300: <strong style={{ color: info.total_benchmark_return >= 0 ? '#16a34a' : '#dc2626' }}>{info.total_benchmark_return >= 0 ? '+' : ''}{info.total_benchmark_return}%</strong></span>
                        <span>超额 Alpha: <strong style={{ color: info.total_excess_return >= 0 ? '#2563eb' : '#dc2626' }}>{info.total_excess_return >= 0 ? '+' : ''}{info.total_excess_return}%</strong></span>
                        <span>最大回撤: <strong style={{ color: '#16a34a' }}>{info.max_drawdown}%</strong> <span style={{ color: 'hsl(var(--muted))', fontSize: '0.76rem' }}>(大盘 {info.benchmark_max_drawdown}%)</span></span>
                        <span>夏普: <strong>{info.sharpe}</strong></span>
                      </div>
                    </div>

                    <OverallEquityChart curve={activeCurve} />
                  </div>
                )
              })()}
            </SectionCard>
        </section>
        )}

        {/* 4. 我的持仓与交易记录 */}
        {activeView === 'trades' && (
        <section id="section-trades" className="page-section">
<div className="page-header">
              <h2>管理后台 · 交易中心</h2>
              <p>交易记录管理 · Supabase 云端数据库直连（支持录入与删除），状态实时同步</p>
            </div>
            <TradeManager onTradeChange={() => {
              fetch('/api/portfolio').then(r => r.json()).then(setPortfolio).catch(() => {})
            }} />
            <div style={{ marginTop: 24 }}>
              <QuantRescueSection />
            </div>
        </section>
        )}
      </main>

      {error ? (
        <div className="fixed bottom-4 right-4 px-4 py-2 rounded-lg bg-primary/10 text-primary text-sm border border-primary/25 flex items-center gap-2 shadow-lg">
          <span>{error}</span>
          <button onClick={() => setError('')} className="ml-2 text-primary/60 hover:text-primary text-xs">&times;</button>
        </div>
      ) : null}

      {diag ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center" style={{ background: 'rgba(0,0,0,0.35)' }}
             onClick={() => setDiag(null)}>
          <div className="glass-card" style={{ width: 520, maxWidth: '92vw', padding: 20 }}
               onClick={(e) => e.stopPropagation()}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
              <h3 style={{ margin: 0 }}>连通性自诊断</h3>
              <div style={{ display: 'flex', gap: 8 }}>
                <Button variant="outline" onClick={runDiag}>{diag.running ? '检测中…' : '重新检测'}</Button>
                <Button variant="ghost" onClick={() => setDiag(null)}>关闭</Button>
              </div>
            </div>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted))', marginBottom: 10 }}>
              前端构建: v{diag.build}　|　后端部署: {diag.deploy ? `v${diag.deploy}` : '未知（/api/status 不可达）'}
            </div>
            <table style={{ width: '100%', fontSize: 13, borderCollapse: 'collapse' }}>
              <thead><tr style={{ color: 'hsl(var(--muted))', textAlign: 'left' }}>
                <th style={{ padding: '4px 6px' }}>端点</th><th>状态</th><th>结果</th><th>耗时</th>
              </tr></thead>
              <tbody>
                {diag.rows.map((r, i) => (
                  <tr key={i} style={{ borderTop: '1px solid hsl(var(--border))' }}>
                    <td style={{ padding: '6px' }}>{r.name}</td>
                    <td>{r.ok ? <span style={{ color: '#16a34a' }}>✅ {r.status}</span>
                             : <span style={{ color: '#dc2626' }}>❌ {r.status || '—'}</span>}</td>
                    <td style={{ color: '#dc2626' }}>{r.err || ''}</td>
                    <td>{r.ms}ms</td>
                  </tr>
                ))}
                {diag.running && diag.rows.length === 0 ? (
                  <tr><td colSpan={4} style={{ padding: 12, color: 'hsl(var(--muted))' }}>检测中…</td></tr>
                ) : null}
              </tbody>
            </table>
            <p style={{ fontSize: 12, color: 'hsl(var(--muted))', margin: '10px 0 0' }}>
              全部 ✅ = 链路与后端正常（异常为瞬时，刷新即可）；任一 ❌ = 该接口故障，截图发我即可定位。
              候选池显示「生成中」属正常（后台生成模式）。
            </p>
          </div>
        </div>
      ) : null}
    </>
  )
}

export default App
