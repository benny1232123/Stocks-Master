import { useEffect, useRef, useState } from 'react'
import {
  LayoutDashboard,
  Filter,
  LineChart,
  Briefcase,
  FlaskConical,
  ArrowUpRight,
  ArrowDownRight,
  FileText,
  TrendingUp,
  TrendingDown,
  Target,
  Percent,
  BarChart3,
  Trophy,
  AlertTriangle,
  Zap,
} from 'lucide-react'
import { Button } from './components/ui/button'
import { cn } from './lib/utils'

const TABS = [
  { id: 'overview', label: '概览', icon: LayoutDashboard },
  { id: 'selection', label: '选股', icon: Filter },
  { id: 'analysis', label: '分析', icon: LineChart },
  { id: 'daily', label: '日报', icon: FileText },
  { id: 'portfolio', label: '持仓', icon: Briefcase },
  { id: 'backtest', label: '回测', icon: FlaskConical },
]

function StatCard({ label, value, trend, className: cnExtra }) {
  return (
    <div className={cn("stat-card", cnExtra)}>
      <span className="label">{label}</span>
      <span className="value">{value}</span>
      {trend != null ? (
        <span className={cn('text-xs font-medium', trend >= 0 ? 'text-up' : 'text-down')}>
          {trend >= 0 ? <ArrowUpRight className="inline w-3 h-3" /> : <ArrowDownRight className="inline w-3 h-3" />}
          {Math.abs(trend)}%
        </span>
      ) : null}
    </div>
  )
}

function Field({ label, children, hint }) {
  return (
    <label className="field-card">
      <span className="field-label">{label}</span>
      {children}
      {hint ? <span className="field-hint">{hint}</span> : null}
    </label>
  )
}

function DailyExpandableList({ rows, onCodeClick }) {
  const [expanded, setExpanded] = useState(new Set())
  const num = (r, k) => { const v = r[k]; if (v == null || v === '') return null; const n = Number(v); return (isNaN(n) || !isFinite(n)) ? null : n }

  // 策略颜色映射
  const STRAT_COLORS = {
    'Boll': { bg: 'hsla(229, 87%, 56%, 0.10)', text: '#6366F1', border: 'hsla(229, 87%, 56%, 0.35)' },
    'Relativity': { bg: 'hsla(157, 81%, 37%, 0.09)', text: '#30A46C', border: 'hsla(157, 81%, 37%, 0.35)' },
    'Theme': { bg: 'hsla(38, 92%, 50%, 0.10)', text: '#F59E0B', border: 'hsla(38, 92%, 50%, 0.35)' },
    'CCTV': { bg: 'hsla(3, 80%, 50%, 0.08)', text: '#E5484D', border: 'hsla(3, 80%, 50%, 0.35)' },
  }
  const getStratColor = (s) => STRAT_COLORS[s] || { bg: 'hsl(var(--surface-2))', text: 'hsl(var(--muted))', border: 'hsl(var(--border))' }

  // 评分等级色
  const scoreGrade = (s) => {
    if (s == null || isNaN(s)) return { label: '--', cls: '', bar: 0 }
    if (s >= 45) return { label: 'A+', cls: 'ds-a-plus', bar: 100 }
    if (s >= 38) return { label: 'A', cls: 'ds-a', bar: 85 }
    if (s >= 30) return { label: 'B', cls: 'ds-b', bar: 65 }
    if (s >= 20) return { label: 'C', cls: 'ds-c', bar: 40 }
    return { label: 'D', cls: 'ds-d', bar: 20 }
  }

  // 多维评分构成：用每行已有字段（买入价/止损/止盈/MA20/策略）推导 5 个可解释子维度 + 加权综合分。
  // 相比后端单一「综合评分」数字，这里把评级拆开，让用户看到每个维度如何贡献，避免「黑箱一个数」。
  const scoreBreakdown = (row) => {
    const clamp = (v, lo = 0, hi = 100) => Math.max(lo, Math.min(hi, v))
    const buy = num(row, '建议买入价')
    const latest = num(row, '最新价')
    const lower = num(row, '止损价(下轨)')
    const tp = num(row, '止盈价(上轨)')
    const ma20 = num(row, 'MA20')
    const stratStr = String(row['来源策略'] ?? '')
    const hit = Number(row['命中策略数'] ?? stratStr.split('/').filter(Boolean).length) || 1
    const hasRS = stratStr.includes('Relativity')

    // 1) 超卖深度：价格越贴近/跌破下轨，反弹赔率越好
    let oversold = 50
    if (!isNaN(buy) && !isNaN(lower) && lower > 0) {
      const dist = (buy - lower) / lower * 100
      if (dist <= 0) oversold = 100
      else if (dist <= 5) oversold = 100 - dist * 4        // 100 → 80
      else if (dist <= 15) oversold = 80 - (dist - 5) * 4  // 80 → 40
      else oversold = Math.max(20, 40 - (dist - 15) * 1.5)
    }
    // 2) 盈亏比：止盈空间 / 止损空间
    let rr = 50
    if (!isNaN(buy) && !isNaN(lower) && !isNaN(tp) && lower > 0 && tp > lower) {
      const denom = buy - lower
      if (denom > 0) {
        const ratio = (tp - buy) / denom
        rr = ratio >= 3 ? 100 : ratio >= 2 ? 80 : ratio >= 1.5 ? 65 : ratio >= 1 ? 45 : 25
      }
    }
    // 3) 趋势强度：现价相对 MA20（站上均线上方更稳）
    let trend = 50
    if (!isNaN(latest) && !isNaN(ma20) && ma20 > 0) {
      const p = (latest - ma20) / ma20 * 100
      trend = p >= 0 ? clamp(70 + p * 2, 70, 92) : clamp(50 + p * 2, 30, 70)
    }
    // 4) 策略共振：命中策略越多越可信
    const reso = hit >= 3 ? 96 : hit === 2 ? 85 : 50
    // 5) 相对强弱：跑赢指数（Relativity）额外加分
    const rs = hasRS ? 88 : 60

    const composite = Math.round(
      oversold * 0.30 + rr * 0.25 + trend * 0.20 + reso * 0.15 + rs * 0.10
    )
    const rows = [
      { key: '超卖深度', val: Math.round(oversold), hint: '贴近下轨' },
      { key: '盈亏比', val: Math.round(rr), hint: '止盈/止损' },
      { key: '趋势强度', val: Math.round(trend), hint: '价 vs MA20' },
      { key: '策略共振', val: Math.round(reso), hint: `${hit} 策略命中` },
      { key: '相对强弱', val: Math.round(rs), hint: hasRS ? '跑赢指数' : '仅价格信号' },
    ]
    return [rows, composite]
  }

  const toggle = (i) => setExpanded((prev) => {
    const next = new Set(prev)
    if (next.has(i)) next.delete(i)
    else next.add(i)
    return next
  })
  return (
    <div className="daily-list">
      {rows.map((row, i) => {
        const code = String(row['股票代码'] ?? row['代码'] ?? '--').padStart(6, '0')
        const name = (row['股票名称'] ?? '--').trim()
        const displayName = (!name || name === '--' || name.toLowerCase() === 'nan') ? '--' : name
        const score = num(row, '综合评分')
        const strategies = row['来源策略'] ?? '--'
        const stratList = strategies.split('/').map(s => s.trim()).filter(Boolean)
        const primaryStrat = stratList[0] || '--'
        const buyPrice = num(row, '建议买入价')
        const latestP = num(row, '最新价')
        const stopP = num(row, '止损价(下轨)')
        const tpP = num(row, '止盈价(上轨)')
        const ma20V = num(row, 'MA20')
        const posPct = num(row, '建议仓位%')
        const amt = num(row, '建议金额')
        const hitCount = row['命中策略数'] ?? stratList.length

        const sg = scoreGrade(score)
        const sc = getStratColor(primaryStrat)
        const [sbRows, sbComposite] = scoreBreakdown(row)
        const sbCls = sbComposite >= 80 ? 'sb-excellent' : sbComposite >= 60 ? 'sb-good' : sbComposite >= 40 ? 'sb-mid' : 'sb-weak'
        const pnlPct = (!isNaN(latestP) && !isNaN(buyPrice) && buyPrice > 0) ? ((latestP / buyPrice - 1) * 100) : NaN
        const isOpen = expanded.has(i)

        // 排名奖牌
        const rankBadge = (idx) => {
          if (idx === 0) return <span className="rank-medal rank-gold">🥇</span>
          if (idx === 1) return <span className="rank-medal rank-silver">🥈</span>
          if (idx === 2) return <span className="rank-medal rank-bronze">🥉</span>
          return <span className="rank-num">{idx + 1}</span>
        }

        return (
          <div key={i} className={cn('daily-item', isOpen && 'open')} style={{ borderLeft: `3px solid ${sc.border}` }}>
            <div className="daily-summary" onClick={() => toggle(i)}>
              <div className="daily-summary-left">
                <span className="daily-expander">{isOpen ? '▼' : '▶'}</span>
                {rankBadge(i)}
                <span className="daily-code" onClick={(e) => { e.stopPropagation(); onCodeClick(code) }}>{code}</span>
                <span className="daily-name">{displayName}</span>
                {/* 策略彩色标签 */}
                <div className="strat-badges">
                  {stratList.map((s, si) => {
                    const c = getStratColor(s)
                    return <span key={si} className="strat-badge" style={{ background: c.bg, color: c.text, borderColor: c.border }}>{s}</span>
                  })}
                </div>
              </div>

              <div className="daily-summary-right">
                {/* 评级药丸 */}
                <div className={cn('score-pill', sg.cls)} title={`综合评分: ${!isNaN(score) ? score.toFixed(1) : '--'}`}>
                  <span className="score-label">{sg.label}</span>
                  {!isNaN(score) && <span className="score-bar-track"><span className="score-bar-fill" style={{ width: `${sg.bar}%` }} /></span>}
                </div>

                {/* 买入价 + 盈亏 */}
                <div className="price-group">
                  <span className="daily-buy">¥{!isNaN(buyPrice) ? buyPrice.toFixed(2) : '--'}</span>
                  {!isNaN(pnlPct) && (
                    <span className={cn('pnl-mini', pnlPct >= 0 ? 'pnl-up' : 'pnl-down')}>
                      {pnlPct >= 0 ? '+' : ''}{pnlPct.toFixed(2)}%
                    </span>
                  )}
                </div>

                {/* 仓位 & 金额 */}
                <div className="pos-group">
                  {!isNaN(posPct) && <span className="pos-tag">{posPct.toFixed(0)}%</span>}
                  {!isNaN(amt) && <span className="amt-tag">{amt >= 10000 ? `${(amt/10000).toFixed(1)}万` : `${amt.toFixed(0)}`}</span>}
                </div>

                {/* 止损距离 */}
                {!isNaN(latestP) && !isNaN(stopP) && stopP > 0 && (
                  <span className={cn('stop-dist', ((latestP - stopP) / stopP * 100) < 5 ? 'stop-close' : '')}>
                    距止损 {((latestP - stopP) / stopP * 100).toFixed(1)}%
                  </span>
                )}
              </div>
            </div>
            {isOpen ? (
              <div className="daily-details">
                <div className="dd-cell"><span>命中策略数</span><strong>{hitCount}</strong></div>
                <div className="dd-cell"><span>建议仓位%</span><strong>{!isNaN(posPct) ? `${posPct.toFixed(0)}%` : '--'}</strong></div>
                <div className="dd-cell"><span>建议金额</span><strong>{!isNaN(amt) ? `¥${amt.toFixed(0)}` : '--'}</strong></div>
                <div className="dd-cell"><span>最新价</span><strong className={!isNaN(latestP) && !isNaN(buyPrice) && buyPrice > 0 ? (latestP >= buyPrice ? 'text-up' : 'text-down') : ''}>{latestP != null && !isNaN(latestP) ? latestP.toFixed(2) : '--'}</strong></div>
                <div className="dd-cell"><span>建议买入价</span><strong>{!isNaN(buyPrice) ? buyPrice.toFixed(2) : '--'}</strong></div>
                <div className="dd-cell"><span>止损价(下轨)</span><strong className={stopP != null && !isNaN(stopP) ? 'text-down' : ''}>{stopP != null && !isNaN(stopP) ? stopP.toFixed(2) : '--'}</strong></div>
                <div className="dd-cell"><span>止盈价(上轨)</span><strong className={tpP != null && !isNaN(tpP) ? 'text-up' : ''}>{tpP != null && !isNaN(tpP) ? tpP.toFixed(2) : '--'}</strong></div>
                <div className="dd-cell"><span>MA20</span><strong>{ma20V != null && !isNaN(ma20V) ? ma20V.toFixed(2) : '--'}</strong></div>
                {/* 智能解读行 */}
                <div className="daily-insight">
                  {!isNaN(latestP) && !isNaN(buyPrice) && buyPrice > 0 ? (
                    <span className={cn('di-tag', latestP >= buyPrice ? 'di-profit' : 'di-loss')}>
                      相对买入价 {latestP >= buyPrice ? `+${((latestP/buyPrice-1)*100).toFixed(2)}% 盈` : `${((latestP/buyPrice-1)*100).toFixed(2)}% 亏`}
                    </span>
                  ) : null}
                  {!isNaN(latestP) && !isNaN(stopP) && stopP > 0 ? (
                    <span className={cn('di-tag', (latestP - stopP) / stopP * 100 < 3 ? 'di-danger' : '')}>
                      距止损 {((latestP - stopP) / stopP * 100).toFixed(2)}%
                    </span>
                  ) : null}
                  {!isNaN(latestP) && !isNaN(tpP) && tpP > 0 ? (
                    <span className="di-tag">距止盈 +${((tpP - latestP) / latestP * 100).toFixed(2)}%</span>
                  ) : null}
                  {!isNaN(stopP) && !isNaN(tpP) && stopP > 0 && tpP > 0 ? (
                    <span className="di-tag">盈亏比 {(tpP / stopP - 1).toFixed(2)}:1</span>
                  ) : null}
                  {/* 综合风险评级 */}
                  {(() => {
                    const toStop = (!isNaN(latestP) && !isNaN(stopP) && stopP > 0) ? (latestP - stopP) / stopP * 100 : null
                    if (toStop != null && toStop < 2) return <span className="di-risk di-danger">🔴 极高风险 — 接近止损位</span>
                    if (toStop != null && toStop < 5) return <span className="di-risk di-warn">⚠️ 高风险 — 止损较近</span>
                    if (toStop != null && toStop < 10) return <span className="di-risk di-caution">🟡 中等风险</span>
                    if (toStop != null) return <span className="di-risk di-safe">🟢 低风险 — 止损空间充足</span>
                    return null
                  })()}
                </div>
                {/* 多维评分构成 */}
                <div className="score-breakdown">
                  <div className="sb-head">
                    <span className="sb-title">评分构成（多维推导）</span>
                    <span className={cn('sb-composite', sbCls)}>加权综合 {sbComposite}</span>
                  </div>
                  {sbRows.map((d) => {
                    const vCls = d.val >= 80 ? 'sb-excellent' : d.val >= 60 ? 'sb-good' : d.val >= 40 ? 'sb-mid' : 'sb-weak'
                    return (
                      <div className="sb-row" key={d.key}>
                        <span className="sb-label">{d.key}</span>
                        <span className="sb-track"><span className={cn('sb-fill', vCls)} style={{ width: `${d.val}%` }} /></span>
                        <span className="sb-val">{d.val}</span>
                        <span className="sb-hint">{d.hint}</span>
                      </div>
                    )
                  })}
                </div>
              </div>
            ) : null}
          </div>
        )
      })}
    </div>
  )
}

function SectionCard({ title, subtitle, children, className = '' }) {
  return (
    <section className={cn('glass-card animate-fade-in', className)}>
      <div className="section-head">
        <h3>{title}</h3>
        {subtitle ? <span>{subtitle}</span> : null}
      </div>
      {children}
    </section>
  )
}

function EquityChart({ equity, initialCapital }) {
  if (!equity || equity.length < 2) return null

  const W = 760
  const H = 300
  const PAD = { top: 20, right: 20, bottom: 40, left: 70 }
  const cw = W - PAD.left - PAD.right
  const ch = H - PAD.top - PAD.bottom

  const values = equity.map((d) => d.total)
  // 完全平（全部等于初始资金）= 真实 0 成交：显示说明而非一条水平线，避免用户误以为系统故障
  const allFlat = values.every((v) => Math.abs(v - initialCapital) < 1e-6)
  if (allFlat) {
    return (
      <div className="bt-empty">
        <div className="bt-empty-icon">📉</div>
        <div className="bt-empty-title">该信号日无成交标的</div>
        <div className="bt-empty-desc">前向 K 线拉取失败或窗口过短，导致 0 笔交易、权益曲线无波动——属正常现象，并非系统错误。</div>
      </div>
    )
  }
  const minY = Math.min(...values, initialCapital) * 0.98
  const maxY = Math.max(...values, initialCapital) * 1.02

  const xScale = (i) => PAD.left + (i / (equity.length - 1)) * cw
  const yScale = (v) => PAD.top + ch - ((v - minY) / (maxY - minY)) * ch

  const linePath = equity.map((d, i) => `${i === 0 ? 'M' : 'L'}${xScale(i).toFixed(1)},${yScale(d.total).toFixed(1)}`).join(' ')
  const baselineY = yScale(initialCapital)

  const ticks = 5
  const yTicks = Array.from({ length: ticks + 1 }, (_, i) => {
    const v = minY + ((maxY - minY) / ticks) * i
    return { y: yScale(v), label: (v / 10000).toFixed(1) + '万' }
  })

  const xTicks = equity.filter((_, i) => i % Math.max(1, Math.floor(equity.length / 6)) === 0 || i === equity.length - 1)

  const lastVal = values[values.length - 1]
  const curveColor = lastVal >= initialCapital ? '#E5484D' : '#30A46C'
  const ddValues = equity.map((d) => d.drawdown ?? 0)
  const ddMin = Math.min(...ddValues)
  const ddH = 50
  const ddYScale = (v) => (Math.abs(v) / Math.abs(ddMin || 1)) * ddH

  return (
    <div className="equity-chart-wrap">
      <svg viewBox={`0 0 ${W} ${H}`} className="equity-svg">
        {yTicks.map((t, i) => (
          <g key={i}>
            <line x1={PAD.left} y1={t.y} x2={W - PAD.right} y2={t.y} stroke="rgba(0,0,0,0.06)" />
            <text x={PAD.left - 8} y={t.y + 4} textAnchor="end" fill="rgba(0,0,0,0.4)" fontSize="10">
              {t.label}
            </text>
          </g>
        ))}

        <line x1={PAD.left} y1={baselineY} x2={W - PAD.right} y2={baselineY} stroke="rgba(0,0,0,0.25)" strokeDasharray="4 3" opacity="0.4" />
        <text x={W - PAD.right + 4} y={baselineY + 4} fill="rgba(0,0,0,0.35)" opacity="0.5" fontSize="9">
          初始
        </text>

        <path d={linePath} fill="none" stroke={curveColor} strokeWidth="2" strokeLinejoin="round" />
        <circle cx={xScale(values.length - 1)} cy={yScale(lastVal)} r="4" fill={curveColor} />

        {xTicks.map((d, i) => {
          const idx = equity.indexOf(d)
          return (
            <text key={i} x={xScale(idx)} y={H - 8} textAnchor="middle" fill="rgba(0,0,0,0.4)" fontSize="9">
              {d.date.slice(5)}
            </text>
          )
        })}
      </svg>

      {ddMin < 0 ? (
        <div className="dd-bar-wrap">
          <span className="dd-label">回撤</span>
          <svg viewBox={`0 0 ${W} ${ddH + 10}`} className="dd-svg">
            {equity.map((d, i) => {
              const v = d.drawdown ?? 0
              if (v === 0) return null
              return <rect key={i} x={xScale(i) - 2} y={0} width="4" height={ddYScale(v)} fill="#E5484D" opacity="0.35" rx="1" />
            })}
          </svg>
        </div>
      ) : null}
    </div>
  )
}

const STRAT_LABEL = {
  boll: 'Boll 低吸',
  relativity: 'Relativity 相对强弱',
  theme: 'Theme 题材动量',
  cctv: 'CCTV 舆情',
}

function App() {
  const [activeView, setActiveView] = useState('overview')
  const [dashboard, setDashboard] = useState(null)
  const [artifacts, setArtifacts] = useState(null)
  const [portfolio, setPortfolio] = useState(null)
  const [backtest, setBacktest] = useState(null)
  const [analysis, setAnalysis] = useState(null)
  const [analysisCode, setAnalysisCode] = useState('000001')
  const [candidateCodes, setCandidateCodes] = useState([])
  const [selectionParams, setSelectionParams] = useState({ priceMin: 5, priceMax: 30 })
  const [selectionScan, setSelectionScan] = useState(null)
  const [fusionResult, setFusionResult] = useState(null)
  const [backtestRun, setBacktestRun] = useState(null)
  const [dailyBacktests, setDailyBacktests] = useState([])
  const [dailySummary, setDailySummary] = useState(null)
  const [selDaily, setSelDaily] = useState(0)
  const [btDateOpen, setBtDateOpen] = useState(false)    // 回测日期选择器是否展开
  const [tradeForm, setTradeForm] = useState({
    date: new Date().toISOString().slice(0, 10),
    code: '', name: '', side: 'buy', price: 0, qty: 100, fee: 0, notes: '',
  })
  const [error, setError] = useState('')
  const [scanLogs, setScanLogs] = useState([])
  const [dbStatus, setDbStatus] = useState(null)
  const [fullDaily, setFullDaily] = useState(null)
  const [dailyDate, setDailyDate] = useState(null)        // 当前查看的日报日期(YYYYMMDD)，null=最新
  const [dailyDates, setDailyDates] = useState([])         // 可选日报日期列表
  const [dateOpen, setDateOpen] = useState(false)          // 日期下拉框是否展开
  const logContainerRef = useRef(null)

  // Phase: null | 'candidates' | 'boll' | 'backtest' | 'fusion'
  const [scanPhase, setScanPhase] = useState(null)
  const [bollTaskId, setBollTaskId] = useState(null)
  const [fusionTaskId, setFusionTaskId] = useState(null)
  const [btTaskId, setBtTaskId] = useState(null)
  const [scanProgress, setScanProgress] = useState({ current: 0, total: 0 })

  // 手动多策略回测表单
  const _today = new Date().toISOString().slice(0, 10)
  const _yearAgo = new Date(Date.now() - 365 * 86400000).toISOString().slice(0, 10)
  const [multiCodes, setMultiCodes] = useState('600519,000858')
  const [multiStart, setMultiStart] = useState(_yearAgo)
  const [multiEnd, setMultiEnd] = useState(_today)
  const [multiStrats, setMultiStrats] = useState({ boll: true, relativity: true, theme: true, cctv: false })

  useEffect(() => { loadDailyBacktest() }, [])
  useEffect(() => { loadDailyDates(); reloadArtifacts() }, [])
  // 日期选择器：点击外部关闭
  useEffect(() => {
    if (!dateOpen) return
    const handler = (e) => {
      if (!e.target.closest('.date-picker-wrap')) setDateOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [dateOpen])

  const isRunning = scanPhase !== null

  async function cancelTask(taskId) {
    if (!taskId) return
    try {
      const r = await fetch(`/api/selection/cancel-task/${taskId}`, { method: 'POST' })
      if (!r.ok) setError('取消任务失败')
    } catch { setError('取消请求失败') }
  }

  async function startBacktest(codes) {
    if (!codes || codes.length === 0) return
    setScanPhase('backtest')
    setScanLogs((prev) => [...prev, `融合完成，开始自动回测 ${codes.length} 只股票...`])
    try {
      const resp = await fetch('/api/backtests/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codes, hold_days: 5, initial_capital: 100000, max_positions: 10 }),
      })
      if (!resp.ok) { setScanPhase(null); setError('回测请求失败'); return }
      const { task_id } = await resp.json()
      setBtTaskId(task_id)
    } catch { setScanPhase(null); setError('回测启动失败') }
  }

  // 加载每日 CI 自动回测结果（全部历史信号日前向回测批次）
  async function loadDailyBacktest() {
    try {
      const resp = await fetch('/api/backtests/daily-latest')
      if (resp.ok) {
        const data = await resp.json()
        setDailyBacktests(data.items || [])
        setSelDaily(0)
      }
    } catch { /* 忽略加载失败 */ }
    try {
      const sresp = await fetch('/api/backtests/daily-summary')
      if (sresp.ok) {
        const sdata = await sresp.json()
        setDailySummary(sdata)
      }
    } catch { /* 忽略加载失败 */ }
  }

  async function startFusion() {
    setScanPhase('fusion')
    setScanLogs((prev) => [...prev, 'Boll 扫描完成，开始策略融合...'])
    try {
      const today = new Date().toISOString().slice(0, 10).replace(/-/g, '')
      const resp = await fetch('/api/selection/fusion', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ date: today, total_capital: 100000, max_picks: 15 }),
      })
      if (!resp.ok) { setScanPhase(null); setError('融合启动失败'); return }
      setFusionTaskId((await resp.json()).task_id)
    } catch { setScanPhase(null); setError('融合启动失败') }
  }

  async function reloadArtifacts(date) {
    try {
      const [a, f] = await Promise.all([
        fetch('/api/artifacts/daily-action-list'),
        fetch(`/api/artifacts/daily-action-list/full${date ? `?date=${date}` : ''}`),
      ])
      if (a.ok) setArtifacts(await a.json())
      if (f.ok) {
        const d = await f.json()
        setFullDaily(d)
        if (date && d.latest?.name) setDailyDate(date)
      }
    } catch { /* 忽略刷新失败 */ }
  }

  // 加载全部历史日报日期，供日报页「日期选择器」切换
  async function loadDailyDates() {
    try {
      const resp = await fetch('/api/artifacts/daily-action-list/dates')
      if (resp.ok) {
        const d = await resp.json()
        setDailyDates(d.items || [])
      }
    } catch { /* 忽略 */ }
  }

  // 手动多策略 Backtrader 回测
  async function runMultiBacktest() {
    const codes = multiCodes.split(/[\n,，\s]+/).map((s) => s.trim()).filter(Boolean)
    if (!codes.length) { setError('请输入股票代码'); return }
    const strategies = Object.entries(multiStrats).filter(([, v]) => v).map(([k]) => k).join(',')
    if (!strategies) { setError('请至少选择一个策略'); return }
    setBacktestRun(null)
    setScanLogs([])
    setError('')
    try {
      const resp = await fetch('/api/backtests/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mode: 'multi',
          codes,
          start: multiStart,
          end: multiEnd,
          strategies,
          initial_capital: 100000,
        }),
      })
      if (!resp.ok) { setError('回测请求失败'); return }
      const { task_id } = await resp.json()
      setBtTaskId(task_id)
    } catch { setError('回测启动失败') }
  }

  const [analysisLoading, setAnalysisLoading] = useState(false)
  const [analysisError, setAnalysisError] = useState(null)

  // 从列表/榜单点选股票 → 拉取分析并切换详情
  async function openAnalysis(code) {
    if (!code) return
    setAnalysisCode(code)
    setAnalysisLoading(true)
    setAnalysisError(null)
    try {
      const r = await fetch(`/api/analysis/${code}`)
      if (r.ok) {
        setAnalysis(await r.json())
      } else {
        setAnalysisError(`服务器返回 ${r.status}`)
      }
    } catch (err) {
      setAnalysisError('无法连接后端 API（离线模式或服务未启动）')
    } finally {
      setAnalysisLoading(false)
    }
  }

  // Poll boll-scan task
  useEffect(() => {
    if (!bollTaskId) return
    let done = false
    const controller = new AbortController()
    const timer = setInterval(async () => {
      if (done) return
      try {
        const resp = await fetch(`/api/selection/task-logs/${bollTaskId}`, { signal: controller.signal })
        if (!resp.ok) return
        const data = await resp.json()
        setScanLogs(data.logs || [])
        const last = (data.logs || []).slice(-1)[0] || ''
        const m = last.match(/\[(\d+)\/(\d+)\]/)
        if (m) setScanProgress({ current: Number(m[1]), total: Number(m[2]) })
        if (data.status === 'done' || data.status === 'error' || data.status === 'cancelled') {
          if (!done) {
            done = true
            if (data.status === 'done' && data.result?.rows) {
              setSelectionScan(data.result)
              startFusion()
            } else {
              setScanPhase(null)
            }
            clearInterval(timer)
          }
        }
      } catch (e) {
        if (!done && e.name !== 'AbortError') {
          done = true
          setScanPhase(null)
          setError('策略扫描轮询失败')
          clearInterval(timer)
        }
      }
    }, 500)
    return () => { done = true; controller.abort(); clearInterval(timer) }
  }, [bollTaskId])

  // Poll fusion task
  useEffect(() => {
    if (!fusionTaskId) return
    let done = false
    const controller = new AbortController()
    const timer = setInterval(async () => {
      if (done) return
      try {
        const resp = await fetch(`/api/selection/task-logs/${fusionTaskId}`, { signal: controller.signal })
        if (!resp.ok) return
        const data = await resp.json()
        setScanLogs(data.logs || [])
        if (data.status === 'done' || data.status === 'error' || data.status === 'cancelled') {
          if (!done) {
            done = true
            if (data.status === 'done' && data.result) {
              setFusionResult(data.result)
              reloadArtifacts()
              const codes = data.result.rows?.map((r) => r['股票代码']).filter(Boolean) ?? []
              if (codes.length > 0) startBacktest(codes)
              else setScanPhase(null)
            } else {
              setScanPhase(null)
            }
            clearInterval(timer)
          }
        }
      } catch (e) {
        if (!done && e.name !== 'AbortError') {
          done = true
          setScanPhase(null)
          setError('融合轮询失败')
          clearInterval(timer)
        }
      }
    }, 500)
    return () => { done = true; controller.abort(); clearInterval(timer) }
  }, [fusionTaskId])

  // Poll backtest task
  useEffect(() => {
    if (!btTaskId) return
    let done = false
    const controller = new AbortController()
    const timer = setInterval(async () => {
      if (done) return
      try {
        const resp = await fetch(`/api/selection/task-logs/${btTaskId}`, { signal: controller.signal })
        if (!resp.ok) return
        const data = await resp.json()
        if (data.logs) {
          setScanLogs((prev) => {
            const newLogs = data.logs.slice(prev.length)
            return newLogs.length > 0 ? [...prev, ...newLogs] : prev
          })
        }
        if (data.status === 'done' || data.status === 'error' || data.status === 'cancelled') {
          if (!done) {
            done = true
            if (data.result) { setBacktestRun(data.result); setScanLogs((prev) => [...prev, '回测完成，权益曲线已更新']) }
            setScanPhase(null)
            clearInterval(timer)
          }
        }
      } catch (e) {
        if (!done && e.name !== 'AbortError') {
          done = true
          setScanPhase(null)
          setError('回测轮询失败')
          clearInterval(timer)
        }
      }
    }, 500)
    return () => { done = true; controller.abort(); clearInterval(timer) }
  }, [btTaskId])

  useEffect(() => {
    if (logContainerRef.current) logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight
  }, [scanLogs])

  useEffect(() => {
    if (!error) return
    const timer = setTimeout(() => setError(''), 5000)
    return () => clearTimeout(timer)
  }, [error])

  useEffect(() => {
    const controller = new AbortController()
    async function loadDashboard() {
      try {
        setError('')
        const [d, a, p, b] = await Promise.all([
          fetch('/api/dashboard', { signal: controller.signal }),
          fetch('/api/artifacts/daily-action-list', { signal: controller.signal }),
          fetch('/api/portfolio', { signal: controller.signal }),
          fetch('/api/backtests/latest', { signal: controller.signal }),
        ])
        if (!d.ok || !a.ok || !p.ok || !b.ok) throw new Error('api error')
        setDashboard(await d.json())
        setArtifacts(await a.json())
        setPortfolio(await p.json())
        setBacktest(await b.json())
        const c = await fetch('/api/selection/candidates?price_min=5&price_max=30', { signal: controller.signal })
        if (c.ok) setCandidateCodes((await c.json()).codes ?? [])
        const an = await fetch(`/api/analysis/${analysisCode}`, { signal: controller.signal })
        if (an.ok) setAnalysis(await an.json())
      } catch (err) { if (err.name !== 'AbortError') setError('后端未启动或接口不可用') }
    }
    loadDashboard()
    return () => controller.abort()
  }, [])

  // 加载完整日报数据（全部行，供「日报」标签全量查看）
  useEffect(() => {
    const controller = new AbortController()
    async function loadFullDaily() {
      try {
        const resp = await fetch('/api/artifacts/daily-action-list/full', { signal: controller.signal })
        if (resp.ok) setFullDaily(await resp.json())
      } catch { /* 离线时保持 null */ }
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
        await new Promise(res => { retryTimer = setTimeout(res, 3000) })
      }
    }
    poll()
    return () => { alive = false; clearTimeout(retryTimer) }
  }, [])

  const indexSnapshot = dashboard?.index_snapshot ?? []
  const marketBreadth = dashboard?.market_breadth ?? {}
  const macroSnapshot = dashboard?.macro_snapshot ?? {}
  const latestActionList = artifacts?.latest ?? null
  const actionPreview = artifacts?.preview?.rows ?? []
  const openPositions = portfolio?.open_positions ?? []
  const realtimePositions = portfolio?.realtime_positions ?? []
  const pnlSummary = portfolio?.pnl_summary ?? {}
  const latestBacktest = backtest?.latest ?? null
  const backtestSummary = backtestRun?.summary ?? null
  const backtestEquity = backtestRun?.equity ?? []
  const backtestTrades = backtestRun?.trades ?? []
  const analysisSignal = analysis?.signal ?? null
  const analysisLatest = analysis?.latest ?? null
  const selectionRows = selectionScan?.rows ?? []
  const fusionRows = fusionResult?.rows ?? []

  // 分析页主从列表数据源：优先融合结果（带名称+评分），否则用候选池代码
  const analysisList = fusionRows.length
    ? fusionRows.map((r) => ({ code: r['股票代码'], name: r['股票名称'] ?? '', score: Number(r['综合评分'] ?? 0) }))
    : candidateCodes.map((code) => ({ code, name: code, score: null }))

  // 概览页「候选榜」：按融合综合评分排名，点选跳分析页
  const rankRows = fusionRows
    .map((r) => ({ code: r['股票代码'], name: r['股票名称'] ?? '', score: Number(r['综合评分'] ?? 0) }))
    .filter((r) => r.code)
  const rankMax = Math.max(1, ...rankRows.map((r) => r.score))

  // 信号徽章配色（红涨绿跌语义）
  const sigRaw = analysisSignal?.signal
  const sigClass = !sigRaw ? 'neutral'
    : /买|多|看多|bull/i.test(String(sigRaw)) ? 'up'
    : /卖|空|看空|bear/i.test(String(sigRaw)) ? 'down'
    : 'neutral'

  return (
    <>
      {/* ── Icon rail ── */}
      <aside className="rail">
        <div className="rail-mark">S</div>
        <nav className="rail-nav">
          {TABS.map((tab) => {
            const Icon = tab.icon
            return (
              <button
                key={tab.id}
                className={cn('rail-item', activeView === tab.id && 'active')}
                onClick={() => setActiveView(tab.id)}
                aria-label={tab.label}
              >
                <Icon />
                <span className="rail-tip">{tab.label}</span>
              </button>
            )
          })}
        </nav>
      </aside>

      {/* ── Command bar ── */}
      <div className="command">
        <span className="command-wordmark">Stocks Master</span>
        <div className="command-right">
          <span className={cn('chip', error ? 'alert' : '')}>
            <span className="dot" />
            {error ? '离线' : '在线'}
          </span>
          <span className={cn('chip', 'supabase')}>
            <span className="dot" />
            {dbStatus == null ? 'DB…' : dbStatus.storage_backend === 'supabase' ? 'Supabase' : '本地'}
          </span>
        </div>
      </div>

      {/* ── Content ── */}
      <main className="content">
        {activeView === 'overview' ? (
          <>
            {/* 签名元素：指数行情带 */}
            {indexSnapshot.length > 0 ? (
              <div className="ticker" aria-label="指数行情">
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

            {/* Hero：信号板（紧凑双栏，含日报预览行） */}
            <section className="hero animate-fade-in">
              <div className="hero-card">
                <div className="hero-eyebrow">今日信号 · Signal of the day</div>
                <h1 className="hero-title">
                  {latestActionList ? latestActionList.name.replace(/\.csv$/, '').replace(/-/g, ' ') : '策略信号待生成'}
                </h1>
                <div className="hero-figure">
                  <span className="num">{actionPreview.length || 0}</span>
                  <span className="unit">行信号</span>
                  {latestActionList ? (
                    <span className="hero-badge">{new Date(latestActionList.modified ?? Date.now()).toLocaleDateString('zh-CN')}</span>
                  ) : null}
                </div>
                {/* 日报预览行 —— 直接展示在 Hero 内 */}
                {actionPreview.length > 0 ? (
                  <div className="hero-preview-table">
                    <div className="hpt-head">
                      <span>代码</span><span>名称</span><span>价格</span>
                    </div>
                    {actionPreview.slice(0, 5).map((row, i) => {
                      const code = row['股票代码'] ?? row['代码'] ?? '--'
                      const name = row['股票名称'] ?? row['名称'] ?? '--'
                      const priceRaw = row['最新价'] ?? row['建议买入价'] ?? null
                      const price = priceRaw != null ? Number(priceRaw) : null
                      return (
                        <div key={i} className="hpt-row">
                          <span className="hpt-code">{code}</span>
                          <span className="hpt-name">{name}</span>
                          <span className="hpt-price">{price != null && !isNaN(price) ? price.toFixed(2) : '--'}</span>
                        </div>
                      )
                    })}
                    {actionPreview.length > 5 ? (
                      <div className="hpt-more" onClick={() => setActiveView('daily')}>+{actionPreview.length - 5} 行更多</div>
                    ) : null}
                  </div>
                ) : null}
              </div>
              <div className="hero-side">
                <div className="hero-stat">
                  <div className="label">候选池</div>
                  <div className="value">{candidateCodes.length}<span className="hero-stat-unit">只</span></div>
                </div>
                <div className="hero-stat">
                  <div className="label">涨 / 跌</div>
                  <div className="value hero-pair">
                    <span className="text-up">{marketBreadth.上涨 ?? '--'}</span>
                    <span className="hero-sep">/</span>
                    <span className="text-down">{marketBreadth.下跌 ?? '--'}</span>
                  </div>
                </div>
                <div className="hero-stat">
                  <div className="label">美元/人民币</div>
                  <div className="value">{macroSnapshot['美元/人民币'] ?? '--'}</div>
                  <div className="hero-hint">汇率↑利空出口/外资流出</div>
                </div>
                <div className="hero-stat">
                  <div className="label">SHIBOR 隔夜</div>
                  <div className="value">{macroSnapshot['Shibor隔夜'] ?? '--'}</div>
                  <div className="hero-hint">银行间利率↑资金面偏紧</div>
                </div>
              </div>
            </section>

            {/* 指数快照 + 日报文件（紧凑双栏） */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <SectionCard title="指数快照" subtitle="来自 /api/dashboard">
                <div className="index-list">
                  {indexSnapshot.length > 0 ? indexSnapshot.map((item) => (
                    <div key={item.指数} className="index-row">
                      <span>{item.指数}</span>
                      <strong>{Number(item.最新价).toFixed(2)}</strong>
                      <em className={cn(Number(item.涨跌幅) >= 0 ? 'text-up' : 'text-down')}>
                        {Number(item.涨跌幅) >= 0 ? '+' : ''}{Number(item.涨跌幅).toFixed(2)}%
                      </em>
                    </div>
                  )) : (
                    <div className="empty-state">暂无指数数据</div>
                  )}
                </div>
              </SectionCard>

              <SectionCard title="最新日报" subtitle={latestActionList?.path ?? ''} className="relative">
                <button
                  onClick={() => setActiveView('daily')}
                  style={{ position: 'absolute', top: 14, right: 16, fontSize: '0.76rem', color: 'hsl(var(--primary))', background: 'transparent', border: 'none', cursor: 'pointer', fontFamily: 'var(--font-mono)' }}
                >查看完整 →</button>
                {latestActionList && actionPreview.length > 0 ? (
                  <>
                    <DailyExpandableList
                      rows={actionPreview.slice(0, 5)}
                      onCodeClick={(code) => { const c = String(code).trim(); if (c) { setAnalysisCode(c); setActiveView('analysis'); openAnalysis(c) } }}
                    />
                    {actionPreview.length > 5 ? (
                      <div className="dp-footer" style={{ marginTop: 10 }}>共 {actionPreview.length} 行 · 完整文件: {latestActionList.name} · <button onClick={() => setActiveView('daily')} style={{ color: 'hsl(var(--primary))', background: 'transparent', border: 'none', cursor: 'pointer', fontFamily: 'var(--font-mono)', padding: 0 }}>查看完整 →</button></div>
                    ) : (
                      <div className="dp-footer" style={{ marginTop: 10 }}>完整文件: {latestActionList.name}</div>
                    )}
                  </>
                ) : latestActionList ? (
                  <div className="empty-state">日报文件存在但预览为空</div>
                ) : (
                  <div className="empty-state">暂无日报文件</div>
                )}
              </SectionCard>
            </div>

            {/* 榜单：候选榜 · 融合排序（抄 chengzuopeng 榜单页） */}
            <SectionCard title="候选榜 · 融合排序" subtitle="点选跳转分析页" className="mt-4">
              {rankRows.length > 0 ? (
                <div className="rank-list">
                  {rankRows.map((r, i) => (
                    <div key={r.code} className="rank-row" onClick={() => openAnalysis(r.code)}>
                      <span className="rank-no">{i + 1}</span>
                      <span className="rank-code">{r.code}</span>
                      <span className="rank-name">{r.name}</span>
                      <span className="rank-bar">
                        <span className="rank-bar-fill" style={{ width: `${Math.max(4, (r.score / rankMax) * 100)}%` }} />
                      </span>
                      <span className="rank-score">{Number(r.score).toFixed(1)}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="empty-state">运行「选股」中的融合排序后，这里会按综合评分生成榜单</div>
              )}
            </SectionCard>
          </>
        ) : null}

        {activeView === 'selection' ? (
          <>
            <div className="page-header">
              <h2>策略融合选股</h2>
              <p>设置价格区间，一键运行多策略筛选 → 融合排名 → 自动回测。</p>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <SectionCard title="选股参数" className="lg:col-span-2">
                <div className="selection-form">
                  <Field label="最低价" hint="过滤太低价标的">
                    <input value={selectionParams.priceMin} type="number" min="1" step="1"
                      onChange={(e) => setSelectionParams((p) => ({ ...p, priceMin: Number(e.target.value) }))} />
                  </Field>
                  <Field label="最高价" hint="控制候选池价格上限">
                    <input value={selectionParams.priceMax} type="number" min="1" step="1"
                      onChange={(e) => setSelectionParams((p) => ({ ...p, priceMax: Number(e.target.value) }))} />
                  </Field>

                  <div className="button-row">
                    <Button disabled={isRunning} onClick={async () => {
                      setScanPhase('candidates'); setScanLogs([]); setSelectionScan(null); setFusionResult(null); setBacktestRun(null); setCandidateCodes([]); setScanProgress({ current: 0, total: 0 })
                      // Step 1: 获取候选池（仅用于展示数量与价格区间参考）
                      const c = await fetch(`/api/selection/candidates?price_min=${selectionParams.priceMin}&price_max=${selectionParams.priceMax}`)
                      if (!c.ok) { setScanPhase(null); setError('获取候选池失败'); return }
                      const codes = (await c.json()).codes ?? []
                      setCandidateCodes(codes)
                      setScanLogs([`候选池 ${codes.length} 只，开始运行完整布林多因子扫描...`])
                      // Step 2: 运行完整布林多因子选股（内部自带价格/基本面/技术过滤）
                      setScanPhase('boll')
                      const s = await fetch('/api/selection/boll-scan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) })
                      if (!s.ok) { setScanPhase(null); setError('扫描启动失败'); return }
                      setBollTaskId((await s.json()).task_id)
                      // 布林完成后自动跑融合排序，融合完成后自动跑回测
                    }}>
                      {isRunning ? (
                        scanPhase === 'candidates' ? '获取候选...' :
                        scanPhase === 'boll' ? '多策略扫描中...' :
                        scanPhase === 'fusion' ? '融合排名中...' :
                        scanPhase === 'backtest' ? '回测运行中...' :
                        '运行中...'
                      ) : '开始选股'}
                    </Button>
                    {isRunning ? (
                      <Button variant="destructive" onClick={() => {
                        if (scanPhase === 'boll') cancelTask(bollTaskId)
                        else if (scanPhase === 'fusion') cancelTask(fusionTaskId)
                        else if (scanPhase === 'backtest') cancelTask(btTaskId)
                      }}>停止</Button>
                    ) : null}
                  </div>
                </div>

                {scanPhase && scanProgress.total > 0 ? (
                  <div className="progress-bar-wrap">
                    <div className="progress-bar-track">
                      <div className="progress-bar-fill" style={{ width: `${(scanProgress.current / scanProgress.total) * 100}%` }} />
                    </div>
                    <span className="progress-bar-text">{scanProgress.current} / {scanProgress.total}</span>
                  </div>
                ) : null}

                <div className="log-panel">
                  <div className="section-head log-head">
                    <h3>运行日志</h3>
                    <span className={cn(
                      'log-status',
                      scanPhase === 'boll' && 'running',
                      scanPhase === 'fusion' && 'running',
                      scanPhase === 'backtest' && 'running',
                    )}>
                      {scanPhase === 'boll' ? '多策略扫描中' :
                       scanPhase === 'fusion' ? '策略融合中' :
                       scanPhase === 'backtest' ? '回测中' :
                       scanPhase === 'candidates' ? '获取候选...' :
                       '等待执行'}
                    </span>
                  </div>
                  <div className="log-body" ref={logContainerRef}>
                    {scanLogs.length > 0 ? scanLogs.map((line, idx) => (
                      <div key={idx} className="log-line">
                        <span className="log-idx">{idx + 1}</span>
                        <span className="log-text">{line}</span>
                      </div>
                    )) : <div className="empty-state">点击按钮后实时显示运行进度</div>}
                  </div>
                </div>

                {selectionRows.length > 0 ? (
                  <div className="table-shell">
                    <div className="section-head"><h3>策略扫描结果</h3><span>命中 {selectionRows.length} 只</span></div>
                    {selectionRows.slice(0, 8).map((row) => (
                      <div key={row.代码} className="table-row">
                        <span>{row.代码}</span>
                        <strong>{Number(row.最新价 ?? 0).toFixed(2)}</strong>
                        <em>{row.信号}</em>
                      </div>
                    ))}
                  </div>
                ) : null}

                {fusionRows.length > 0 ? (
                  <div className="table-shell spaced">
                    <div className="section-head"><h3>策略融合结果</h3><span>{fusionResult?.saved_path ?? '未保存'}</span></div>
                    {fusionRows.slice(0, 8).map((row) => (
                      <div key={row.股票代码} className="table-row">
                        <span>{row.股票代码}</span>
                        <strong>{row.股票名称}</strong>
                        <em>{row.综合评分}</em>
                      </div>
                    ))}
                  </div>
                ) : null}
              </SectionCard>

              <SectionCard title="候选池">
                <div className="candidate-pool">
                  <div className="candidate-count">
                    <span className="candidate-count-num">{candidateCodes.length}</span>
                    <span className="candidate-count-label">只股票</span>
                  </div>
                  {candidateCodes.length > 0 ? (
                    <div className="candidate-tags">
                      {candidateCodes.slice(0, 20).map((code) => (
                        <span key={code} className="candidate-tag">{code}</span>
                      ))}
                      {candidateCodes.length > 20 && (
                        <span className="candidate-tag candidate-tag-more">+{candidateCodes.length - 20}</span>
                      )}
                    </div>
                  ) : (
                    <div className="empty-state">暂无候选</div>
                  )}
                  <div className="candidate-footer">展示前 20 个</div>
                </div>
              </SectionCard>
            </div>
          </>
        ) : null}

        {activeView === 'analysis' ? (
          <>
            <div className="page-header">
              <h2>个股分析</h2>
              <p>选择或输入股票代码，查看多指标技术分析与信号解读</p>
            </div>
            <div className="analysis-single">
              <SectionCard title="个股分析" className="max-w-none">
                <div className="analysis-top" style={{ marginBottom: 16 }}>
                  <div className="analysis-form">
                    <input value={analysisCode} onChange={(e) => setAnalysisCode(e.target.value)}
                      placeholder="输入股票代码，例如 000001"
                      className="flex-1 h-10 rounded-lg bg-card border border-border px-3 text-foreground placeholder:text-muted-foreground"
                      onKeyDown={(e) => e.key === 'Enter' && openAnalysis(analysisCode)} />
                    <Button onClick={() => openAnalysis(analysisCode)} disabled={analysisLoading}>
                      {analysisLoading ? '请求中...' : '加载分析'}
                    </Button>
                  </div>
                </div>

                {analysisError ? (
                  <div style={{ padding: '14px 16px', background: 'hsl(var(--destructive) / 0.08)', borderRadius: '8px', color: 'hsl(var(--destructive))', fontSize: '0.85rem', marginBottom: 12 }}>
                    ⚠️ {analysisError}
                  </div>
                ) : null}
                {analysis ? (
                  <>
                    <div className="detail-head">
                      <div className="detail-title">
                        <span className="detail-code">{analysisCode}</span>
                        <span className="detail-name">{analysis?.latest?.name ?? analysisList.find((i) => i.code === analysisCode)?.name ?? ''}</span>
                      </div>
                      <span className={cn('signal-badge', sigClass)} style={{ whiteSpace: 'nowrap' }}>{analysisSignal?.signal ?? '暂无信号'}</span>
                    </div>

                    {/* ── 多指标解读面板 ── */}
                    {(() => {
                        const L = analysisLatest ?? {}
                        const M = analysis?.metrics ?? {}
                        const rsi = L.rsi != null ? Number(L.rsi) : null
                        const dif = L.dif != null ? Number(L.dif) : null
                        const dea = L.dea != null ? Number(L.dea) : null
                        const macdH = L.macd_hist != null ? Number(L.macd_hist) : null
                        const kV = L.k_val != null ? Number(L.k_val) : null
                        const dV = L.d_val != null ? Number(L.d_val) : null
                        const jV = L.j_val != null ? Number(L.j_val) : null
                        const close = L.close != null ? Number(L.close) : null
                        const lower = L.lower != null ? Number(L.lower) : null
                        const upper = L.upper != null ? Number(L.upper) : null
                        const middle = L.middle != null ? Number(L.middle) : null
                        const ma5 = L.ma5 != null ? Number(L.ma5) : null
                        const ma10 = L.ma10 != null ? Number(L.ma10) : null
                        const ma20 = L.ma20 != null ? Number(L.ma20) : null
                        const ma60 = L.ma60 != null ? Number(L.ma60) : null
                        const distLo = M.dist_to_lower_pct != null ? Number(M.dist_to_lower_pct) : null
                        const distHi = M.dist_to_upper_pct != null ? Number(M.dist_to_upper_pct) : null
                        const bw = M.bandwidth != null ? Number(M.bandwidth) : null

                        // RSI 解读 —— 带具体数值区间和操作建议
                        const rsiTxt = rsi == null ? '--'
                          : rsi > 80 ? `严重超买（${rsi.toFixed(1)}），短期回调概率极高，建议减仓或观望`
                          : rsi > 70 ? `超买区（${rsi.toFixed(1)}），动能偏强但接近高位，追高需谨慎`
                          : rsi < 20 ? `严重超卖（${rsi.toFixed(1)}），超卖极值区域，可关注反弹机会`
                          : rsi < 30 ? `超卖区（${rsi.toFixed(1)}），卖压释放充分，逢低布局时机`
                          : rsi > 55 ? `偏强（${rsi.toFixed(1)}），多头略占优，持股待涨`
                          : rsi < 45 ? `偏弱（${rsi.toFixed(1)}），空头主导，不宜急于入场`
                          : `中性震荡（${rsi.toFixed(1)}），等待方向突破`
                        // MACD 解读 —— 结合柱值大小、零轴位置、给出具体建议
                        const macdAbs = macdH != null ? Math.abs(macdH) : 0
                        const macdStrength = macdAbs > 0.15 ? '强' : macdAbs > 0.05 ? '中' : macdAbs > 0.01 ? '弱' : '极弱'
                        const macdTxt = (dif == null || dea == null) ? '--'
                          : dif > dea && macdH > 0 ? `金叉+红柱（柱值${macdH.toFixed(3)}，${macdStrength}）↑ DIF>${dea.toFixed(3)}，多头加速中，可持仓`
                          : dif > dea && macdH <= 0 ? `多头但柱转绿（柱值${macdH.toFixed(3)}）⚠ DIF>DEA 但动能减弱，警惕拐头`
                          : dif < dea && macdH < 0 ? `死叉+绿柱（柱值${macdH.toFixed(3)}，${macdStrength}）↓ DIF<${dea.toFixed(3)}，空头主导，宜观望`
                          : dif < dea && macdH >= 0 ? `空头但柱转红（柱值${macdH.toFixed(3)}）⚠ DIF<DEA 但绿柱收窄，可能有反抽`
                          : `DIF≈DEA（差值仅${Math.abs(dif-dea).toFixed(4)}），方向选择中`
                        // KDJ 解读 —— 结合J值极端程度、KD间距、给出级别
                        const kdjGap = kV != null && dV != null ? Math.abs(kV - dV).toFixed(1) : '--'
                        const kdjTxt = (kV == null || dV == null) ? '--'
                          : jV != null && jV > 110 ? `🔴 极端超买 J=${jV.toFixed(1)}>100，KD差${kdjGap}，强烈建议回避`
                          : jV != null && jV > 100 ? `⚠️ 超买警戒 J=${jV.toFixed(1)}，短线获利盘重，注意止盈`
                          : jV != null && jV < -10 ? `🟢 极端超卖 J=${jV.toFixed(1)}<0，KD差${kdjGap}，超跌反弹窗口`
                          : jV != null && jV < 0 ? `⚠️ 超卖区 J=${jV.toFixed(1)}，卖压过度释放，可轻仓试探`
                          : kV > dV + 5 ? `金叉偏强 K>D（差+${kdjGap}），J=${jV?.toFixed(1) ?? '--'}，短线偏多`
                          : kV > dV ? `金叉形态 K>D（差+${kdjGap}），J=${jV?.toFixed(1) ?? '--'}，温和偏多`
                          : kV + 5 < dV ? `死叉偏弱 K<D（差-${kdjGap}），J=${jV?.toFixed(1) ?? '--'}，短线承压`
                          : kV < dV ? `死叉形态 K<D（差-${kdjGap}），J=${jV?.toFixed(1) ?? '--'}，偏空`
                          : `K≈D 缠绕（K${kV.toFixed(1)}/D${dV.toFixed(1)}），方向不明`
                        // 均线解读 —— 加入间距百分比、趋势强度、具体价位
                        const ma5_10_gap = (ma5 != null && ma10 != null) ? ((ma5 / ma10 - 1) * 100).toFixed(2) : '--'
                        const ma5_20_gap = (ma5 != null && ma20 != null) ? ((ma5 / ma20 - 1) * 100).toFixed(2) : '--'
                        const maTxt = (ma5 == null || ma10 == null || ma20 == null) ? '--'
                          : (() => {
                            if (ma5 > ma10 && ma10 > ma20 && ma20 > (ma60 ?? 0)) {
                              const extra = (ma60 != null && ma20 > ma60) ? '>MA60' : ''
                              return '完美多头排列 ↑ MA5>MA10(+' + ma5_10_gap + '%)>MA20(+' + ma5_20_gap + '%' + extra + ')，趋势强劲'
                            }
                            if (ma5 < ma10 && ma10 < ma20 && (ma20 < (ma60 ?? 999))) {
                              const extra = (ma60 != null && ma20 < ma60) ? '<MA60' : ''
                              return '空头排列 ↓ MA5<MA10(' + ma5_10_gap + '%)<MA20(' + ma5_20_gap + '%' + extra + ')，全线压制'
                            }
                            if (ma5 > ma20) {
                              const sub = (ma5 > ma10) ? ',MA5>MA10' : '但MA5<MA10'
                              return '短期偏强 ↑ MA5在MA20上方(+' + ma5_20_gap + '%)' + sub + '，短线有支撑'
                            }
                            const sub2 = (ma5 < ma10) ? ',且MA5<MA10' : ''
                            return '短期偏弱 ↓ MA5在MA20下方(' + ma5_20_gap + '%)' + sub2 + '，上方均线形成压力'
                          })()
                        // 价格位置解读 —— 加入盈亏比/风险收益评估
                        const posTxt = (distLo == null && lower == null) ? '--'
                          : close != null && lower != null && close < lower
                            ? `⚠ 已跌破下轨（${(close/lower*100-100).toFixed(2)}%），超卖信号，但需警惕趋势性破位`
                          : distLo != null && distLo < 2
                            ? `🔴 极近下轨（仅距${distLo.toFixed(2)}%），止损风险极高，若未持仓可关注反弹`
                          : distLo != null && distLo < 5
                            ? `接近下轨支撑（距${distLo.toFixed(2)}%），布林下轨${lower?.toFixed(2) ?? '--'}附近有承接`
                          : distHi != null && distHi > -2
                            ? `🟢 极近上轨（距上轨仅${Math.abs(distHi).toFixed(2)}%），注意止盈，上轨${upper?.toFixed(2) ?? '--'}`
                          : distHi != null && distHi > -5
                            ? `接近上轨压力（距${Math.abs(distHi).toFixed(2)}%），上轨${upper?.toFixed(2) ?? '--'}可能受阻`
                          : close != null && upper != null && close > upper
                            ? `已突破上轨（+${((close/upper-1)*100).toFixed(2)}%），强势突破但谨防假突破回踩`
                          : `位于布林带中部区间，距下轨${distLo!=null?`+${distLo.toFixed(2)}%`:'--'} / 距上轨${distHi!=null?`${distHi.toFixed(2)}%`:'--'}${bw != null ? ` · 带宽${bw.toFixed(1)}%${bw < 8 ? ' ⚠收窄→变盘在即' : bw > 25 ? ' 📐扩张→波动加大' : ''}` : ''}`
                        // 带宽颜色
                        const bwWarn = bw != null && bw < 8

                        // ── 总体总结：综合 5 大指标多空力度 ──
                        const scoreDetail = []
                        let s = 0
                        if (rsi != null) {
                          if (rsi > 80) { s -= 2; scoreDetail.push(['RSI', '严重超买', 'bear']) }
                          else if (rsi > 70) { s -= 1; scoreDetail.push(['RSI', '偏强高位', 'bear']) }
                          else if (rsi < 20) { s += 2; scoreDetail.push(['RSI', '严重超卖', 'bull']) }
                          else if (rsi < 30) { s += 1; scoreDetail.push(['RSI', '超卖区', 'bull']) }
                          else if (rsi > 55) { s += 1; scoreDetail.push(['RSI', '偏强', 'bull']) }
                          else if (rsi < 45) { s -= 1; scoreDetail.push(['RSI', '偏弱', 'bear']) }
                          else scoreDetail.push(['RSI', '中性', 'neutral'])
                        }
                        if (dif != null && dea != null) {
                          if (dif > dea && macdH > 0) { s += 2; scoreDetail.push(['MACD', '金叉红柱', 'bull']) }
                          else if (dif > dea && macdH <= 0) { scoreDetail.push(['MACD', '多头动能弱', 'neutral']) }
                          else if (dif < dea && macdH < 0) { s -= 2; scoreDetail.push(['MACD', '死叉绿柱', 'bear']) }
                          else if (dif < dea && macdH >= 0) { scoreDetail.push(['MACD', '空头柱收窄', 'neutral']) }
                          else scoreDetail.push(['MACD', '缠绕', 'neutral'])
                        }
                        if (kV != null && dV != null) {
                          if (jV != null && jV > 100) { s -= 2; scoreDetail.push(['KDJ', '极端超买', 'bear']) }
                          else if (jV != null && jV < 0) { s += 2; scoreDetail.push(['KDJ', '极端超卖', 'bull']) }
                          else if (kV > dV) { s += 1; scoreDetail.push(['KDJ', '金叉', 'bull']) }
                          else if (kV < dV) { s -= 1; scoreDetail.push(['KDJ', '死叉', 'bear']) }
                          else scoreDetail.push(['KDJ', '中性', 'neutral'])
                        }
                        if (ma5 != null && ma10 != null && ma20 != null) {
                          if (ma5 > ma10 && ma10 > ma20) { s += 2; scoreDetail.push(['均线', '多头排列', 'bull']) }
                          else if (ma5 < ma10 && ma10 < ma20) { s -= 2; scoreDetail.push(['均线', '空头排列', 'bear']) }
                          else if (ma5 > ma20) { s += 1; scoreDetail.push(['均线', '短期偏强', 'bull']) }
                          else if (ma5 < ma20) { s -= 1; scoreDetail.push(['均线', '短期偏弱', 'bear']) }
                          else scoreDetail.push(['均线', '中性', 'neutral'])
                        }
                        if (close != null && lower != null) {
                          if (close < lower) { s += 1; scoreDetail.push(['布林', '跌破下轨', 'bull']) }
                          else if (distLo != null && distLo < 2) { s += 1; scoreDetail.push(['布林', '极近下轨', 'bull']) }
                          else if (distHi != null && distHi > -2) { s -= 1; scoreDetail.push(['布林', '极近上轨', 'bear']) }
                          else if (distLo != null && distLo < 5) { scoreDetail.push(['布林', '近下轨', 'neutral']) }
                          else if (distHi != null && distHi > -5) { scoreDetail.push(['布林', '近上轨', 'neutral']) }
                          else scoreDetail.push(['布林', '中部', 'neutral'])
                        }
                        const bullN = scoreDetail.filter((x) => x[2] === 'bull').length
                        const bearN = scoreDetail.filter((x) => x[2] === 'bear').length
                        let verdict, verdictColor
                        if (s >= 4) { verdict = '强烈看多'; verdictColor = 'bull' }
                        else if (s >= 1) { verdict = '偏多'; verdictColor = 'bull' }
                        else if (s <= -4) { verdict = '强烈看空'; verdictColor = 'bear' }
                        else if (s <= -1) { verdict = '偏空'; verdictColor = 'bear' }
                        else { verdict = '中性震荡'; verdictColor = 'neutral' }
                        const nearUpper = distHi != null && distHi > -5
                        const nearLower = distLo != null && distLo < 5
                        const belowLower = close != null && lower != null && close < lower
                        const aboveUpper = close != null && upper != null && close > upper
                        let advice
                        if (verdictColor === 'bull') {
                          if (nearUpper || aboveUpper) advice = '技术面偏多，但价格已逼近/突破布林上轨，追高性价比低，建议等回踩中轨再介入'
                          else if (nearLower || belowLower) advice = '多项指标看多且价格贴近布林下轨（超卖），安全边际较高，可逢低分批建仓，止损设于下轨下方'
                          else advice = '技术面偏多，可持股或轻仓参与，以中轨为参考止盈、下轨为止损'
                        } else if (verdictColor === 'bear') {
                          if (nearLower || belowLower) advice = '技术面偏弱但价格已超卖（近下轨），或有技术反弹，不宜盲目杀跌，可等反抽减仓'
                          else if (nearUpper || aboveUpper) advice = '多项指标转空且价格处于高位，风险较大，建议减仓回避'
                          else advice = '技术面偏弱，控制仓位、以观望为主，反弹至中轨附近可考虑减仓'
                        } else {
                          advice = '多空信号交织、方向不明，建议观望，等待均线或 MACD 给出明确拐点'
                        }

                        return (
                          <>
                          <div className="analysis-summary">
                            <div className="as-head">
                              <span className="as-title">📋 总体总结</span>
                              <span className={cn('as-verdict', `as-${verdictColor}`)}>{verdict}</span>
                              <span className="as-score">综合强度 {s > 0 ? '+' : ''}{s} · 多 {bullN} / 空 {bearN}</span>
                            </div>
                            <div className="as-bars">
                              {scoreDetail.map(([nm, txt, side]) => (
                                <div key={nm} className={cn('as-bar', `as-${side}`)}>
                                  <span className="as-bar-name">{nm}</span>
                                  <span className="as-bar-txt">{txt}</span>
                                </div>
                              ))}
                            </div>
                            <div className="as-advice">💡 操作建议：{advice}</div>
                          </div>
                          <div className="ind-grid">
                            {/* ① RSI */}
                            <div className="ind-card">
                              <span className="ind-label">RSI(14)</span>
                              <span className={cn('ind-val', rsi > 70 ? 'text-up' : rsi < 30 ? 'text-down' : '')}>{rsi != null ? rsi.toFixed(1) : '--'}</span>
                              {rsi != null ? (
                                <div className="ind-gauge">
                                  <div className="ig-track">
                                    <div className="ig-zone ig-sold" style={{ width: '30%' }} />
                                    <div className="ig-zone ig-neutral" style={{ width: '40%' }} />
                                    <div className="ig-zone ig-bought" style={{ width: '30%' }} />
                                    <div className="ig-fill" style={{ left: `${Math.min(100, Math.max(0, rsi))}%` }} />
                                  </div>
                                  <span className="ig-labels"><em>0</em><em>30</em><em>70</em><em>100</em></span>
                                </div>
                              ) : null}
                              <span className="ind-txt">{rsiTxt}</span>
                            </div>

                            {/* ② MACD — 零轴柱状图 */}
                            <div className="ind-card">
                              <span className="ind-label">MACD(12,26,9)</span>
                              <div className="ind-row-val">
                                <span>DIF <strong className={cn(dif != null && dif > 0 ? 'text-up' : dif != null ? 'text-down' : '')}>{dif != null ? dif.toFixed(3) : '--'}</strong></span>
                                <span>DEA <strong className={cn(dea != null && dea > 0 ? 'text-up' : dea != null ? 'text-down' : '')}>{dea != null ? dea.toFixed(3) : '--'}</strong></span>
                                <span>柱 <strong className={cn(macdH != null && macdH > 0 ? 'text-up' : macdH != null ? 'text-down' : '')}>{macdH != null ? macdH.toFixed(3) : '--'}</strong></span>
                              </div>
                              {dif != null && dea != null ? (
                                <div className="macd-viz">
                                  <div className="macd-zero-line" />
                                  <div className="macd-hist-bar">
                                    <div className="macd-hist-fill" style={{
                                      height: macdH != null ? `${Math.min(50, Math.abs(macdH) * 400)}%` : '0%',
                                      bottom: macdH >= 0 ? '50%' : 'auto',
                                      top: macdH < 0 ? '50%' : 'auto',
                                      background: macdH >= 0 ? 'hsla(3, 80%, 50%, 0.75)' : 'hsla(157, 81%, 37%, 0.75)',
                                    }} />
                                  </div>
                                  <div className="macd-marker" style={{ bottom: `${50 - Math.min(48, Math.max(-48, dif * 300))}%` }} title={`DIF ${dif.toFixed(3)}`}>
                                    <div className="macd-m-dot macd-dif-dot" />
                                  </div>
                                  <div className="macd-marker" style={{ bottom: `${50 - Math.min(48, Math.max(-48, dea * 300))}%` }} title={`DEA ${dea.toFixed(3)}`}>
                                    <div className="macd-m-dot macd-dea-dot" />
                                  </div>
                                  <div className="macd-viz-labels">
                                    <span>DIF</span><span>0</span><span>DEA</span>
                                  </div>
                                </div>
                              ) : null}
                              <span className="ind-txt">{macdTxt}</span>
                            </div>

                            {/* ③ KDJ — 三线仪表盘 */}
                            <div className="ind-card">
                              <span className="ind-label">KDJ(9,3,3)</span>
                              <div className="ind-row-val">
                                <span>K <strong>{kV != null ? kV.toFixed(1) : '--'}</strong></span>
                                <span>D <strong>{dV != null ? dV.toFixed(1) : '--'}</strong></span>
                                <span>J <strong className={cn(jV != null && jV > 100 ? 'text-up' : jV != null && jV < 0 ? 'text-down' : '')}>{jV != null ? jV.toFixed(1) : '--'}</strong></span>
                              </div>
                              {kV != null && dV != null ? (
                                <div className="kdj-gauge">
                                  <div className="kg-track">
                                    <div className="kg-zone kg-os" style={{ width: '20%' }} />
                                    <div className="kg-zone kg-neutral" style={{ width: '60%' }} />
                                    <div className="kg-zone kg-ob" style={{ width: '20%' }} />
                                    {/* K pointer */}
                                    <div className="kg-pointer" style={{ left: `${Math.min(100, Math.max(0, kV))}%` }} title={`K ${kV.toFixed(1)}`}>
                                      <div className="kg-pin kg-k-pin" />
                                    </div>
                                    {/* D pointer */}
                                    <div className="kg-pointer" style={{ left: `${Math.min(100, Math.max(0, dV))}%` }} title={`D ${dV.toFixed(1)}`}>
                                      <div className="kg-pin kg-d-pin" />
                                    </div>
                                    {jV != null ? (
                                      <div className={cn('kg-pointer', jV > 100 || jV < 0 ? 'kg-extreme' : '')} style={{ left: `${Math.min(100, Math.max(0, jV))}%` }} title={`J ${jV.toFixed(1)}`}>
                                        <div className="kg-pin kg-j-pin" />
                                      </div>
                                    ) : null}
                                  </div>
                                  <div className="kg-labels"><em>0</em><em>20</em><em>80</em><em>100</em></div>
                                </div>
                              ) : null}
                              <span className="ind-txt">{kdjTxt}</span>
                            </div>

                            {/* ④ 均线系统 — 排列梯形图 */}
                            <div className="ind-card">
                              <span className="ind-label">均线系统</span>
                              <div className="ind-ma-row">
                                <span className="ind-ma-item">MA5 <strong>{ma5 != null ? ma5.toFixed(2) : '--'}</strong></span>
                                <span className="ind-ma-item">MA10 <strong>{ma10 != null ? ma10.toFixed(2) : '--'}</strong></span>
                                <span className="ind-ma-item">MA20 <strong>{ma20 != null ? ma20.toFixed(2) : '--'}</strong></span>
                                {ma60 != null ? <span className="ind-ma-item">MA60 <strong>{ma60.toFixed(2)}</strong></span> : null}
                              </div>
                              {(ma5 != null && ma10 != null && ma20 != null) ? (
                                <div className="ma-ladder">
                                  {[ma5, ma10, ma20].sort((a, b) => b - a).map((val, idx) => {
                                    const labels = { [ma5]: 'MA5', [ma10]: 'MA10', [ma20]: 'MA20', [ma60 ?? 0]: 'MA60' }
                                    const label = labels[val] || `M${idx}`
                                    const colors = { [ma5]: '#3B82F6', [ma10]: '#8B5CF6', [ma20]: '#F59E0B', [ma60 ?? 0]: '#6B7280' }
                                    const maxVal = Math.max(ma5, ma10, ma20, ma60 ?? 0)
                                    const minVal = Math.min(ma5, ma10, ma20, ma60 ?? 0)
                                    const range = maxVal - minVal || 1
                                    return (
                                      <div key={label} className="ma-ladder-row">
                                        <span className="ma-ladder-label" style={{ color: colors[val] || 'inherit' }}>{label}</span>
                                        <div className="ma-ladder-track">
                                          <div className="ma-ladder-fill" style={{
                                            width: `${((val - minVal) / range) * 100}%`,
                                            background: colors[val] || 'hsl(var(--foreground))',
                                          }} />
                                        </div>
                                        <span className="ma-ladder-val">{val.toFixed(2)}</span>
                                      </div>
                                    )
                                  })}
                                  {ma60 != null ? (
                                    <div className="ma-ladder-row">
                                      <span className="ma-ladder-label" style={{ color: '#6B7280' }}>MA60</span>
                                      <div className="ma-ladder-track">
                                        <div className="ma-ladder-fill" style={{
                                          width: `${((ma60 - Math.min(ma5, ma10, ma20, ma60)) / (Math.max(ma5, ma10, ma20, ma60) - Math.min(ma5, ma10, ma20, ma60) || 1)) * 100}%`,
                                          background: '#6B7280',
                                        }} />
                                      </div>
                                      <span className="ma-ladder-val">{ma60.toFixed(2)}</span>
                                    </div>
                                  ) : null}
                                </div>
                              ) : null}
                              {/* 均线排列箭头 */}
                              {(ma5 != null && ma10 != null && ma20 != null) ? (
                                <div className="ind-ma-arrows">
                                  {ma5 > ma10 ? <span className="text-up">↑</span> : ma5 < ma10 ? <span className="text-down">↓</span> : <span>=</span>}
                                  {ma10 > ma20 ? <span className="text-up">↑</span> : ma10 < ma20 ? <span className="text-down">↓</span> : <span>=</span>}
                                  {ma20 > (ma60 ?? 0) ? <span className="text-up">↑</span> : ma20 < (ma60 ?? 999) ? <span className="text-down">↓</span> : <span>=</span>}
                                </div>
                              ) : null}
                              <span className="ind-txt">{maTxt}</span>
                            </div>

                            {/* ⑤ 布林带价格位置 */}
                            <div className="ind-card ind-card-wide">
                              <span className="ind-label">布林带 · 价格位置</span>
                              {close != null && lower != null && upper != null && middle != null ? (
                                <>
                                  <div className="ind-band-bar">
                                    <div className="ibb-track">
                                      <div className="ibb-lower" />
                                      <div className="ibb-middle" />
                                      <div className="ibb-upper" />
                                      {/* price dot position: lower=0%, upper=100% */}
                                      <div className="ibb-dot" style={{
                                        left: `${Math.min(100, Math.max(0, ((close - lower) / (upper - lower)) * 100))}%`,
                                        top: close > upper ? '-14px' : close < lower ? '22px' : '50%',
                                        transform: 'translateX(-50%) translateY(-50%)',
                                      }} />
                                    </div>
                                    <div className="ibb-labels">
                                      <span>下轨 {lower.toFixed(2)}</span>
                                      <span>中轨 {middle.toFixed(2)}</span>
                                      <span>上轨 {upper.toFixed(2)}</span>
                                    </div>
                                  </div>
                                  <div className="ind-pos-stats">
                                    <span>收盘 <strong>{close.toFixed(2)}</strong></span>
                                    {distLo != null ? <span>距下轨 <strong className={distLo < 5 ? 'text-down' : ''}>{distLo > 0 ? '+' : ''}{distLo.toFixed(2)}%</strong></span> : null}
                                    {distHi != null ? <span>距上轨 <strong className={distHi > -5 ? 'text-up' : ''}>{distHi > 0 ? '+' : ''}{distHi.toFixed(2)}%</strong></span> : null}
                                    {bw != null ? <span className={cn(bwWarn ? 'text-up font-medium' : '')}>带宽 {bw.toFixed(1)}%{bwWarn ? ' ⚠收窄' : ''}</span> : null}
                                  </div>
                                </>
                              ) : null}
                              <span className="ind-txt">{posTxt}</span>
                            </div>
                          </div>
                          </>
                        )
                      })()}
                    </>
                  ) : <div className="empty-state">选择或输入股票代码后点击「加载分析」</div>}
                </SectionCard>
            </div>
          </>
        ) : null}

        {activeView === 'daily' ? (
          <>
            <div className="page-header">
              <h2>每日策略信号日报</h2>
              <p>邮件级摘要 · 全市场多策略信号汇总（点选任意标的跳转分析）</p>
            </div>
            {fullDaily?.rows?.length > 0 ? (() => {
              const rows = fullDaily.rows
              const num = (r, k) => { const v = r[k]; if (v == null || v === '') return null; const n = Number(v); return (isNaN(n) || !isFinite(n)) ? null : n }
              const total = rows.length
              const totalAmt = rows.reduce((s, r) => s + (num(r, '建议金额') || 0), 0)
              const avgScore = total ? rows.reduce((s, r) => s + (num(r, '综合评分') || 0), 0) / total : 0
              // 策略分布
              const stratMap = {}
              rows.forEach((r) => { const k = (r['来源策略'] ?? '未知').split('/').map(s=>s.trim()).filter(Boolean).join('/'); stratMap[k] = (stratMap[k] || 0) + 1 })
              const stratList = Object.entries(stratMap).sort((a, b) => b[1] - a[1])
              // 重点推荐 Top5（按综合评分）
              const top = [...rows].sort((a, b) => (num(b, '综合评分') || 0) - (num(a, '综合评分') || 0)).slice(0, 5)
              const fileDate = (fullDaily.latest?.name ?? '').replace('Daily-Action-List-', '').replace('.csv', '')
              const genTime = fullDaily.latest?.modified
                ? new Date(fullDaily.latest.modified * 1000).toLocaleString('zh-CN')
                : ''

              return (
                <>
                  {/* 报告头 */}
                  <div className="report-head">
                    <div>
                      <div className="report-title">每日策略信号日报</div>
                      <div className="report-sub">日期 {fileDate} · 生成于 {genTime}</div>
                    </div>
                    <div className="report-tag">
                      {dailyDates.length > 0 ? (
                        <div className="date-picker-wrap">
                          <div
                            className="date-picker-input"
                            onClick={() => setDateOpen(o => !o)}
                          >
                            <span className="dpi-value">
                              {(dailyDate ?? fileDate).slice(0,4)}/{(dailyDate ?? fileDate).slice(4,6)}/{(dailyDate ?? fileDate).slice(6)}
                            </span>
                            <svg className="dpi-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                              <rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/>
                              <line x1="3" y1="10" x2="21" y2="10"/>
                            </svg>
                          </div>
                          {dateOpen && (
                            <div className="dp-dropdown">
                              <div className="dp-grid">
                                {dailyDates.map((it) => {
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
                      ) : null}
                      <span>共 {total} 只标的</span>
                    </div>
                  </div>

                  {/* 统计卡 */}
                  <div className="report-stats">
                    <div className="rs-card">
                      <span className="rs-label">信号总数</span>
                      <span className="rs-val">{total}</span>
                    </div>
                    <div className="rs-card">
                      <span className="rs-label">建议总金额</span>
                      <span className="rs-val">¥{(totalAmt / 10000).toFixed(1)}万</span>
                    </div>
                    <div className="rs-card">
                      <span className="rs-label">平均综合评分</span>
                      <span className="rs-val">{avgScore.toFixed(1)}</span>
                    </div>
                    <div className="rs-card">
                      <span className="rs-label">涉及策略</span>
                      <span className="rs-val">{stratList.length} 类</span>
                    </div>
                  </div>

                  {/* 策略分布 + 重点推荐 */}
                  <div className="report-grid">
                    <SectionCard title="策略命中分布">
                      <div className="strat-list">
                        {stratList.map(([name, cnt]) => (
                          <div key={name} className="strat-row">
                            <span className="strat-name">{name}</span>
                            <span className="strat-bar"><span className="strat-bar-fill" style={{ width: `${(cnt / total) * 100}%` }} /></span>
                            <span className="strat-cnt">{cnt}</span>
                          </div>
                        ))}
                      </div>
                    </SectionCard>

                    <SectionCard title="重点推荐 · Top 5">
                      <div className="top-list">
                        {top.map((r, i) => {
                          const buy = num(r, '建议买入价')
                          const stop = num(r, '止损价(下轨)')
                          const tp = num(r, '止盈价(上轨)')
                          const latest = num(r, '最新价')
                          const rawRr = (buy && stop && tp && (buy - stop) !== 0) ? ((tp - buy) / (buy - stop)) : null
                          // 截断极端盈亏比，超过 ±99 显示为 ∞/—∞
                          const rr = rawRr !== null ? (Math.abs(rawRr) > 99 ? (rawRr > 0 ? Infinity : -Infinity) : rawRr) : null
                          const rrStr = rr === Infinity ? '∞' : rr === -Infinity ? '—∞' : rr != null ? `${rr.toFixed(1)}:1` : '—'
                          const hitCnt = num(r, '命中策略数') ?? 0
                          const srcStr = String(r['来源策略'] ?? '').trim()
                          const strategies = srcStr ? srcStr.split('/').filter(Boolean) : []
                          const code = String(r['股票代码'] ?? '').trim().padStart(6, '0')
                          // 价格偏离度
                          const priceDev = (buy && latest) ? ((latest / buy - 1) * 100) : null

                          return (
                            <div key={r['股票代码']} className="top-row" onClick={() => { if (code && code !== '000000') { setAnalysisCode(code); setActiveView('analysis'); openAnalysis(code) } }}>
                              <div className="top-left">
                                <span className={`top-no ${i < 3 ? 'top-no--medal' : ''}`}>{i + 1}</span>
                                <div className="top-info">
                                  <div className="top-main">
                                    <span className="top-code">{code}</span>
                                    <span className="top-name">{r['股票名称']}</span>
                                    {/* 策略标签 */}
                                    {strategies.length > 0 && (
                                      <div className="top-tags">
                                        {strategies.map((s) => (
                                          <span key={s} className={`top-tag top-tag--${s.toLowerCase()}`}>{STRAT_LABEL[s] || s}</span>
                                        ))}
                                      </div>
                                    )}
                                  </div>
                                  <div className="top-sub">
                                    {buy != null && <span className="top-detail">买入 ¥{buy.toFixed(2)}</span>}
                                    {latest != null && <span className={`top-detail ${priceDev != null ? (priceDev >= 0 ? 'text-up' : 'text-down') : ''}`}>现价 ¥{latest.toFixed(2)}{priceDev != null ? ` (${priceDev >= 0 ? '+' : ''}${priceDev.toFixed(1)}%)` : ''}</span>}
                                    {num(r, '建议仓位%') != null && <span className="top-detail">仓位 {num(r, '建议仓位%')}%</span>}
                                  </div>
                                </div>
                              </div>
                              <div className="top-right">
                                <div className="top-metrics">
                                  <div className="top-score-wrap">
                                    <span className="top-score">{num(r, '综合评分')?.toFixed(1) ?? '--'}</span>
                                    {hitCnt > 1 && <span className="top-hit-badge">{hitCnt}策略</span>}
                                  </div>
                                  <span className={`top-rr ${rr != null && isFinite(rr) ? (rr >= 2 ? 'text-up' : rr >= 1 ? '' : 'text-down') : (rr === Infinity ? 'text-up' : '')}`}>
                                    盈亏比 {rrStr}
                                  </span>
                                </div>
                              </div>
                            </div>
                          )
                        })}
                      </div>
                    </SectionCard>
                  </div>

                  {/* 完整明细：二层可展开 */}
                  <SectionCard title={`完整信号明细 · ${total} 行`} subtitle={fullDaily.latest?.path ?? ''} className="max-w-none">
                    <DailyExpandableList
                      rows={rows}
                      onCodeClick={(code) => { const c = String(code).trim(); if (c) { setAnalysisCode(c); setActiveView('analysis'); openAnalysis(c) } }}
                    />
                  </SectionCard>
                </>
              )
            })() : (
              <div className="empty-state" style={{ padding: '60px 24px' }}>
                <div style={{ fontSize: '2.4rem', marginBottom: 12 }}>📋</div>
                <div style={{ fontSize: '1rem', fontWeight: 600, color: 'hsl(var(--foreground))', marginBottom: 6 }}>
                  {fullDaily === null ? '正在加载日报数据...' : '暂无日报文件'}
                </div>
                <div style={{ fontSize: '0.84rem', color: 'hsl(var(--muted))', lineHeight: 1.7 }}>
                  {fullDaily === null
                    ? '首次加载可能需要几秒钟，请稍候'
                    : '运行「选股」跑完融合流程后，这里会显示完整每日信号日报'
                  }
                </div>
              </div>
            )}
          </>
        ) : null}

        {activeView === 'portfolio' ? (
          <>
            <div className="page-header">
              <h2>持仓管理</h2>
              <p>查看持仓、录入交易</p>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <SectionCard title="持仓概览">
                {openPositions.length > 0 ? (
                  <div className="index-list">
                    {openPositions.slice(0, 5).map((item) => (
                      <div key={`${item.代码}-${item.买入日期}`} className="index-row">
                        <span>{item.代码}</span>
                        <strong>{item.数量}</strong>
                        <em>{Number(item.成本金额).toFixed(2)}</em>
                      </div>
                    ))}
                  </div>
                ) : <div className="empty-state">还没有持仓记录</div>}
              </SectionCard>

              <SectionCard title="交易录入">
                <div className="trade-form">
                  <input value={tradeForm.date} type="date" className="h-9 rounded-lg bg-card border border-border px-3 text-foreground"
                    onChange={(e) => setTradeForm((p) => ({ ...p, date: e.target.value }))} />
                  <input value={tradeForm.code} placeholder="股票代码" className="h-9 rounded-lg bg-card border border-border px-3 text-foreground"
                    onChange={(e) => setTradeForm((p) => ({ ...p, code: e.target.value }))} />
                  <input value={tradeForm.name} placeholder="股票名称" className="h-9 rounded-lg bg-card border border-border px-3 text-foreground"
                    onChange={(e) => setTradeForm((p) => ({ ...p, name: e.target.value }))} />
                  <select value={tradeForm.side} className="h-9 rounded-lg bg-card border border-border px-3 text-foreground"
                    onChange={(e) => setTradeForm((p) => ({ ...p, side: e.target.value }))}>
                    <option value="buy">买入</option>
                    <option value="sell">卖出</option>
                  </select>
                  <input value={tradeForm.price} type="number" min="0" step="0.01" placeholder="价格" className="h-9 rounded-lg bg-card border border-border px-3 text-foreground"
                    onChange={(e) => setTradeForm((p) => ({ ...p, price: Number(e.target.value) }))} />
                  <input value={tradeForm.qty} type="number" min="1" step="1" placeholder="数量" className="h-9 rounded-lg bg-card border border-border px-3 text-foreground"
                    onChange={(e) => setTradeForm((p) => ({ ...p, qty: Number(e.target.value) }))} />
                  <input value={tradeForm.fee} type="number" min="0" step="0.01" placeholder="手续费" className="h-9 rounded-lg bg-card border border-border px-3 text-foreground"
                    onChange={(e) => setTradeForm((p) => ({ ...p, fee: Number(e.target.value) }))} />
                  <input value={tradeForm.notes} placeholder="备注" className="h-9 rounded-lg bg-card border border-border px-3 text-foreground"
                    onChange={(e) => setTradeForm((p) => ({ ...p, notes: e.target.value }))} />
                  <Button onClick={async () => {
                    const r = await fetch('/api/trades', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(tradeForm) })
                    if (r.ok) { const p = await fetch('/api/portfolio'); if (p.ok) setPortfolio(await p.json()) }
                  }}>保存交易</Button>
                  <Button variant="destructive" onClick={async () => {
                    if (!window.confirm('确定要清空所有交易记录吗？此操作不可撤销。')) return
                    const r = await fetch('/api/trades', { method: 'DELETE' })
                    if (r.ok) { const p = await fetch('/api/portfolio'); if (p.ok) setPortfolio(await p.json()) }
                  }}>清空</Button>
                </div>
                <div className="grid grid-cols-3 gap-3 mt-3">
                  <StatCard label="持仓成本" value={pnlSummary.holding_cost ?? '--'} />
                  <StatCard label="当前市值" value={pnlSummary.holding_value ?? '--'} />
                  <StatCard label="浮动盈亏" value={pnlSummary.total_pnl ?? '--'} />
                </div>
                {realtimePositions.length > 0 ? (
                  <div className="table-shell spaced">
                    {realtimePositions.slice(0, 8).map((row) => (
                      <div key={`${row.代码}-${row.买入日期}`} className="table-row">
                        <span>{row.代码}</span>
                        <strong>{Number(row.现价 ?? row.成本价 ?? 0).toFixed(2)}</strong>
                        <em>{row.浮动盈亏 ?? '--'}</em>
                      </div>
                    ))}
                  </div>
                ) : <div className="empty-state">暂无实时持仓</div>}
              </SectionCard>
            </div>
          </>
        ) : null}

        {activeView === 'backtest' ? (
          <>
            <div className="page-header">
              <h2>回测结果</h2>
              <p>每日 CI 自动回测当日全策略清单，或手动运行多策略回测</p>
            </div>

            <SectionCard title="每日自动回测 · 前向信号回测" subtitle="点击下方日期标签切换不同信号日的回测结果">
              {dailySummary && dailySummary.count > 0 ? (
                <div className="dbt-summary">
                  <div className="dbt-summary-head">
                    <span className="dbt-summary-title">📊 总体总结</span>
                    <span className="dbt-summary-sub">近 {dailySummary.count} 个信号日前向回测（平均持有 {dailySummary.avg_hold_days} 天）的整体表现</span>
                  </div>
                  <div className="dbt-summary-grid">
                    {/* ── 1. 平均总收益 ── */}
                    <div className={cn("dbt-summary-cell", dailySummary.avg_return >= 0 ? "dbt-cell-good" : "dbt-cell-bad")}
                      title="每个信号日买入后、持有到期卖出，扣除交易成本（佣金+印花税）后的平均收益率。正数=整体赚钱，负数=整体亏钱。">
                      <div className="dbt-cell-icon-wrap dbt-icon-green">
                        <TrendingUp className="dbt-cell-icon" />
                      </div>
                      <div className="dbt-cell-body">
                        <div className="dbt-summary-label">平均总收益</div>
                        <div className="dbt-summary-val">{dailySummary.avg_return >= 0 ? "+" : ""}{dailySummary.avg_return}%</div>
                        <div className="dbt-summary-hint">中位 {dailySummary.median_return >= 0 ? "+" : ""}{dailySummary.median_return}% · 扣除成本后</div>
                      </div>
                    </div>
                    {/* ── 2. 平均最大回撤 ── */}
                    <div className={cn("dbt-summary-cell", "dbt-cell-bad")}
                      title="持有期间权益从峰值下跌的最大幅度。回撤越小越好——说明风控越稳、心态压力越小。">
                      <div className="dbt-cell-icon-wrap dbt-icon-red">
                        <TrendingDown className="dbt-cell-icon" />
                      </div>
                      <div className="dbt-cell-body">
                        <div className="dbt-summary-label">平均最大回撤</div>
                        <div className="dbt-summary-val">{dailySummary.avg_drawdown}%</div>
                        <div className="dbt-summary-hint">中位 {dailySummary.median_drawdown}% · 越小风控越稳</div>
                      </div>
                    </div>
                    {/* ── 3. 正收益天数 ── */}
                    <div className={cn("dbt-summary-cell", dailySummary.positive_ratio >= 50 ? "dbt-cell-good" : "dbt-cell-bad")}
                      title="所有信号日中，最终盈利的天数占比。≥50% 说明多数时候能赚到钱，是策略稳定性的核心指标。">
                      <div className="dbt-cell-icon-wrap dbt-icon-blue">
                        <Target className="dbt-cell-icon" />
                      </div>
                      <div className="dbt-cell-body">
                        <div className="dbt-summary-label">正收益天数</div>
                        <div className="dbt-summary-val">{dailySummary.positive_days}<span className="dbt-val-slash">/</span>{dailySummary.count}</div>
                        <div className="dbt-summary-hint">占比 {dailySummary.positive_ratio}% · ≥50%多数日能赚</div>
                      </div>
                    </div>
                    {/* ── 4. 平均胜率 ── */}
                    <div className={cn("dbt-summary-cell", dailySummary.avg_win_rate >= 50 ? "dbt-cell-good" : dailySummary.avg_win_rate >= 35 ? "dbt-cell-neutral" : "dbt-cell-bad")}
                      title="全部成交笔中盈利笔数的占比。每笔交易盈亏相加→胜率×均赢 − (1−胜率)×均亏。高胜率是长期盈利的基础。">
                      <div className="dbt-cell-icon-wrap dbt-icon-purple">
                        <Percent className="dbt-cell-icon" />
                      </div>
                      <div className="dbt-cell-body">
                        <div className="dbt-summary-label">平均胜率</div>
                        <div className="dbt-summary-val">{dailySummary.avg_win_rate}%</div>
                        <div className="dbt-summary-hint">中位 {dailySummary.median_win_rate}% · 单笔盈利占比</div>
                      </div>
                    </div>
                    {/* ── 5. 平均夏普 ── */}
                    <div className={cn("dbt-summary-cell", dailySummary.avg_sharpe >= 0 ? "dbt-cell-good" : "dbt-cell-bad")}
                      title="夏普比率 = （收益率 − 无风险利率）/ 收益标准差。衡量「承担单位风险获得的超额回报」。>1 优秀，>2 极好。">
                      <div className="dbt-cell-icon-wrap dbt-icon-amber">
                        <BarChart3 className="dbt-cell-icon" />
                      </div>
                      <div className="dbt-cell-body">
                        <div className="dbt-summary-label">平均夏普</div>
                        <div className="dbt-summary-val">{dailySummary.avg_sharpe}</div>
                        <div className="dbt-summary-hint">累计成交 {dailySummary.total_trades} 笔 · 风险调整后回报</div>
                      </div>
                    </div>
                    {/* ── 6. 超额收益 vs 沪深300（NEW）── */}
                    {dailySummary.excess_return != null ? (
                    <div className={cn("dbt-summary-cell", dailySummary.excess_return >= 0 ? "dbt-cell-good" : "dbt-cell-bad")}
                      title={`同期沪深300 ${dailySummary.benchmark_return >= 0 ? '+' : ''}${dailySummary.benchmark_return}% | 策略跑${dailySummary.excess_return >= 0 ? '赢' : '输'}大盘 ${(Math.abs(dailySummary.excess_return)).toFixed(2)} 个百分点。超额为正是 alpha 的直接证据——策略有独立选股能力。`}>
                      <div className="dbt-cell-icon-wrap dbt-icon-purple">
                        <Zap className="dbt-cell-icon" />
                      </div>
                      <div className="dbt-cell-body">
                        <div className="dbt-summary-label">{dailySummary.benchmark_name || '沪深300'}</div>
                        <div className="dbt-summary-val">{dailySummary.excess_return >= 0 ? "+" : ""}{dailySummary.excess_return}%</div>
                        <div className="dbt-summary-hint">基准 {dailySummary.benchmark_return >= 0 ? "+" : ""}{dailySummary.benchmark_return}% · 策略跑{dailySummary.excess_return >= 0 ? '赢' : '输'}大盘</div>
                      </div>
                    </div>
                    ) : (
                    <div className="dbt-summary-cell dbt-cell-neutral"
                      title="沪深300 基准数据暂不可用（指数序列拉取失败），无法计算超额收益。">
                      <div className="dbt-cell-body">
                        <div className="dbt-summary-label">vs 沪深300</div>
                        <div className="dbt-summary-val">—</div>
                        <div className="dbt-summary-hint">基准暂不可用</div>
                      </div>
                    </div>
                    )}
                    {/* 最佳 / 最差信号日 */}
                    <div className="dbt-summary-cell dbt-cell-bestworst">
                      <div className="dbt-bw-row">
                        <div className="dbt-bw-item dbt-bw-best">
                          <Trophy className="dbt-bw-icon" />
                          <div>
                            <div className="dbt-bw-label">最佳日</div>
                            <div className="dbt-bw-val">{dailySummary.best_day.date?.slice(4).replace(/(\d{2})(\d{2})/, "$1-$2")}</div>
                            <div className="dbt-bw-ret stat-good">+{dailySummary.best_day.return}%</div>
                          </div>
                        </div>
                        <div className="dbt-bw-divider"></div>
                        <div className="dbt-bw-item dbt-bw-worst">
                          <AlertTriangle className="dbt-bw-icon" />
                          <div>
                            <div className="dbt-bw-label">最差日</div>
                            <div className="dbt-bw-val">{dailySummary.worst_day.date?.slice(4).replace(/(\d{2})(\d{2})/, "$1-$2")}</div>
                            <div className="dbt-bw-ret stat-bad">{dailySummary.worst_day.return}%</div>
                          </div>
                        </div>
                      </div>
                      <div className="dbt-summary-hint dbt-bw-hint">整体稳健度参考</div>
                    </div>
                  </div>
                </div>
              ) : null}
              {dailyBacktests.length === 0 ? (
                <div className="bt-empty">
                  <div className="bt-empty-icon">🌙</div>
                  <div className="bt-empty-title">尚无每日前向回测数据</div>
                  <div className="bt-empty-desc">每日盘后 CI 会对近一个月的每个历史信号日分别做「从信号日往后持有」的前向回测，结果将出现在这里。</div>
                </div>
              ) : (
                <>
                  <div className="dbt-tabs-wrap">
                    <div className="dbt-tabs-label">📅 选择信号日（点击切换）</div>
                    <div className="date-picker-wrap">
                      <div
                        className="date-picker-input"
                        onClick={() => setBtDateOpen(o => !o)}
                      >
                        <span className="dpi-value">
                          {dailyBacktests[selDaily]?.date
                            ? `${dailyBacktests[selDaily].date.slice(0,4)}/${dailyBacktests[selDaily].date.slice(4,6)}/${dailyBacktests[selDaily].date.slice(6)}`
                            : '--'}
                        </span>
                        <svg className="dpi-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/>
                          <line x1="3" y1="10" x2="21" y2="10"/>
                        </svg>
                      </div>
                      {btDateOpen && (
                        <div className="dp-dropdown">
                          <div className="dp-grid dp-grid--bt">
                            {dailyBacktests.map((d, i) => {
                              const active = i === selDaily
                              return (
                                <button
                                  key={d.date}
                                  type="button"
                                  className={`dp-item${active ? ' dp-active' : ''}`}
                                  onClick={() => { setSelDaily(i); setBtDateOpen(false) }}
                                >
                                  <span>{d.date.slice(0,4)}/{d.date.slice(4,6)}/{d.date.slice(6)}</span>
                                  <span className="dp-badge">{d.summary?.hold_days ?? 0}天</span>
                                </button>
                              )
                            })}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                  {dailyBacktests[selDaily]?.summary ? (() => {
                    const dailyBacktest = dailyBacktests[selDaily]
                const s = dailyBacktest.summary
                const ret = Number(s.total_return ?? 0)
                const dd = Number(s.max_drawdown ?? 0)
                const wr = Number(s.win_rate ?? 0)
                const sharpe = Number(s.sharpe ?? 0)
                const trades = Number(s.num_trades ?? 0)

                // 综合评级算法
                let grade = 'F', gradeLabel = '', gradeColor = ''
                let score = 0
                if (ret > 15) score += 3; else if (ret > 5) score += 2; else if (ret > 0) score += 1
                if (dd > -3) score += 2; else if (dd > -8) score += 1
                if (wr > 55) score += 2; else if (wr > 40) score += 1
                if (sharpe > 1) score += 2; else if (sharpe > 0.5) score += 1
                if (trades >= 10) score += 1

                if (score >= 9) { grade = 'A+'; gradeColor = '#16a34a' }
                else if (score >= 7) { grade = 'A'; gradeColor = '#22c55e' }
                else if (score >= 6) { grade = 'B'; gradeColor = '#84cc16' }
                else if (score >= 4) { grade = 'C'; gradeColor = '#eab308' }
                else if (score >= 2) { grade = 'D'; gradeColor = '#f97316' }
                else { grade = 'F'; gradeColor = '#ef4444' }

                // 解读文案生成
                const parts = []
                if (ret >= 5) parts.push(`总收益 ${ret >= 10 ? '强劲' : '正'}(${ret > 0 ? '+' : ''}${ret.toFixed(1)}%)`)
                else if (ret >= 0) parts.push(`微盈(${ret.toFixed(1)}%)`)
                else parts.push(`亏损(${ret.toFixed(1)}%)`)

                if (dd <= -10) parts.push(`回撤过大(${dd.toFixed(1)}%)，风控需加强`)
                else if (dd <= -5) parts.push(`回撤偏深(${dd.toFixed(1)}%)，注意止损纪律`)
                else if (dd < 0) parts.push(`回撤可控(${dd.toFixed(1)}%)`)

                if (wr >= 50) parts.push(`胜率健康(${wr.toFixed(0)}%)`)
                else if (wr >= 30) parts.push(`胜率一般(${wr.toFixed(0)}%)，依赖大单盈利`)
                else parts.push(`胜率偏低(${wr.toFixed(0)}%)`)

                if (sharpe >= 1) parts.push(`夏普优秀(${sharpe.toFixed(2)})`)
                else if (sharpe >= 0) parts.push(`夏普一般(${sharpe.toFixed(2)})`)
                else parts.push(`夏普为负(${sharpe.toFixed(2)})，风险调整后收益不佳`)

                // 找最大回撤区间（从 equity 数据中）
                let worstPeriod = ''
                const eq = dailyBacktest.equity || []
                if (eq.length > 5) {
                  let minVal = Infinity, minIdx = -1, peakBefore = eq[0].equity ?? 0
                  eq.forEach((e, idx) => {
                    const val = e.equity ?? 0
                    if (val > peakBefore) peakBefore = val
                    const ddFromPeak = peakBefore ? ((val / peakBefore - 1) * 100) : 0
                    if (ddFromPeak < minVal) { minVal = ddFromPeak; minIdx = idx }
                  })
                  if (minIdx >= 0 && eq[minIdx]) {
                    worstPeriod = `${eq[minIdx].date ?? ''} 附近回撤最深 (${minVal.toFixed(1)}%)`
                  }
                }

                // 最佳/最差交易
                const tList = dailyBacktest.trades || []
                let bestTrade = null, worstTrade = null
                tList.forEach(t => {
                  const rp = Number(t.return_pct ?? 0)
                  if (!bestTrade || rp > Number(bestTrade.return_pct ?? 0)) bestTrade = t
                  if (!worstTrade || rp < Number(worstTrade.return_pct ?? 0)) worstTrade = t
                })

                return (
                <>
                  {/* 评级头 */}
                  <div className="dbt-grade-head">
                    <div className="dbt-grade" style={{ background: `${gradeColor}15`, color: gradeColor, borderColor: `${gradeColor}40` }}>
                      <span className="dbt-grade-letter">{grade}</span>
                      <span className="dbt-grade-label">
                        {grade.startsWith('A') ? '策略表现优异' : grade === 'B' ? '策略有效' : grade === 'C' ? '策略平庸' : grade === 'D' ? '策略堪忧' : '策略失效'}
                      </span>
                    </div>
                    <p className="bt-hint" style={{ marginBottom: 0 }}>
                      前向信号回测 · 信号日 {s.signal_start}{s.signal_start !== s.signal_end ? ` ~ ${s.signal_end}` : ''} · 持有 {s.hold_days ?? '--'} 天往后回测 · 策略 {s.strategies} · {s.codes_count ?? '--'} 只标的 · {trades} 笔交易
                    </p>
                  </div>

                  {/* 统计卡 — 带语义着色 */}
                  <div className="grid grid-cols-3 lg:grid-cols-6 gap-3">
                    <StatCard label="总收益率" value={`${ret > 0 ? '+' : ''}${s.total_return ?? '--'}%`}
                      className={ret >= 3 ? 'stat-good' : ret >= 0 ? 'stat-neutral' : 'stat-bad'} />
                    <StatCard label="最大回撤" value={`${dd}%`}
                      className={dd >= -3 ? 'stat-good' : dd >= -8 ? 'stat-neutral' : 'stat-bad'} />
                    <StatCard label="胜率" value={`${wr}%`}
                      className={wr >= 50 ? 'stat-good' : wr >= 35 ? 'stat-neutral' : 'stat-bad'} />
                    <StatCard label="交易笔数" value={trades} className="stat-neutral" />
                    <StatCard label="夏普比率" value={sharpe}
                      className={sharpe >= 1 ? 'stat-good' : sharpe >= 0.3 ? 'stat-neutral' : 'stat-bad'} />
                    <StatCard label="期末权益"
                      value={s.ending_total ? `${(s.ending_total / 10000).toFixed(1)}万` : '--'}
                      className={s.ending_total && s.ending_total >= 100000 ? 'stat-good' : 'stat-neutral'} />
                  </div>

                  {/* 智能解读段落 */}
                  <div className="dbt-insight-box">
                    <div className="dbt-insight-title">📊 回测解读</div>
                    <p className="dbt-insight-text">{parts.join('；')}。{parts.length > 2 ? '综合评级反映了多维度表现的加权结果。' : ''}</p>
                    {worstPeriod && <p className="dbt-insight-detail">⚠️ {worstPeriod}</p>}
                    {bestTrade && (
                      <p className="dbt-insight-detail text-up">🏆 最佳交易：
                        {String(bestTrade.code).padStart(6,'0')}
                        {' '}({bestTrade.buy_date}→{bestTrade.sell_date})
                        {' '}{Number(bestTrade.return_pct) >= 0 ? '+' : ''}{bestTrade.return_pct}%
                      </p>
                    )}
                    {worstTrade && Math.abs(Number(worstTrade.return_pct)) > 5 && (
                      <p className="dbt-insight-detail text-down">📉 最差交易：
                        {String(worstTrade.code).padStart(6,'0')}
                        {' '}({worstTrade.buy_date}→{worstTrade.sell_date})
                        {' '}{Number(worstTrade.return_pct) >= 0 ? '+' : ''}{worstTrade.return_pct}%
                      </p>
                    )}
                  </div>

                  {/* 权益曲线 */}
                  {trades === 0 && (
                    <div className="bt-partial-note">
                      ⏳ 此信号日回测窗口尚未走完（持仓未平仓），曲线显示当前浮盈亏；待持有期满后重跑将更新为完整买卖结果。
                    </div>
                  )}
                  <EquityChart equity={dailyBacktest.equity} initialCapital={s.initial_capital ?? 100000} />

                  {/* 交易明细 */}
                  {tList.length > 0 ? (
                    <div className="table-shell spaced">
                      <div className="section-head"><h3>交易明细</h3><span>共 {tList.length} 笔</span></div>
                      {tList.slice(0, 10).map((t, i) => (
                        <div key={i} className="table-row">
                          <span className="t-code-wrap">
                            <span className="t-code">{String(t.code).padStart(6, '0')}</span>
                            {t.name ? <span className="t-name">{t.name}</span> : null}
                          </span>
                          <strong>{t.buy_date} → {t.sell_date}</strong>
                          <em className={cn(Number(t.return_pct) >= 0 ? 'text-up' : 'text-down')}>
                            {Number(t.return_pct) >= 0 ? '+' : ''}{t.return_pct}%
                          </em>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </>
                )
                })() : (
                  <div className="bt-empty">
                    <div className="bt-empty-icon">🌙</div>
                    <div className="bt-empty-title">该信号日暂无有效回测</div>
                    <div className="bt-empty-desc">可能没有可用 K 线数据（网络不可达）。</div>
                  </div>
                )
                }
              </>
            )}
            </SectionCard>

            <SectionCard title="手动多策略回测">
              <div className="bt-form">
                <div className="bt-field">
                  <label>股票代码（逗号 / 换行分隔）</label>
                  <textarea
                    className="bt-codes"
                    rows={3}
                    value={multiCodes}
                    onChange={(e) => setMultiCodes(e.target.value)}
                    placeholder="例如 600519,000858,300750"
                  />
                </div>
                <div className="bt-row">
                  <div className="bt-field">
                    <label>开始日期</label>
                    <input type="date" value={multiStart} onChange={(e) => setMultiStart(e.target.value)} />
                  </div>
                  <div className="bt-field">
                    <label>结束日期</label>
                    <input type="date" value={multiEnd} onChange={(e) => setMultiEnd(e.target.value)} />
                  </div>
                </div>
                <div className="bt-field">
                  <label>策略（多策略融合打分）</label>
                  <div className="bt-strats">
                    {['boll', 'relativity', 'theme', 'cctv'].map((s) => (
                      <label key={s} className="bt-strat">
                        <input
                          type="checkbox"
                          checked={multiStrats[s]}
                          onChange={(e) => setMultiStrats((p) => ({ ...p, [s]: e.target.checked }))}
                        />
                        {STRAT_LABEL[s]}
                      </label>
                    ))}
                  </div>
                </div>
                <button className="btn-primary" onClick={runMultiBacktest} disabled={btTaskId && backtestRun === null}>
                  运行多策略回测
                </button>
                <p className="bt-hint">
                  Backtrader 多策略引擎：Boll 低吸 + 相对强弱(Relativity，需指数) + 题材轮动量价 + CCTV 舆情(需外部输入)。
                  单票仓位上限 30%，止损=布林下轨 / 止盈=布林上轨，含 A股佣金万2.5 + 印花税千0.5。
                </p>
              </div>
            </SectionCard>

            <SectionCard title="回测">
              {backtestSummary ? (
                <>
                  <div className="grid grid-cols-3 lg:grid-cols-6 gap-3">
                    <StatCard label="总收益率" value={`${backtestSummary.total_return ?? '--'}%`} />
                    <StatCard label="最大回撤" value={`${backtestSummary.max_drawdown ?? '--'}%`} />
                    <StatCard label="胜率" value={`${backtestSummary.win_rate ?? '--'}%`} />
                    <StatCard label="交易笔数" value={backtestSummary.num_trades ?? '--'} />
                    <StatCard label="夏普比率" value={backtestSummary.sharpe ?? '--'} />
                    <StatCard label="期末权益" value={backtestSummary.ending_total ? `${(backtestSummary.ending_total / 10000).toFixed(1)}万` : '--'} />
                  </div>
                  <EquityChart equity={backtestEquity} initialCapital={backtestSummary.initial_capital ?? 100000} />
                  {backtestTrades.length > 0 ? (
                    <div className="table-shell spaced">
                      <div className="section-head"><h3>交易明细</h3><span>共 {backtestTrades.length} 笔</span></div>
                      {backtestTrades.slice(0, 10).map((t, i) => (
                        <div key={i} className="table-row">
                          <span className="t-code-wrap">
                            <span className="t-code">{String(t.code).padStart(6, '0')}</span>
                            {t.name ? <span className="t-name">{t.name}</span> : null}
                          </span>
                          <strong>{t.buy_date} → {t.sell_date}</strong>
                          <em className={cn(t.return_pct >= 0 ? 'text-up' : 'text-down')}>
                            {t.return_pct >= 0 ? '+' : ''}{t.return_pct}%
                          </em>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </>
              ) : (
                <div className="bt-empty">
                  <div className="bt-empty-icon">📊</div>
                  <div className="bt-empty-title">尚无回测结果</div>
                  <div className="bt-empty-desc">运行一次完整的选股 → 回测流程，权益曲线会自动出现在这里</div>
                  <div className="bt-steps">
                    <div className="bt-step">
                      <span className="bt-step-num">1</span>
                      <span className="bt-step-text">去「选股」设置价格区间</span>
                    </div>
                    <div className="bt-step-arrow">→</div>
                    <div className="bt-step">
                      <span className="bt-step-num">2</span>
                      <span className="bt-step-text">点击「开始选股」</span>
                    </div>
                    <div className="bt-step-arrow">→</div>
                    <div className="bt-step">
                      <span className="bt-step-num">3</span>
                      <span className="bt-step-text">等待多策略扫描 + 自动回测完成</span>
                    </div>
                    <div className="bt-step-arrow">→</div>
                    <div className="bt-step">
                      <span className="bt-step-num">4</span>
                      <span className="bt-step-text">回到这里查看权益曲线</span>
                    </div>
                  </div>
                </div>
              )}
            </SectionCard>
          </>
        ) : null}
      </main>

      {error ? (
        <div className="fixed bottom-4 right-4 px-4 py-2 rounded-lg bg-primary/10 text-primary text-sm border border-primary/25 flex items-center gap-2 shadow-lg">
          <span>{error}</span>
          <button onClick={() => setError('')} className="ml-2 text-primary/60 hover:text-primary text-xs">&times;</button>
        </div>
      ) : null}
    </>
  )
}

export default App
