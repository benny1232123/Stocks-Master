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

function StatCard({ label, value, trend }) {
  return (
    <div className="stat-card">
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
  const [tradeForm, setTradeForm] = useState({
    date: new Date().toISOString().slice(0, 10),
    code: '', name: '', side: 'buy', price: 0, qty: 100, fee: 0, notes: '',
  })
  const [error, setError] = useState('')
  const [scanLogs, setScanLogs] = useState([])
  const [dbStatus, setDbStatus] = useState(null)
  const [fullDaily, setFullDaily] = useState(null)
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
    setScanLogs((prev) => [...prev, `选股完成，开始自动回测 ${codes.length} 只股票...`])
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
              const codes = data.result.rows.map((r) => r['代码']).filter(Boolean)
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
            if (data.status === 'done' && data.result) setFusionResult(data.result)
            setScanPhase(null)
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
                      <span>股票</span><span>信号</span><span>价格</span>
                    </div>
                    {actionPreview.slice(0, 5).map((row, i) => {
                      const keys = Object.keys(row ?? {})
                      const code = row[keys[0]] ?? '--'
                      const sig = row[keys[1]] ?? '--'
                      const price = row[keys[2]] != null ? Number(row[keys[2]]) : null
                      return (
                        <div key={i} className="hpt-row">
                          <span className="hpt-code">{code}</span>
                          <span className={cn('hpt-sig', /买|多|看多/i.test(String(sig)) ? 'sig-up' : /卖|空|看空/i.test(String(sig)) ? 'sig-down' : '')}>{sig}</span>
                          <span className="hpt-price">{price != null ? price.toFixed(2) : '--'}</span>
                        </div>
                      )
                    })}
                    {actionPreview.length > 5 ? (
                      <div className="hpt-more">+{actionPreview.length - 5} 行更多</div>
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
                </div>
                <div className="hero-stat">
                  <div className="label">Shibor 隔夜</div>
                  <div className="value">{macroSnapshot['Shibor隔夜'] ?? '--'}</div>
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
                  <div className="daily-preview">
                    <div className="dp-head">
                      {Object.keys(actionPreview[0] ?? {}).map((k) => (
                        <span key={k}>{k}</span>
                      ))}
                    </div>
                    {actionPreview.map((row, i) => (
                      <div key={i} className="dp-row">
                        {Object.values(row ?? {}).map((v, j) => (
                          <span key={j}>{v != null ? String(v) : '--'}</span>
                        ))}
                      </div>
                    ))}
                    <div className="dp-footer">共 {actionPreview.length} 行 · 完整文件: {latestActionList.name}</div>
                  </div>
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
                      setScanPhase('candidates'); setScanLogs([]); setSelectionScan(null); setFusionResult(null); setBacktestRun(null); setScanProgress({ current: 0, total: 0 })
                      // Step 1: 候选池
                      const c = await fetch(`/api/selection/candidates?price_min=${selectionParams.priceMin}&price_max=${selectionParams.priceMax}`)
                      if (!c.ok) { setScanPhase(null); setError('获取候选池失败'); return }
                      const codes = (await c.json()).codes ?? []
                      setCandidateCodes(codes)
                      setScanLogs([`候选池 ${codes.length} 只，开始多策略扫描...`])
                      // Step 2: 布林扫描（策略之一）
                      setScanPhase('boll')
                      const s = await fetch('/api/selection/boll-scan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ codes, window: 20, k: 1.645, near_ratio: 1.015 }) })
                      if (!s.ok) { setScanPhase(null); setError('扫描启动失败'); return }
                      setBollTaskId((await s.json()).task_id)
                      // 布林完成后会自动触发回测；回测结束后再自动跑融合排序
                    }}>
                      {isRunning ? (
                        scanPhase === 'candidates' ? '获取候选...' :
                        scanPhase === 'boll' ? '多策略扫描中...' :
                        scanPhase === 'backtest' ? '回测运行中...' :
                        scanPhase === 'fusion' ? '融合排名中...' :
                        '运行中...'
                      ) : '开始选股'}
                    </Button>
                    <Button variant="secondary" disabled={isRunning} onClick={async () => {
                      setScanPhase('boll'); setScanLogs([]); setSelectionScan(null); setFusionResult(null); setBacktestRun(null); setCandidateCodes([]); setScanProgress({ current: 0, total: 0 })
                      const s = await fetch('/api/selection/boll-scan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) })
                      if (!s.ok) { setScanPhase(null); setError('完整布林选股启动失败'); return }
                      setBollTaskId((await s.json()).task_id)
                    }}>运行完整布林选股(多因子)</Button>
                    {(scanPhase === 'boll' || scanPhase === 'backtest') ? (
                      <Button variant="destructive" onClick={() => cancelTask(bollTaskId)}>停止</Button>
                    ) : scanPhase === 'fusion' ? (
                      <Button variant="destructive" onClick={() => cancelTask(fusionTaskId)}>停止</Button>
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
              <p>从左侧候选列表点选，右侧查看多指标技术分析与信号解读</p>
            </div>
            <div className="split">
              <aside className="split-list" aria-label="候选列表">
                {analysisList.length > 0 ? analysisList.map((item) => (
                  <button
                    key={item.code}
                    className={cn('split-item', analysisCode === item.code && 'active')}
                    onClick={() => openAnalysis(item.code)}
                  >
                    <span className="si-code">{item.code}</span>
                    <span className="si-name">{item.name}</span>
                    {item.score != null ? <span className="si-score">{Number(item.score).toFixed(1)}</span> : null}
                  </button>
                )) : (
                  <div className="empty-state" style={{ border: 'none', background: 'transparent' }}>去「选股」生成候选池</div>
                )}
              </aside>

              <div className="split-detail">
                <SectionCard title="个股分析" className="max-w-none">
                  <div className="analysis-form" style={{ marginBottom: 16 }}>
                    <input value={analysisCode} onChange={(e) => setAnalysisCode(e.target.value)}
                      placeholder="输入股票代码，例如 000001"
                      className="flex-1 h-10 rounded-lg bg-card border border-border px-3 text-foreground placeholder:text-muted-foreground"
                      onKeyDown={(e) => e.key === 'Enter' && openAnalysis(analysisCode)} />
                    <Button onClick={() => openAnalysis(analysisCode)} disabled={analysisLoading}>
                      {analysisLoading ? '请求中...' : '加载分析'}
                    </Button>
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

                        // RSI 解读
                        const rsiTxt = rsi == null ? '--'
                          : rsi > 70 ? `超买区（${rsi.toFixed(1)}），短期回调风险较高`
                          : rsi < 30 ? `超卖区（${rsi.toFixed(1)}），可能存在反弹机会`
                          : rsi > 55 ? `偏强（${rsi.toFixed(1)}），多头略占优`
                          : rsi < 45 ? `偏弱（${rsi.toFixed(1)}），空头略占优`
                          : `中性（${rsi.toFixed(1)}），多空均衡`
                        // MACD 解读
                        const macdTxt = (dif == null || dea == null) ? '--'
                          : dif > dea && macdH > 0 ? `金叉确认，DIF(${dif.toFixed(2)})>DEA(${dea.toFixed(2)})，红柱放大`
                          : dif > dea && macdH <= 0 ? `多头趋势但动能减弱，DIF>DEA 柱转绿/平`
                          : dif < dea && macdH < 0 ? `死叉确认，DIF(${dif.toFixed(2)})<DEA(${dea.toFixed(2)})，绿柱`
                          : dif < dea && macdH >= 0 ? `空头趋势但动能衰减，DIF<DEA 柱转红/平`
                          : `DIF≈DEA，方向待选择`
                        // KDJ 解读
                        const kdjTxt = (kV == null || dV == null) ? '--'
                          : jV != null && jV > 100 ? `极端超买 J=${jV.toFixed(1)}>100，注意回落`
                          : jV != null && jV < 0 ? `极端超卖 J=${jV.toFixed(1)}<0，可能反弹`
                          : kV > dV ? `K(${kV.toFixed(1)})>D(${dV.toFixed(1)}) 金叉形态，偏多`
                          : kV < dV ? `K(${kV.toFixed(1)})<D(${dV.toFixed(1)}) 死叉形态，偏空`
                          : `K≈D 震荡缠绕`
                        // 均线解读
                        const maTxt = (ma5 == null || ma10 == null || ma20 == null) ? '--'
                          : ma5 > ma10 && ma10 > ma20 && ma20 > (ma60 ?? 0) ? `完美多头排列 ↑ 短中长期均线全部向上发散`
                          : ma5 < ma10 && ma10 < ma20 && (ma20 < (ma60 ?? 999)) ? `空头排列 ↓ 均线依次向下压制`
                          : ma5 > ma20 ? `短期偏强，MA5(${ma5.toFixed(2)})在MA20(${ma20.toFixed(2)})上方`
                          : ma5 < ma20 ? `短期偏弱，MA5(${ma5.toFixed(2)})在MA20(${ma20.toFixed(2)})下方`
                          : `均线纠缠，方向不明等待突破`
                        // 价格位置解读
                        const posTxt = (distLo == null && lower == null) ? '--'
                          : close != null && lower != null && close < lower ? `已跌破下轨（${(close/lower*100-100).toFixed(2)}%），超卖信号`
                          : distLo != null && distLo < 3 ? `接近下轨（距${distLo.toFixed(2)}%），支撑位附近`
                          : distHi != null && distHi > -3 ? `接近上轨（距${Math.abs(distHi).toFixed(2)}%），压力位附近`
                          : close != null && upper != null && close > upper ? `已突破上轨（${((close/upper-1)*100).toFixed(2)}%），强势但注意回调`
                          : `位于布林带中部区间${bw != null ? `，带宽${bw.toFixed(1)}%${bw < 8 ? ' 收窄→变盘前兆' : ''}` : ''}`
                        // 带宽颜色
                        const bwWarn = bw != null && bw < 8

                        return (
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

                            {/* ② MACD */}
                            <div className="ind-card">
                              <span className="ind-label">MACD(12,26,9)</span>
                              <div className="ind-row-val">
                                <span>DIF <strong className={cn(dif != null && dif > 0 ? 'text-up' : dif != null ? 'text-down' : '')}>{dif != null ? dif.toFixed(3) : '--'}</strong></span>
                                <span>DEA <strong className={cn(dea != null && dea > 0 ? 'text-up' : dea != null ? 'text-down' : '')}>{dea != null ? dea.toFixed(3) : '--'}</strong></span>
                                <span>柱 <strong className={cn(macdH != null && macdH > 0 ? 'text-up' : macdH != null ? 'text-down' : '')}>{macdH != null ? macdH.toFixed(3) : '--'}</strong></span>
                              </div>
                              <span className="ind-txt">{macdTxt}</span>
                            </div>

                            {/* ③ KDJ */}
                            <div className="ind-card">
                              <span className="ind-label">KDJ(9,3,3)</span>
                              <div className="ind-row-val">
                                <span>K <strong>{kV != null ? kV.toFixed(1) : '--'}</strong></span>
                                <span>D <strong>{dV != null ? dV.toFixed(1) : '--'}</strong></span>
                                <span>J <strong className={cn(jV != null && jV > 100 ? 'text-up' : jV != null && jV < 0 ? 'text-down' : '')}>{jV != null ? jV.toFixed(1) : '--'}</strong></span>
                              </div>
                              <span className="ind-txt">{kdjTxt}</span>
                            </div>

                            {/* ④ 均线系统 */}
                            <div className="ind-card">
                              <span className="ind-label">均线系统</span>
                              <div className="ind-ma-row">
                                <span className="ind-ma-item">MA5 <strong>{ma5 != null ? ma5.toFixed(2) : '--'}</strong></span>
                                <span className="ind-ma-item">MA10 <strong>{ma10 != null ? ma10.toFixed(2) : '--'}</strong></span>
                                <span className="ind-ma-item">MA20 <strong>{ma20 != null ? ma20.toFixed(2) : '--'}</strong></span>
                                {ma60 != null ? <span className="ind-ma-item">MA60 <strong>{ma60.toFixed(2)}</strong></span> : null}
                              </div>
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
                        )
                      })()}
                    </>
                  ) : <div className="empty-state">从左侧列表点选，或输入代码后点击「加载分析」</div>}
                </SectionCard>
              </div>
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
              const num = (r, k) => { const v = r[k]; return v != null && v !== '' ? Number(v) : NaN }
              const total = rows.length
              const totalAmt = rows.reduce((s, r) => s + (num(r, '建议金额') || 0), 0)
              const avgScore = total ? rows.reduce((s, r) => s + (num(r, '综合评分') || 0), 0) / total : 0
              // 策略分布
              const stratMap = {}
              rows.forEach((r) => { const k = r['来源策略'] ?? '未知'; stratMap[k] = (stratMap[k] || 0) + 1 })
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
                    <div className="report-tag">共 {total} 只标的</div>
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
                          const rr = (buy && stop && tp && (buy - stop) !== 0) ? ((tp - buy) / (buy - stop)) : null
                          return (
                            <div key={r['股票代码']} className="top-row" onClick={() => { const c = String(r['股票代码'] ?? '').trim(); if (c) { setAnalysisCode(c); setActiveView('analysis'); openAnalysis(c) } }}>
                              <span className="top-no">{i + 1}</span>
                              <span className="top-code">{r['股票代码']}</span>
                              <span className="top-name">{r['股票名称']}</span>
                              <span className="top-score">{num(r, '综合评分')?.toFixed(1) ?? '--'}</span>
                              <span className="top-rr">{rr != null ? `盈亏比 ${rr.toFixed(1)}` : '—'}</span>
                            </div>
                          )
                        })}
                      </div>
                    </SectionCard>
                  </div>

                  {/* 完整明细 */}
                  <SectionCard title={`完整信号明细 · ${total} 行`} subtitle={fullDaily.latest?.path ?? ''} className="max-w-none">
                    <div className="daily-full">
                      <div className="df-head">
                        {fullDaily.columns.map((col) => (
                          <span key={col}>{col}</span>
                        ))}
                      </div>
                      {rows.map((row, i) => {
                        const code = row['股票代码'] ?? row['代码'] ?? Object.values(row)[0] ?? ''
                        return (
                          <div key={i} className="df-row" onClick={() => { const c = String(code).trim(); if (c) { setAnalysisCode(c); setActiveView('analysis'); openAnalysis(c) } }}>
                            {fullDaily.columns.map((col) => (
                              <span key={col}>{row[col] != null ? String(row[col]) : '--'}</span>
                            ))}
                          </div>
                        )
                      })}
                    </div>
                  </SectionCard>
                </>
              )
            })() : (
              <div className="empty-state">
                {fullDaily === null ? '加载中...' : '暂无日报文件，去「选股」跑完流程后这里会显示完整信号'}
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
              <p>运行手动多策略回测，或选股后自动生成权益曲线</p>
            </div>

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
                          <span>{t.code}</span>
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
