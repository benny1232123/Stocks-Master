import { useEffect, useRef, useState } from 'react'
import {
  LayoutDashboard,
  Filter,
  LineChart,
  Briefcase,
  FlaskConical,
  ArrowUpRight,
  ArrowDownRight,
} from 'lucide-react'
import { Button } from './components/ui/button'
import { cn } from './lib/utils'

const TABS = [
  { id: 'overview', label: '概览', icon: LayoutDashboard },
  { id: 'selection', label: '选股', icon: Filter },
  { id: 'analysis', label: '分析', icon: LineChart },
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
  const logContainerRef = useRef(null)

  // Phase: null | 'candidates' | 'boll' | 'backtest' | 'fusion'
  const [scanPhase, setScanPhase] = useState(null)
  const [bollTaskId, setBollTaskId] = useState(null)
  const [fusionTaskId, setFusionTaskId] = useState(null)
  const [btTaskId, setBtTaskId] = useState(null)
  const [scanProgress, setScanProgress] = useState({ current: 0, total: 0 })

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

  // 从列表/榜单点选股票 → 拉取分析并切换详情
  async function openAnalysis(code) {
    if (!code) return
    setAnalysisCode(code)
    try {
      const r = await fetch(`/api/analysis/${code}`)
      if (r.ok) setAnalysis(await r.json())
    } catch { /* 离线时保持空，由 error chip 提示 */ }
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
          setError('布林扫描轮询失败')
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

              <SectionCard title="最新日报" subtitle={latestActionList?.path ?? ''}>
                {latestActionList ? (
                  <div className="artifact-box">
                    <div className="artifact-name">{latestActionList.name}</div>
                    <div className="artifact-path">{latestActionList.path}</div>
                    <div className="artifact-count">预览行数 {actionPreview.length}</div>
                  </div>
                ) : <div className="empty-state">暂无日报文件</div>}
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
              <p>设置价格区间，点击按钮生成候选池并执行布林扫描。扫描完成后自动回测。</p>
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
                      const c = await fetch(`/api/selection/candidates?price_min=${selectionParams.priceMin}&price_max=${selectionParams.priceMax}`)
                      if (!c.ok) { setScanPhase(null); setError('获取候选池失败'); return }
                      const codes = (await c.json()).codes ?? []
                      setCandidateCodes(codes)
                      setScanLogs([`候选池 ${codes.length} 只，开始布林扫描...`])
                      setScanPhase('boll')
                      const s = await fetch('/api/selection/boll-scan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ codes, window: 20, k: 1.645, near_ratio: 1.015 }) })
                      if (!s.ok) { setScanPhase(null); setError('布林扫描启动失败'); return }
                      setBollTaskId((await s.json()).task_id)
                    }}>
                      {scanPhase === 'boll' || scanPhase === 'backtest' ? '扫描进行中...' : '开始扫描 + 回测'}
                    </Button>
                    {scanPhase === 'boll' ? (
                      <Button variant="destructive" onClick={() => cancelTask(bollTaskId)}>停止</Button>
                    ) : null}
                  </div>

                  <div className="button-row">
                    <Button variant="secondary" disabled={isRunning} onClick={async () => {
                      setScanPhase('fusion'); setScanLogs(['开始策略融合排序...']); setSelectionScan(null); setFusionResult(null); setBacktestRun(null); setScanProgress({ current: 0, total: 0 })
                      const r = await fetch('/api/selection/fusion', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ total_capital: 100000, max_picks: 15 }) })
                      if (!r.ok) { setScanPhase(null); setError('融合排序启动失败'); return }
                      setFusionTaskId((await r.json()).task_id)
                    }}>
                      {scanPhase === 'fusion' ? '融合进行中...' : '仅运行融合排序'}
                    </Button>
                    {scanPhase === 'fusion' ? (
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
                      {scanPhase === 'boll' ? '布林扫描中' :
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
                    <div className="section-head"><h3>布林扫描结果</h3><span>命中 {selectionRows.length} 只</span></div>
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
              <p>从左侧候选列表点选，右侧查看布林带信号与技术指标（雪球式主从布局）</p>
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
                      className="flex-1 h-10 rounded-lg bg-card border border-border px-3 text-foreground placeholder:text-muted-foreground" />
                    <Button onClick={() => openAnalysis(analysisCode)}>加载分析</Button>
                  </div>
                  {analysis ? (
                    <>
                      <div className="detail-head">
                        <div className="detail-title">
                          <span className="detail-code">{analysisCode}</span>
                          <span className="detail-name">{analysis?.latest?.name ?? analysisList.find((i) => i.code === analysisCode)?.name ?? ''}</span>
                        </div>
                        <span className={cn('signal-badge', sigClass)} style={{ whiteSpace: 'nowrap' }}>{analysisSignal?.signal ?? '暂无信号'}</span>
                      </div>
                      <div className="analysis-grid">
                        <StatCard label="信号" value={analysisSignal?.signal ?? '--'} />
                        <StatCard label="最新收盘" value={analysisLatest?.close ?? '--'} />
                        <StatCard label="RSI" value={analysisLatest?.rsi ?? '--'} />
                        <StatCard label="距下轨%" value={analysis?.metrics?.dist_to_lower_pct ?? '--'} />
                      </div>
                    </>
                  ) : <div className="empty-state">从左侧列表点选，或输入代码后点击「加载分析」</div>}
                </SectionCard>
              </div>
            </div>
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
              <p>选股完成后自动生成权益曲线</p>
            </div>
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
                      <span className="bt-step-text">点击「开始扫描 + 回测」</span>
                    </div>
                    <div className="bt-step-arrow">→</div>
                    <div className="bt-step">
                      <span className="bt-step-num">3</span>
                      <span className="bt-step-text">等待布林扫描 + 自动回测完成</span>
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
