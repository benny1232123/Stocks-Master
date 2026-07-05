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
      <span>{label}</span>
      <strong>{value}</strong>
      {trend != null ? (
        <span className={cn('text-xs font-medium', trend >= 0 ? 'text-primary' : 'text-destructive')}>
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
  const curveColor = lastVal >= initialCapital ? 'hsl(160, 60%, 65%)' : 'hsl(0, 70%, 60%)'
  const ddValues = equity.map((d) => d.drawdown ?? 0)
  const ddMin = Math.min(...ddValues)
  const ddH = 50
  const ddYScale = (v) => (Math.abs(v) / Math.abs(ddMin || 1)) * ddH

  return (
    <div className="equity-chart-wrap">
      <svg viewBox={`0 0 ${W} ${H}`} className="equity-svg">
        {yTicks.map((t, i) => (
          <g key={i}>
            <line x1={PAD.left} y1={t.y} x2={W - PAD.right} y2={t.y} stroke="hsl(215, 30%, 15%)" />
            <text x={PAD.left - 8} y={t.y + 4} textAnchor="end" fill="hsl(215, 25%, 35%)" fontSize="10">
              {t.label}
            </text>
          </g>
        ))}

        <line x1={PAD.left} y1={baselineY} x2={W - PAD.right} y2={baselineY} stroke="hsl(38, 75%, 55%)" strokeDasharray="4 3" opacity="0.4" />
        <text x={W - PAD.right + 4} y={baselineY + 4} fill="hsl(38, 75%, 55%)" opacity="0.5" fontSize="9">
          初始
        </text>

        <path d={linePath} fill="none" stroke={curveColor} strokeWidth="2" strokeLinejoin="round" />
        <circle cx={xScale(values.length - 1)} cy={yScale(lastVal)} r="4" fill={curveColor} />

        {xTicks.map((d, i) => {
          const idx = equity.indexOf(d)
          return (
            <text key={i} x={xScale(idx)} y={H - 8} textAnchor="middle" fill="hsl(215, 25%, 35%)" fontSize="9">
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
              return <rect key={i} x={xScale(i) - 2} y={0} width="4" height={ddYScale(v)} fill="hsl(0, 70%, 60%)" opacity="0.35" rx="1" />
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

  return (
    <div className="flex min-h-screen">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <h1>Stocks Master</h1>
          <p>策略选股工作台</p>
        </div>
        <nav className="sidebar-nav">
          {TABS.map((tab) => {
            const Icon = tab.icon
            return (
              <button
                key={tab.id}
                className={cn('sidebar-item', activeView === tab.id && 'active')}
                onClick={() => setActiveView(tab.id)}
              >
                <Icon />
                <span>{tab.label}</span>
              </button>
            )
          })}
        </nav>
        <div className="sidebar-footer">
          {error ? (
            <span className="text-destructive">连接失败</span>
          ) : (
            <span className="text-primary">已连接</span>
          )}
        </div>
      </aside>

      <main className="main-content">
        {activeView === 'overview' ? (
          <>
            <div className="page-header">
              <h2>首页概览</h2>
              <p>页面上的空状态表示"当前还没跑出结果"，不是功能缺失。</p>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <SectionCard title="指数快照" subtitle="来自 /api/dashboard" className="glass-card-accent">
                <div className="index-list">
                  {indexSnapshot.length > 0 ? indexSnapshot.map((item) => (
                    <div key={item.指数} className="index-row">
                      <span>{item.指数}</span>
                      <strong>{Number(item.最新价).toFixed(2)}</strong>
                      <em className={cn(Number(item.涨跌幅) >= 0 ? 'text-primary' : 'text-destructive')}>
                        {Number(item.涨跌幅) >= 0 ? '+' : ''}{Number(item.涨跌幅).toFixed(2)}%
                      </em>
                    </div>
                  )) : (
                    <div className="empty-state">暂无指数数据</div>
                  )}
                </div>
              </SectionCard>

              <div className="grid grid-cols-1 gap-4">
                <SectionCard title="最新日报" className="glass-card">
                  {latestActionList ? (
                    <div className="artifact-box">
                      <div className="artifact-name">{latestActionList.name}</div>
                      <div className="artifact-path">{latestActionList.path}</div>
                      <div className="artifact-count">预览行数 {actionPreview.length}</div>
                    </div>
                  ) : <div className="empty-state">暂无日报文件</div>}
                </SectionCard>
                <div className="grid grid-cols-2 gap-4">
                  <SectionCard title="市场热度" className="glass-card">
                    <div className="metric-stack">
                      <StatCard label="上涨" value={marketBreadth.上涨 ?? '--'} />
                      <StatCard label="下跌" value={marketBreadth.下跌 ?? '--'} />
                      <StatCard label="上涨比例" value={marketBreadth.上涨比例 ?? '--'} />
                    </div>
                  </SectionCard>
                  <SectionCard title="宏观指标" className="glass-card">
                    <div className="metric-stack">
                      <StatCard label="美元/人民币" value={macroSnapshot['美元/人民币'] ?? '--'} />
                      <StatCard label="Shibor 隔夜" value={macroSnapshot['Shibor隔夜'] ?? '--'} />
                    </div>
                  </SectionCard>
                </div>
              </div>
            </div>
          </>
        ) : null}

        {activeView === 'selection' ? (
          <>
            <div className="page-header">
              <h2>策略融合选股</h2>
              <p>设置价格区间，点击按钮生成候选池并执行布林扫描。扫描完成后自动回测。</p>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <SectionCard title="选股参数" className="glass-card-accent lg:col-span-2">
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

              <SectionCard title="候选池" className="glass-card">
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
              <p>输入股票代码查看布林带信号和技术指标</p>
            </div>
            <SectionCard title="个股分析" className="glass-card-accent max-w-3xl">
              <div className="analysis-form">
                <input value={analysisCode} onChange={(e) => setAnalysisCode(e.target.value)}
                  placeholder="输入股票代码，例如 000001"
                  className="flex-1 h-10 rounded-lg bg-card border border-border px-3 text-foreground placeholder:text-muted-foreground" />
                <Button onClick={async () => {
                  const r = await fetch(`/api/analysis/${analysisCode}`)
                  if (r.ok) setAnalysis(await r.json())
                }}>加载分析</Button>
              </div>
              {analysis ? (
                <div className="analysis-grid">
                  <StatCard label="信号" value={analysisSignal?.signal ?? '--'} />
                  <StatCard label="最新收盘" value={analysisLatest?.close ?? '--'} />
                  <StatCard label="RSI" value={analysisLatest?.rsi ?? '--'} />
                  <StatCard label="距下轨%" value={analysis?.metrics?.dist_to_lower_pct ?? '--'} />
                </div>
              ) : <div className="empty-state">输入代码后点击「加载分析」</div>}
            </SectionCard>
          </>
        ) : null}

        {activeView === 'portfolio' ? (
          <>
            <div className="page-header">
              <h2>持仓管理</h2>
              <p>查看持仓、录入交易</p>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <SectionCard title="持仓概览" className="glass-card-accent">
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

              <SectionCard title="交易录入" className="glass-card">
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
            <SectionCard title="回测" className="glass-card-accent">
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
                          <em className={cn(t.return_pct >= 0 ? 'text-primary' : 'text-destructive')}>
                            {t.return_pct >= 0 ? '+' : ''}{t.return_pct}%
                          </em>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </>
              ) : (
                <div className="empty-state">
                  {scanPhase === 'backtest' ? '正在回测中...' : '在「选股」页点击开始后，回测将自动运行并显示在此处'}
                </div>
              )}
            </SectionCard>
          </>
        ) : null}
      </main>

      {error ? (
        <div className="fixed bottom-4 right-4 px-4 py-2 rounded-lg bg-destructive/15 text-destructive text-sm border border-destructive/30 flex items-center gap-2">
          <span>{error}</span>
          <button onClick={() => setError('')} className="ml-2 text-destructive/60 hover:text-destructive text-xs">&times;</button>
        </div>
      ) : null}
    </div>
  )
}

export default App
