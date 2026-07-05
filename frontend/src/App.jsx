import { useEffect, useState } from 'react'

const QUICK_START = ['看概览', '跑选股', '查分析', '录交易', '看回测']

const TABS = [
  { id: 'overview', label: '概览' },
  { id: 'selection', label: '选股' },
  { id: 'analysis', label: '分析' },
  { id: 'portfolio', label: '持仓' },
  { id: 'backtest', label: '回测' },
]

function StatCard({ label, value }) {
  return (
    <div className="stat-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

function SectionCard({ title, subtitle, children, className = '' }) {
  return (
    <section className={`dashboard-card ${className}`}>
      <div className="section-head">
        <h3>{title}</h3>
        {subtitle ? <span>{subtitle}</span> : null}
      </div>
      {children}
    </section>
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
  const [selectionParams, setSelectionParams] = useState({
    priceMin: 5,
    priceMax: 30,
    window: 20,
    k: 1.645,
    nearRatio: 1.015,
  })
  const [selectionScan, setSelectionScan] = useState(null)
  const [fusionResult, setFusionResult] = useState(null)
  const [backtestRun, setBacktestRun] = useState(null)
  const [tradeForm, setTradeForm] = useState({
    date: new Date().toISOString().slice(0, 10),
    code: '',
    name: '',
    side: 'buy',
    price: 0,
    qty: 100,
    fee: 0,
    notes: '',
  })
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()

    async function loadDashboard() {
      try {
        setError('')
        const [dashboardResponse, artifactsResponse, portfolioResponse, backtestResponse] = await Promise.all([
          fetch('/api/dashboard', { signal: controller.signal }),
          fetch('/api/artifacts/daily-action-list', { signal: controller.signal }),
          fetch('/api/portfolio', { signal: controller.signal }),
          fetch('/api/backtests/latest', { signal: controller.signal }),
        ])

        if (!dashboardResponse.ok || !artifactsResponse.ok || !portfolioResponse.ok || !backtestResponse.ok) {
          throw new Error('api error')
        }

        setDashboard(await dashboardResponse.json())
        setArtifacts(await artifactsResponse.json())
        setPortfolio(await portfolioResponse.json())
        setBacktest(await backtestResponse.json())

        const candidateResponse = await fetch('/api/selection/candidates?price_min=5&price_max=30', { signal: controller.signal })
        if (candidateResponse.ok) {
          const candidateData = await candidateResponse.json()
          setCandidateCodes(candidateData.codes ?? [])
        }

        const analysisResponse = await fetch(`/api/analysis/${analysisCode}`, { signal: controller.signal })
        if (analysisResponse.ok) {
          setAnalysis(await analysisResponse.json())
        }
      } catch (err) {
        if (err.name !== 'AbortError') {
          setError('后端未启动或接口不可用')
        }
      }
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
  const backtestPreview = backtest?.preview?.rows ?? []
  const backtestSummary = backtestRun?.summary ?? null
  const analysisSignal = analysis?.signal ?? null
  const analysisLatest = analysis?.latest ?? null
  const selectionRows = selectionScan?.rows ?? []
  const fusionRows = fusionResult?.rows ?? []

  return (
    <div className="page-shell">
      <header className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Stocks-Master</p>
          <h1>一个更清楚的选股工作台</h1>
          <p className="hero-text">概览看方向，选股看信号，分析看个股，持仓看仓位，回测看结果。页面改成分页式工作区后，重点更明确，视觉也会干净很多。</p>
          <div className="quick-start">
            {QUICK_START.map((item) => (
              <span key={item}>{item}</span>
            ))}
          </div>
        </div>
        <div className="hero-panel">
          <div className="panel-title">当前状态</div>
          <div className="panel-value">{error ? '连接失败' : '已连接'}</div>
          <div className="panel-meta">{dashboard ? `更新时间 ${dashboard.generated_at}` : '等待数据加载'}</div>
          <div className="panel-stats">
            <div>
              <span>日报</span>
              <strong>{latestActionList ? '已就绪' : '空'}</strong>
            </div>
            <div>
              <span>持仓</span>
              <strong>{openPositions.length}</strong>
            </div>
            <div>
              <span>候选</span>
              <strong>{candidateCodes.length}</strong>
            </div>
          </div>
        </div>
      </header>

      <section className="section-intro">
        <h2>首页概览</h2>
        <p>页面上的空状态表示“当前还没跑出结果”，不是功能缺失。先跑选股或录入交易，下面的结果卡片会自动变成有内容。</p>
      </section>

      <nav className="workspace-tabs" aria-label="工作区导航">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            type="button"
            className={activeView === tab.id ? 'tab-pill active' : 'tab-pill'}
            onClick={() => setActiveView(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {activeView === 'overview' ? (
        <section className="dashboard-grid workspace-grid overview-grid">
          <SectionCard title="指数快照" subtitle="来自 /api/dashboard" className="wide accent-card">
            <div className="index-list">
              {indexSnapshot.length > 0 ? (
                indexSnapshot.map((item) => (
                  <div key={item.指数} className="index-row">
                    <span>{item.指数}</span>
                    <strong>{Number(item.最新价).toFixed(2)}</strong>
                    <em>{Number(item.涨跌幅).toFixed(2)}%</em>
                  </div>
                ))
              ) : (
                <div className="empty-state">暂无指数数据，可能是缓存未预热或行情接口暂时不可用</div>
              )}
            </div>
          </SectionCard>

          <SectionCard title="最新日报" className="soft-card">
            {latestActionList ? (
              <div className="artifact-box">
                <div className="artifact-name">{latestActionList.name}</div>
                <div className="artifact-path">{latestActionList.path}</div>
                <div className="artifact-count">预览行数 {actionPreview.length}</div>
              </div>
            ) : (
              <div className="empty-state">暂无日报文件，先跑一次选股或检查 stock_data 目录</div>
            )}
          </SectionCard>

          <SectionCard title="市场热度" className="soft-card">
            <div className="metric-stack">
              <StatCard label="上涨" value={marketBreadth.上涨 ?? '--'} />
              <StatCard label="下跌" value={marketBreadth.下跌 ?? '--'} />
              <StatCard label="上涨比例" value={marketBreadth.上涨比例 ?? '--'} />
            </div>
          </SectionCard>

          <SectionCard title="宏观指标" className="soft-card">
            <div className="metric-stack">
              <StatCard label="美元/人民币" value={macroSnapshot['美元/人民币'] ?? '--'} />
              <StatCard label="Shibor 隔夜" value={macroSnapshot['Shibor隔夜'] ?? '--'} />
            </div>
          </SectionCard>
        </section>
      ) : null}

      {activeView === 'selection' ? (
        <section className="dashboard-grid workspace-grid single-col">
          <SectionCard title="选股扫描" className="wide accent-card">
            <div className="selection-form">
              <input value={selectionParams.priceMin} type="number" min="1" step="1" onChange={(event) => setSelectionParams((prev) => ({ ...prev, priceMin: Number(event.target.value) }))} placeholder="最低价" />
              <input value={selectionParams.priceMax} type="number" min="1" step="1" onChange={(event) => setSelectionParams((prev) => ({ ...prev, priceMax: Number(event.target.value) }))} placeholder="最高价" />
              <input value={selectionParams.window} type="number" min="10" step="1" onChange={(event) => setSelectionParams((prev) => ({ ...prev, window: Number(event.target.value) }))} placeholder="窗口" />
              <input value={selectionParams.k} type="number" min="1" step="0.001" onChange={(event) => setSelectionParams((prev) => ({ ...prev, k: Number(event.target.value) }))} placeholder="K" />
              <input value={selectionParams.nearRatio} type="number" min="1" step="0.001" onChange={(event) => setSelectionParams((prev) => ({ ...prev, nearRatio: Number(event.target.value) }))} placeholder="接近下轨阈值" />
              <button
                type="button"
                onClick={async () => {
                  const params = new URLSearchParams({
                    price_min: String(selectionParams.priceMin),
                    price_max: String(selectionParams.priceMax),
                  })
                  const candidateResponse = await fetch(`/api/selection/candidates?${params.toString()}`)
                  if (!candidateResponse.ok) return
                  const candidateData = await candidateResponse.json()
                  const codes = candidateData.codes ?? []
                  setCandidateCodes(codes)
                  const scanResponse = await fetch('/api/selection/boll-scan', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      codes,
                      window: selectionParams.window,
                      k: selectionParams.k,
                      near_ratio: selectionParams.nearRatio,
                    }),
                  })
                  if (scanResponse.ok) setSelectionScan(await scanResponse.json())
                }}
              >
                扫描布林信号
              </button>
              <button
                type="button"
                onClick={async () => {
                  const response = await fetch('/api/selection/fusion', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ total_capital: 100000, max_picks: 15 }),
                  })
                  if (response.ok) setFusionResult(await response.json())
                }}
              >
                运行策略融合
              </button>
            </div>

            {selectionRows.length > 0 ? (
              <div className="table-shell">
                {selectionRows.slice(0, 8).map((row) => (
                  <div key={row.代码} className="table-row">
                    <span>{row.代码}</span>
                    <strong>{Number(row.最新价 ?? 0).toFixed(2)}</strong>
                    <em>{row.信号}</em>
                  </div>
                ))}
              </div>
            ) : (
              <div className="empty-state">先选参数，再点「扫描布林信号」</div>
            )}

            {fusionRows.length > 0 ? (
              <div className="table-shell spaced">
                <div className="section-head">
                  <h3>策略融合结果</h3>
                  <span>{fusionResult?.saved_path ?? '未保存'}</span>
                </div>
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

          <SectionCard title="候选池" className="soft-card">
            <div className="artifact-box">
              <div className="artifact-name">候选数量 {candidateCodes.length}</div>
              <div className="artifact-path">展示前 20 个</div>
              <div className="artifact-count">{candidateCodes.slice(0, 20).join(' · ') || '暂无候选'}</div>
            </div>
          </SectionCard>
        </section>
      ) : null}

      {activeView === 'analysis' ? (
        <section className="dashboard-grid workspace-grid single-col">
          <SectionCard title="个股分析" className="wide accent-card">
            <div className="analysis-form">
              <input value={analysisCode} onChange={(event) => setAnalysisCode(event.target.value)} placeholder="输入股票代码，例如 000001" />
              <button
                type="button"
                onClick={async () => {
                  const response = await fetch(`/api/analysis/${analysisCode}`)
                  if (response.ok) setAnalysis(await response.json())
                }}
              >
                加载分析
              </button>
            </div>
            {analysis ? (
              <div className="analysis-grid">
                <StatCard label="信号" value={analysisSignal?.signal ?? '--'} />
                <StatCard label="最新收盘" value={analysisLatest?.close ?? '--'} />
                <StatCard label="RSI" value={analysisLatest?.rsi ?? '--'} />
                <StatCard label="距下轨%" value={analysis?.metrics?.dist_to_lower_pct ?? '--'} />
              </div>
            ) : (
              <div className="empty-state">输入股票代码后点击「加载分析」</div>
            )}
          </SectionCard>
        </section>
      ) : null}

      {activeView === 'portfolio' ? (
        <section className="dashboard-grid workspace-grid single-col">
          <SectionCard title="持仓概览" className="wide accent-card">
            {openPositions.length > 0 ? (
              <div className="index-list compact">
                {openPositions.slice(0, 5).map((item) => (
                  <div key={`${item.代码}-${item.买入日期}`} className="index-row compact">
                    <span>{item.代码}</span>
                    <strong>{item.数量}</strong>
                    <em>{Number(item.成本金额).toFixed(2)}</em>
                  </div>
                ))}
              </div>
            ) : (
              <div className="empty-state">还没有持仓记录，先在下方录入一笔交易</div>
            )}
          </SectionCard>

          <SectionCard title="交易录入" className="wide">
            <div className="trade-form">
              <input value={tradeForm.date} type="date" onChange={(event) => setTradeForm((prev) => ({ ...prev, date: event.target.value }))} />
              <input value={tradeForm.code} placeholder="股票代码" onChange={(event) => setTradeForm((prev) => ({ ...prev, code: event.target.value }))} />
              <input value={tradeForm.name} placeholder="股票名称" onChange={(event) => setTradeForm((prev) => ({ ...prev, name: event.target.value }))} />
              <select value={tradeForm.side} onChange={(event) => setTradeForm((prev) => ({ ...prev, side: event.target.value }))}>
                <option value="buy">买入</option>
                <option value="sell">卖出</option>
              </select>
              <input value={tradeForm.price} type="number" min="0" step="0.01" placeholder="价格" onChange={(event) => setTradeForm((prev) => ({ ...prev, price: Number(event.target.value) }))} />
              <input value={tradeForm.qty} type="number" min="1" step="1" placeholder="数量" onChange={(event) => setTradeForm((prev) => ({ ...prev, qty: Number(event.target.value) }))} />
              <input value={tradeForm.fee} type="number" min="0" step="0.01" placeholder="手续费" onChange={(event) => setTradeForm((prev) => ({ ...prev, fee: Number(event.target.value) }))} />
              <input value={tradeForm.notes} placeholder="备注" onChange={(event) => setTradeForm((prev) => ({ ...prev, notes: event.target.value }))} />
              <button
                type="button"
                onClick={async () => {
                  const response = await fetch('/api/trades', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(tradeForm),
                  })
                  if (response.ok) {
                    const portfolioResponse = await fetch('/api/portfolio')
                    if (portfolioResponse.ok) setPortfolio(await portfolioResponse.json())
                  }
                }}
              >
                保存交易
              </button>
              <button
                type="button"
                onClick={async () => {
                  const response = await fetch('/api/trades', { method: 'DELETE' })
                  if (response.ok) {
                    const portfolioResponse = await fetch('/api/portfolio')
                    if (portfolioResponse.ok) setPortfolio(await portfolioResponse.json())
                  }
                }}
              >
                清空记录
              </button>
            </div>
            <div className="dashboard-grid compact-grid">
              <StatCard label="持仓成本" value={pnlSummary.holding_cost ?? '--'} />
              <StatCard label="当前市值" value={pnlSummary.holding_value ?? '--'} />
              <StatCard label="总浮动盈亏" value={pnlSummary.total_pnl ?? '--'} />
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
            ) : (
              <div className="empty-state">暂无实时持仓概览</div>
            )}
          </SectionCard>
        </section>
      ) : null}

      {activeView === 'backtest' ? (
        <section className="dashboard-grid workspace-grid single-col">
          <SectionCard title="最新回测" className="wide accent-card">
            {latestBacktest ? (
              <div className="artifact-box">
                <div className="artifact-name">{latestBacktest.name}</div>
                <div className="artifact-path">{latestBacktest.path}</div>
                <div className="artifact-count">预览行数 {backtestPreview.length}</div>
              </div>
            ) : (
              <div className="empty-state">暂无回测结果</div>
            )}
            <button
              type="button"
              className="inline-action"
              onClick={async () => {
                const response = await fetch('/api/backtests/run-latest', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ hold_days: 5, initial_capital: 100000, max_positions: 10, slippage: 0.001 }),
                })
                if (response.ok) setBacktestRun(await response.json())
              }}
            >
              运行最新回测
            </button>
            {backtestSummary ? (
              <div className="metric-stack top-gap">
                <StatCard label="总收益率" value={backtestSummary.total_return ?? '--'} />
                <StatCard label="最大回撤" value={backtestSummary.max_drawdown ?? '--'} />
                <StatCard label="胜率" value={backtestSummary.win_rate ?? '--'} />
                <StatCard label="交易笔数" value={backtestSummary.num_trades ?? '--'} />
              </div>
            ) : null}
          </SectionCard>
        </section>
      ) : null}

      {error ? <footer className="footer warning">{error}</footer> : null}
    </div>
  )
}

export default App