import { useEffect, useState } from 'react'

const QUICK_START = [
  '1. 先看看板和最新日报',
  '2. 再跑选股扫描或策略融合',
  '3. 用代码进入个股分析',
  '4. 录入交易后看持仓与回测',
]

function StatCard({ label, value }) {
  return (
    <div className="stat-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

function App() {
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
        if (!dashboardResponse.ok) {
          throw new Error(`HTTP ${dashboardResponse.status}`)
        }
        if (!artifactsResponse.ok) {
          throw new Error(`HTTP ${artifactsResponse.status}`)
        }
        if (!portfolioResponse.ok) {
          throw new Error(`HTTP ${portfolioResponse.status}`)
        }
        if (!backtestResponse.ok) {
          throw new Error(`HTTP ${backtestResponse.status}`)
        }
        const dashboardData = await dashboardResponse.json()
        const artifactsData = await artifactsResponse.json()
        const portfolioData = await portfolioResponse.json()
        const backtestData = await backtestResponse.json()
        setDashboard(dashboardData)
        setArtifacts(artifactsData)
        setPortfolio(portfolioData)
        setBacktest(backtestData)

        const candidateResponse = await fetch('/api/selection/candidates?price_min=5&price_max=30', { signal: controller.signal })
        if (candidateResponse.ok) {
          const candidateData = await candidateResponse.json()
          setCandidateCodes(candidateData.codes ?? [])
        }

        const analysisResponse = await fetch(`/api/analysis/${analysisCode}`, { signal: controller.signal })
        if (analysisResponse.ok) {
          const analysisData = await analysisResponse.json()
          setAnalysis(analysisData)
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
  const latestBacktest = backtest?.latest ?? null
  const backtestPreview = backtest?.preview?.rows ?? []
  const backtestSummary = backtestRun?.summary ?? null
  const analysisSignal = analysis?.signal ?? null
  const analysisLatest = analysis?.latest ?? null
  const selectionRows = selectionScan?.rows ?? []
  const fusionRows = fusionResult?.rows ?? []
  const realtimePositions = portfolio?.realtime_positions ?? []
  const pnlSummary = portfolio?.pnl_summary ?? {}

  return (
    <div className="page-shell">
      <header className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Stocks-Master</p>
          <h1>选股、分析、持仓、回测，放在一页里</h1>
          <p className="hero-text">这是一个单服务 React + FastAPI 工作台。左边看结果，右边做操作；先用看板和最新日报定位方向，再进入选股、分析、持仓和回测。</p>
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
        </div>
      </header>

      <section className="section-intro">
        <h2>首页概览</h2>
        <p>页面上的空状态表示“当前还没跑出结果”，不是功能缺失。先跑选股或录入交易，下面的结果卡片会自动变成有内容。</p>
      </section>

      <section className="dashboard-grid">
        <div className="dashboard-card wide">
          <div className="section-head">
            <h3>指数快照</h3>
            <span>来自 /api/dashboard</span>
          </div>
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
        </div>

        <div className="dashboard-card">
          <div className="section-head">
            <h3>最新日报</h3>
          </div>
          {latestActionList ? (
            <div className="artifact-box">
              <div className="artifact-name">{latestActionList.name}</div>
              <div className="artifact-path">{latestActionList.path}</div>
              <div className="artifact-count">预览行数 {actionPreview.length}</div>
            </div>
          ) : (
            <div className="empty-state">暂无日报文件，先跑一次选股或检查 stock_data 目录</div>
          )}
        </div>

        <div className="dashboard-card">
          <div className="section-head">
            <h3>市场热度</h3>
          </div>
          <div className="metric-stack">
            <StatCard label="上涨" value={marketBreadth.上涨 ?? '--'} />
            <StatCard label="下跌" value={marketBreadth.下跌 ?? '--'} />
            <StatCard label="上涨比例" value={marketBreadth.上涨比例 ?? '--'} />
          </div>
        </div>

        <div className="dashboard-card">
          <div className="section-head">
            <h3>持仓概览</h3>
          </div>
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
            <div className="empty-state">还没有持仓记录，先在左侧录入一笔交易</div>
          )}
        </div>

        <div className="dashboard-card wide">
          <div className="section-head">
            <h3>交易录入</h3>
          </div>
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
                  if (portfolioResponse.ok) {
                    setPortfolio(await portfolioResponse.json())
                  }
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
                  if (portfolioResponse.ok) {
                    setPortfolio(await portfolioResponse.json())
                  }
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
        </div>

        <div className="dashboard-card">
          <div className="section-head">
            <h3>最新回测</h3>
          </div>
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
              if (response.ok) {
                setBacktestRun(await response.json())
              }
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
        </div>

        <div className="dashboard-card wide">
          <div className="section-head">
            <h3>个股分析</h3>
          </div>
          <div className="analysis-form">
            <input
              value={analysisCode}
              onChange={(event) => setAnalysisCode(event.target.value)}
              placeholder="输入股票代码，例如 000001"
            />
            <button
              type="button"
              onClick={async () => {
                const response = await fetch(`/api/analysis/${analysisCode}`)
                if (response.ok) {
                  setAnalysis(await response.json())
                }
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
        </div>

        <div className="dashboard-card wide">
          <div className="section-head">
            <h3>选股扫描</h3>
          </div>
          <div className="selection-form">
            <input
              value={selectionParams.priceMin}
              type="number"
              min="1"
              step="1"
              onChange={(event) => setSelectionParams((prev) => ({ ...prev, priceMin: Number(event.target.value) }))}
              placeholder="最低价"
            />
            <input
              value={selectionParams.priceMax}
              type="number"
              min="1"
              step="1"
              onChange={(event) => setSelectionParams((prev) => ({ ...prev, priceMax: Number(event.target.value) }))}
              placeholder="最高价"
            />
            <input
              value={selectionParams.window}
              type="number"
              min="10"
              step="1"
              onChange={(event) => setSelectionParams((prev) => ({ ...prev, window: Number(event.target.value) }))}
              placeholder="窗口"
            />
            <input
              value={selectionParams.k}
              type="number"
              min="1"
              step="0.001"
              onChange={(event) => setSelectionParams((prev) => ({ ...prev, k: Number(event.target.value) }))}
              placeholder="K"
            />
            <input
              value={selectionParams.nearRatio}
              type="number"
              min="1"
              step="0.001"
              onChange={(event) => setSelectionParams((prev) => ({ ...prev, nearRatio: Number(event.target.value) }))}
              placeholder="接近下轨阈值"
            />
            <button
              type="button"
              onClick={async () => {
                const params = new URLSearchParams({
                  price_min: String(selectionParams.priceMin),
                  price_max: String(selectionParams.priceMax),
                })
                const candidateResponse = await fetch(`/api/selection/candidates?${params.toString()}`)
                if (!candidateResponse.ok) {
                  return
                }
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
                if (scanResponse.ok) {
                  setSelectionScan(await scanResponse.json())
                }
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
                if (response.ok) {
                  setFusionResult(await response.json())
                }
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
        </div>

        <div className="dashboard-card">
          <div className="section-head">
            <h3>候选池</h3>
          </div>
          <div className="artifact-box">
            <div className="artifact-name">候选数量 {candidateCodes.length}</div>
            <div className="artifact-path">展示前 20 个</div>
            <div className="artifact-count">{candidateCodes.slice(0, 20).join(' · ') || '暂无候选'}</div>
          </div>
        </div>

        <div className="dashboard-card">
          <div className="section-head">
            <h3>宏观指标</h3>
          </div>
          <div className="metric-stack">
            <StatCard label="美元/人民币" value={macroSnapshot['美元/人民币'] ?? '--'} />
            <StatCard label="Shibor 隔夜" value={macroSnapshot['Shibor隔夜'] ?? '--'} />
          </div>
        </div>
      </section>

      {error ? <footer className="footer warning">{error}</footer> : null}
    </div>
  )
}

export default App