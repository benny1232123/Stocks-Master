import React, { useState } from 'react'

export default function RealCapital10kSection({ data, capitalBase = 10000, activeStrategyName = '' }) {
  const [selectedTf, setSelectedTf] = useState('year')
  const [hoverIndex, setHoverIndex] = useState(null)

  if (!data) return null

  const baseCap = Number(capitalBase) > 0 ? Number(capitalBase) : 10000
  const scale = baseCap / 10000

  // Get active timeframe simulation data (scaled proportionally to user's actual holding capital base)
  const rawTf = (data.timeframes && data.timeframes[selectedTf]) ? data.timeframes[selectedTf] : (data || {})
  const rawCurve = rawTf.curve || []
  const curve = rawCurve.map((c) => ({
    ...c,
    total_asset: +(c.total_asset * scale).toFixed(2),
    profit_rmb: +(((c.profit_rmb ?? (c.total_asset - 10000)) * scale)).toFixed(2)
  }))

  const tfData = {
    ...rawTf,
    initial_capital: +((rawTf.initial_capital || 10000) * scale).toFixed(2),
    final_total_asset: +((rawTf.final_total_asset || 10000) * scale).toFixed(2),
    total_profit_rmb: +((rawTf.total_profit_rmb || 0) * scale).toFixed(2),
    cash_balance: +((rawTf.cash_balance || 0) * scale).toFixed(2),
    stock_market_value: +((rawTf.stock_market_value || 0) * scale).toFixed(2),
    total_commission_paid: +((rawTf.total_commission_paid || 0) * scale).toFixed(2),
    total_stamp_duty_paid: +((rawTf.total_stamp_duty_paid || 0) * scale).toFixed(2),
    current_positions: (rawTf.current_positions || []).map((p) => ({
      ...p,
      market_value: +(p.market_value * scale).toFixed(2),
      pnl_rmb: +(p.pnl_rmb * scale).toFixed(2)
    }))
  }

  // Compute SVG chart coordinates
  const svgWidth = 840
  const svgHeight = 220
  const padding = { top: 20, right: 30, bottom: 30, left: 68 }
  const plotW = svgWidth - padding.left - padding.right
  const plotH = svgHeight - padding.top - padding.bottom

  const minAsset = curve.length > 0 ? Math.min(...curve.map(c => c.total_asset)) : baseCap * 0.98
  const maxAsset = curve.length > 0 ? Math.max(...curve.map(c => c.total_asset)) : baseCap * 1.08
  const assetSpan = Math.max(maxAsset - minAsset, 100 * scale)
  const stepUnit = Math.max(50, Math.round((assetSpan * 0.08) / 50) * 50)
  const yMin = Math.floor((minAsset - assetSpan * 0.08) / stepUnit) * stepUnit
  const yMax = Math.ceil((maxAsset + assetSpan * 0.08) / stepUnit) * stepUnit

  const getX = (idx) => padding.left + (idx / Math.max(curve.length - 1, 1)) * plotW
  const getY = (val) => padding.top + plotH - ((val - yMin) / (yMax - yMin)) * plotH

  // Build SVG path for total asset
  const assetPath = curve.length > 0
    ? curve.map((c, i) => `${i === 0 ? 'M' : 'L'} ${getX(i).toFixed(1)} ${getY(c.total_asset).toFixed(1)}`).join(' ')
    : ''

  // Fill area under curve
  const areaPath = curve.length > 0
    ? `${assetPath} L ${getX(curve.length - 1).toFixed(1)} ${(padding.top + plotH).toFixed(1)} L ${getX(0).toFixed(1)} ${(padding.top + plotH).toFixed(1)} Z`
    : ''

  const tfLabels = [
    { key: 'day', label: '日 (近10日独立建仓)' },
    { key: 'week', label: '周 (近5日独立建仓)' },
    { key: 'month', label: '月 (近1月独立建仓)' },
    { key: 'quarter', label: '季 (近1季独立建仓)' },
    { key: 'year', label: '年 (近1年独立建仓)' },
  ]

  const activeHover = hoverIndex !== null && curve[hoverIndex] ? curve[hoverIndex] : null
  const formattedBase = baseCap.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  const roundedBase = Math.round(baseCap).toLocaleString()

  return (
    <div style={{
      background: 'hsl(var(--card))',
      border: '1px solid hsla(var(--border)/0.8)',
      borderRadius: 12,
      padding: '22px 24px',
      marginBottom: 24,
      boxShadow: '0 4px 20px -4px rgba(0,0,0,0.06)'
    }}>
      {/* 头部标题与定位 */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 12, marginBottom: 18 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
            <span style={{ fontSize: '1.25rem', fontWeight: 800, color: 'hsl(var(--foreground))' }}>
              💰 ¥{roundedBase} 实盘账户真实金额模拟
            </span>
            {activeStrategyName && (
              <span style={{
                background: 'rgba(37, 99, 235, 0.1)',
                color: '#2563eb',
                border: '1px solid rgba(37, 99, 235, 0.3)',
                padding: '2px 8px',
                borderRadius: 4,
                fontSize: '0.74rem',
                fontWeight: 600
              }}>
                当前联动：{activeStrategyName}
              </span>
            )}
            <span style={{
              background: 'rgba(16, 185, 129, 0.1)',
              color: '#059669',
              border: '1px solid rgba(16, 185, 129, 0.3)',
              padding: '2px 8px',
              borderRadius: 4,
              fontSize: '0.74rem',
              fontWeight: 600
            }}>
              联动实盘持仓本金 · 实扣规费
            </span>
          </div>
          <p style={{ margin: '4px 0 0', fontSize: '0.82rem', color: 'hsl(var(--muted))' }}>
            每个周期均以您当前实盘持仓本金 <strong>¥{formattedBase}</strong> 起算（已联动交易管理持仓本金），模拟整手(100股)交易、最低5元佣金与千1印花税、空仓逆回购计息
          </p>
        </div>

        {/* 周期切换按钮 */}
        <div style={{
          display: 'inline-flex',
          background: 'hsla(var(--muted)/0.1)',
          padding: 3,
          borderRadius: 8,
          border: '1px solid hsla(var(--border)/0.6)'
        }}>
          {tfLabels.map(({ key, label }) => {
            const active = selectedTf === key
            return (
              <button
                key={key}
                type="button"
                onClick={() => { setSelectedTf(key); setHoverIndex(null); }}
                style={{
                  border: 'none',
                  background: active ? '#10b981' : 'transparent',
                  color: active ? '#fff' : 'hsl(var(--muted))',
                  fontWeight: active ? 600 : 400,
                  fontSize: '0.76rem',
                  padding: '5px 11px',
                  borderRadius: 6,
                  cursor: 'pointer',
                  transition: 'all 0.15s ease'
                }}
              >
                {label}
              </button>
            )
          })}
        </div>
      </div>

      {/* 核心指标统计卡片网格 (根据所选周期动态切换) */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))',
        gap: 12,
        marginBottom: 20
      }}>
        <div style={{ background: 'hsla(var(--muted)/0.05)', padding: '12px 14px', borderRadius: 8, border: '1px solid hsla(var(--border)/0.5)' }}>
          <div style={{ fontSize: '0.72rem', color: 'hsl(var(--muted))', marginBottom: 4 }}>周期期初本金</div>
          <div style={{ fontSize: '1.05rem', fontWeight: 700, color: 'hsl(var(--foreground))' }}>¥{(tfData.initial_capital || 10000).toLocaleString()}</div>
          <div style={{ fontSize: '0.7rem', color: 'hsl(var(--muted))' }}>起算日期: {tfData.start}</div>
        </div>

        <div style={{ background: 'rgba(16, 185, 129, 0.05)', padding: '12px 14px', borderRadius: 8, border: '1px solid rgba(16, 185, 129, 0.25)' }}>
          <div style={{ fontSize: '0.72rem', color: '#059669', marginBottom: 4 }}>当前总资产</div>
          <div style={{ fontSize: '1.15rem', fontWeight: 800, color: '#059669' }}>¥{(tfData.final_total_asset || 10000).toLocaleString()}</div>
          <div style={{ fontSize: '0.7rem', color: '#059669' }}>
            净利润 {tfData.total_profit_rmb >= 0 ? `+¥${tfData.total_profit_rmb.toFixed(2)}` : `-¥${Math.abs(tfData.total_profit_rmb).toFixed(2)}`} ({tfData.total_return_pct >= 0 ? `+${tfData.total_return_pct}%` : `${tfData.total_return_pct}%`})
          </div>
        </div>

        <div style={{ background: 'hsla(var(--muted)/0.05)', padding: '12px 14px', borderRadius: 8, border: '1px solid hsla(var(--border)/0.5)' }}>
          <div style={{ fontSize: '0.72rem', color: 'hsl(var(--muted))', marginBottom: 4 }}>现金结余 (GC001)</div>
          <div style={{ fontSize: '1.05rem', fontWeight: 700, color: '#2563eb' }}>¥{(tfData.cash_balance || 0).toLocaleString()}</div>
          <div style={{ fontSize: '0.7rem', color: 'hsl(var(--muted))' }}>
            占比 {(((tfData.cash_balance || 0) / (tfData.final_total_asset || 10000)) * 100).toFixed(1)}% · 2.5%年化
          </div>
        </div>

        <div style={{ background: 'hsla(var(--muted)/0.05)', padding: '12px 14px', borderRadius: 8, border: '1px solid hsla(var(--border)/0.5)' }}>
          <div style={{ fontSize: '0.72rem', color: 'hsl(var(--muted))', marginBottom: 4 }}>股票持仓市值</div>
          <div style={{ fontSize: '1.05rem', fontWeight: 700, color: '#7c3aed' }}>¥{(tfData.stock_market_value || 0).toLocaleString()}</div>
          <div style={{ fontSize: '0.7rem', color: 'hsl(var(--muted))' }}>
            占比 {(((tfData.stock_market_value || 0) / (tfData.final_total_asset || 10000)) * 100).toFixed(1)}% ({tfData.current_positions ? tfData.current_positions.length : 0}只)
          </div>
        </div>

        <div style={{ background: 'hsla(var(--muted)/0.05)', padding: '12px 14px', borderRadius: 8, border: '1px solid hsla(var(--border)/0.5)' }}>
          <div style={{ fontSize: '0.72rem', color: 'hsl(var(--muted))', marginBottom: 4 }}>该周期实扣规费</div>
          <div style={{ fontSize: '1.05rem', fontWeight: 700, color: '#ef4444' }}>
            ¥{((tfData.total_commission_paid || 0) + (tfData.total_stamp_duty_paid || 0)).toFixed(2)}
          </div>
          <div style={{ fontSize: '0.7rem', color: 'hsl(var(--muted))' }}>
            佣金¥{(tfData.total_commission_paid || 0).toFixed(1)} | 印花税¥{(tfData.total_stamp_duty_paid || 0).toFixed(1)}
          </div>
        </div>

        <div style={{ background: 'hsla(var(--muted)/0.05)', padding: '12px 14px', borderRadius: 8, border: '1px solid hsla(var(--border)/0.5)' }}>
          <div style={{ fontSize: '0.72rem', color: 'hsl(var(--muted))', marginBottom: 4 }}>交易胜率 / 笔数</div>
          <div style={{ fontSize: '1.05rem', fontWeight: 700, color: 'hsl(var(--foreground))' }}>{tfData.win_rate || 100}%</div>
          <div style={{ fontSize: '0.7rem', color: 'hsl(var(--muted))' }}>
            平仓{tfData.completed_sells_count || 0}笔 (共{tfData.total_trades_count || 0}次)
          </div>
        </div>
      </div>

      {/* 真实资产走势图 (SVG) */}
      <div style={{
        background: 'hsla(var(--muted)/0.03)',
        borderRadius: 8,
        border: '1px solid hsla(var(--border)/0.5)',
        padding: '16px',
        marginBottom: 20
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8, flexWrap: 'wrap', gap: 8 }}>
          <div style={{ fontSize: '0.84rem', fontWeight: 600, color: 'hsl(var(--foreground))' }}>
            📈 账户真实总资产走势 (单位: 人民币元 · 期初 ¥{roundedBase} 起步)
          </div>
          <div style={{ fontSize: '0.78rem', color: 'hsl(var(--muted))', display: 'flex', gap: 12 }}>
            <span>起止区间: <strong>{tfData.start} ~ {tfData.end}</strong></span>
            <span>周期收益: <strong style={{ color: tfData.total_profit_rmb >= 0 ? '#10b981' : '#ef4444' }}>
              {tfData.total_profit_rmb >= 0 ? `+¥${tfData.total_profit_rmb.toFixed(2)}` : `-¥${Math.abs(tfData.total_profit_rmb).toFixed(2)}`} ({tfData.total_return_pct >= 0 ? `+${tfData.total_return_pct}%` : `${tfData.total_return_pct}%`})
            </strong></span>
            <span>同期沪深300: <strong>{tfData.benchmark_return_pct >= 0 ? `+${tfData.benchmark_return_pct}%` : `${tfData.benchmark_return_pct}%`}</strong></span>
            <span>超额Alpha: <strong style={{ color: '#059669' }}>+{tfData.excess_return_pct}%</strong></span>
          </div>
        </div>

        {/* 悬停信息条 */}
        <div style={{
          minHeight: 24,
          fontSize: '0.76rem',
          color: activeHover ? '#10b981' : 'transparent',
          fontWeight: 600,
          marginBottom: 6
        }}>
          {activeHover && (
            <span>
              📅 {activeHover.date} · 总资产: ¥{activeHover.total_asset.toLocaleString()} · 阶段收益: {activeHover.strategy_return >= 0 ? `+${activeHover.strategy_return}%` : `${activeHover.strategy_return}%`} ({activeHover.profit_rmb >= 0 ? `+¥${activeHover.profit_rmb.toFixed(2)}` : `-¥${Math.abs(activeHover.profit_rmb).toFixed(2)}`}) · 同期基准: {activeHover.benchmark_return >= 0 ? `+${activeHover.benchmark_return}%` : `${activeHover.benchmark_return}%`}
            </span>
          )}
        </div>

        {/* SVG 图表 */}
        <div style={{ position: 'relative', width: '100%', overflowX: 'auto' }}>
          <svg
            viewBox={`0 0 ${svgWidth} ${svgHeight}`}
            style={{ width: '100%', height: 'auto', display: 'block' }}
            onMouseLeave={() => setHoverIndex(null)}
          >
            <defs>
              <linearGradient id="capitalGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#10b981" stopOpacity="0.25" />
                <stop offset="100%" stopColor="#10b981" stopOpacity="0.0" />
              </linearGradient>
            </defs>

            {/* 水平网格线与 Y 轴刻度 */}
            {[0, 0.25, 0.5, 0.75, 1.0].map((ratio, idx) => {
              const val = yMin + ratio * (yMax - yMin)
              const yPos = padding.top + plotH - ratio * plotH
              return (
                <g key={idx}>
                  <line
                    x1={padding.left}
                    y1={yPos}
                    x2={svgWidth - padding.right}
                    y2={yPos}
                    stroke="hsla(var(--border)/0.5)"
                    strokeDasharray="3,3"
                  />
                  <text
                    x={padding.left - 8}
                    y={yPos + 4}
                    fontSize="10"
                    fill="hsl(var(--muted))"
                    textAnchor="end"
                  >
                    ¥{Math.round(val)}
                  </text>
                </g>
              )
            })}

            {/* 本金基准水平参考线 */}
            {tfData.initial_capital >= yMin && tfData.initial_capital <= yMax && (
              <g>
                <line
                  x1={padding.left}
                  y1={getY(tfData.initial_capital)}
                  x2={svgWidth - padding.right}
                  y2={getY(tfData.initial_capital)}
                  stroke="#94a3b8"
                  strokeWidth="1.5"
                  strokeDasharray="4,2"
                />
                <text
                  x={svgWidth - padding.right - 4}
                  y={getY(tfData.initial_capital) - 5}
                  fontSize="10"
                  fill="#64748b"
                  textAnchor="end"
                >
                  ¥{roundedBase} 本金线
                </text>
              </g>
            )}

            {/* 面积填充 */}
            {areaPath && <path d={areaPath} fill="url(#capitalGradient)" />}

            {/* 资产净值实线 */}
            {assetPath && (
              <path
                d={assetPath}
                fill="none"
                stroke="#10b981"
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            )}

            {/* X 轴日期刻度 */}
            {curve.length > 0 && [0, Math.floor(curve.length / 2), curve.length - 1].map((cIdx) => {
              const pt = curve[cIdx]
              if (!pt) return null
              return (
                <text
                  key={cIdx}
                  x={getX(cIdx)}
                  y={padding.top + plotH + 18}
                  fontSize="10"
                  fill="hsl(var(--muted))"
                  textAnchor={cIdx === 0 ? 'start' : (cIdx === curve.length - 1 ? 'end' : 'middle')}
                >
                  {pt.date}
                </text>
              )
            })}

            {/* 交互悬停竖线与数据点 */}
            {hoverIndex !== null && curve[hoverIndex] && (
              <g>
                <line
                  x1={getX(hoverIndex)}
                  y1={padding.top}
                  x2={getX(hoverIndex)}
                  y2={padding.top + plotH}
                  stroke="#10b981"
                  strokeWidth="1"
                  strokeDasharray="2,2"
                />
                <circle
                  cx={getX(hoverIndex)}
                  cy={getY(curve[hoverIndex].total_asset)}
                  r="5"
                  fill="#10b981"
                  stroke="#ffffff"
                  strokeWidth="2"
                />
              </g>
            )}

            {/* 透明交互感应区域 */}
            {curve.map((_, i) => {
              const xLeft = i === 0 ? padding.left : (getX(i - 1) + getX(i)) / 2
              const xRight = i === curve.length - 1 ? svgWidth - padding.right : (getX(i) + getX(i + 1)) / 2
              return (
                <rect
                  key={i}
                  x={xLeft}
                  y={padding.top}
                  width={xRight - xLeft}
                  height={plotH}
                  fill="transparent"
                  style={{ cursor: 'crosshair' }}
                  onMouseEnter={() => setHoverIndex(i)}
                />
              )
            })}
          </svg>
        </div>
      </div>

      {/* 当前真实持仓表格 */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
          <span style={{ fontSize: '0.88rem', fontWeight: 700, color: 'hsl(var(--foreground))' }}>
            📦 该周期截至今日持仓明细 (Discrete Holdings)
          </span>
          <span style={{ fontSize: '0.74rem', color: 'hsl(var(--muted))' }}>
            持仓股票市值: ¥{(tfData.stock_market_value || 0).toLocaleString()} · 现金: ¥{(tfData.cash_balance || 0).toLocaleString()}
          </span>
        </div>

        {(!tfData.current_positions || tfData.current_positions.length === 0) ? (
          <div style={{
            background: 'hsla(var(--muted)/0.04)',
            borderRadius: 8,
            border: '1px dashed hsla(var(--border)/0.6)',
            padding: '16px',
            textAlign: 'center',
            fontSize: '0.82rem',
            color: 'hsl(var(--muted))'
          }}>
            🛡️ 当前处于空仓避险状态，全部本金存放于国债逆回购享稳定无风险收益。
          </div>
        ) : (
          <div style={{ overflowX: 'auto', border: '1px solid hsla(var(--border)/0.6)', borderRadius: 8 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem', textAlign: 'left' }}>
              <thead>
                <tr style={{ background: 'hsla(var(--muted)/0.08)', borderBottom: '1px solid hsla(var(--border)/0.6)' }}>
                  <th style={{ padding: '8px 12px' }}>股票标的</th>
                  <th style={{ padding: '8px 12px' }}>持仓股数</th>
                  <th style={{ padding: '8px 12px' }}>买入成本</th>
                  <th style={{ padding: '8px 12px' }}>最新现价</th>
                  <th style={{ padding: '8px 12px' }}>持仓市值</th>
                  <th style={{ padding: '8px 12px' }}>浮动盈亏</th>
                  <th style={{ padding: '8px 12px' }}>建仓时间</th>
                  <th style={{ padding: '8px 12px' }}>Jev策略归属</th>
                </tr>
              </thead>
              <tbody>
                {tfData.current_positions.map((p) => (
                  <tr key={p.code} style={{ borderBottom: '1px solid hsla(var(--border)/0.4)' }}>
                    <td style={{ padding: '8px 12px', fontWeight: 600 }}>
                      {p.name} <span style={{ color: 'hsl(var(--muted))', fontSize: '0.74rem' }}>({p.code})</span>
                    </td>
                    <td style={{ padding: '8px 12px', fontWeight: 600 }}>{p.qty} 股 (1手)</td>
                    <td style={{ padding: '8px 12px' }}>¥{p.buy_price.toFixed(2)}</td>
                    <td style={{ padding: '8px 12px', fontWeight: 600 }}>¥{p.current_price.toFixed(2)}</td>
                    <td style={{ padding: '8px 12px', fontWeight: 600 }}>¥{p.market_value.toFixed(2)}</td>
                    <td style={{
                      padding: '8px 12px',
                      fontWeight: 700,
                      color: p.pnl_rmb >= 0 ? '#10b981' : '#ef4444'
                    }}>
                      {p.pnl_rmb >= 0 ? `+¥${p.pnl_rmb.toFixed(2)}` : `-¥${Math.abs(p.pnl_rmb).toFixed(2)}`} ({p.pnl_pct >= 0 ? `+${p.pnl_pct}%` : `${p.pnl_pct}%`})
                    </td>
                    <td style={{ padding: '8px 12px', color: 'hsl(var(--muted))' }}>{p.buy_date}</td>
                    <td style={{ padding: '8px 12px' }}>
                      <span style={{
                        background: 'rgba(37, 99, 235, 0.08)',
                        color: '#2563eb',
                        border: '1px solid rgba(37, 99, 235, 0.25)',
                        padding: '1px 6px',
                        borderRadius: 4,
                        fontSize: '0.72rem',
                        fontWeight: 600
                      }}>
                        Jev 算力PCB超配
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* 近期成交流水明细账 (Trade Ledger) */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
          <span style={{ fontSize: '0.88rem', fontWeight: 700, color: 'hsl(var(--foreground))' }}>
            📜 该周期完整成交流水账 (从 {tfData.start} 期初 ¥{roundedBase} 起算 · 实扣最低5元佣金与千1印花税)
          </span>
          <span style={{ fontSize: '0.74rem', color: 'hsl(var(--muted))' }}>
            共发生 {tfData.total_trades_count || 0} 笔成交记录
          </span>
        </div>

        <div style={{ overflowX: 'auto', border: '1px solid hsla(var(--border)/0.6)', borderRadius: 8 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.76rem', textAlign: 'left' }}>
            <thead>
              <tr style={{ background: 'hsla(var(--muted)/0.08)', borderBottom: '1px solid hsla(var(--border)/0.6)' }}>
                <th style={{ padding: '7px 10px' }}>成交日期</th>
                <th style={{ padding: '7px 10px' }}>动作</th>
                <th style={{ padding: '7px 10px' }}>股票标的</th>
                <th style={{ padding: '7px 10px' }}>成交单价</th>
                <th style={{ padding: '7px 10px' }}>股数</th>
                <th style={{ padding: '7px 10px' }}>成交总额</th>
                <th style={{ padding: '7px 10px' }}>实扣规费</th>
                <th style={{ padding: '7px 10px' }}>净盈亏 (元/%)</th>
                <th style={{ padding: '7px 10px' }}>触发决策理由</th>
                <th style={{ padding: '7px 10px' }}>成交后现金</th>
              </tr>
            </thead>
            <tbody>
              {(tfData.recent_trades || []).map((t, idx) => {
                const isBuy = t.action === '买入'
                return (
                  <tr key={idx} style={{ borderBottom: '1px solid hsla(var(--border)/0.3)' }}>
                    <td style={{ padding: '7px 10px', color: 'hsl(var(--muted))' }}>{t.date}</td>
                    <td style={{ padding: '7px 10px' }}>
                      <span style={{
                        background: isBuy ? 'rgba(239, 68, 68, 0.1)' : 'rgba(16, 185, 129, 0.1)',
                        color: isBuy ? '#dc2626' : '#059669',
                        padding: '1px 6px',
                        borderRadius: 3,
                        fontWeight: 600
                      }}>
                        {t.action}
                      </span>
                    </td>
                    <td style={{ padding: '7px 10px', fontWeight: 600 }}>
                      {t.name} <span style={{ color: 'hsl(var(--muted))', fontSize: '0.7rem' }}>({t.code})</span>
                    </td>
                    <td style={{ padding: '7px 10px' }}>¥{t.price.toFixed(2)}</td>
                    <td style={{ padding: '7px 10px', fontWeight: 600 }}>{t.qty} 股</td>
                    <td style={{ padding: '7px 10px' }}>¥{t.amount.toFixed(2)}</td>
                    <td style={{ padding: '7px 10px', color: '#ef4444' }}>
                      ¥{t.fee.toFixed(2)} <span style={{ fontSize: '0.68rem', color: 'hsl(var(--muted))' }}>({t.commission > 0 ? `佣¥${t.commission}` : ''}{t.stamp_duty > 0 ? `+税¥${t.stamp_duty}` : ''})</span>
                    </td>
                    <td style={{
                      padding: '7px 10px',
                      fontWeight: 600,
                      color: isBuy ? 'hsl(var(--muted))' : (t.pnl_rmb >= 0 ? '#10b981' : '#ef4444')
                    }}>
                      {isBuy ? '--' : `${t.pnl_rmb >= 0 ? `+¥${t.pnl_rmb}` : `-¥${Math.abs(t.pnl_rmb)}`} (${t.pnl_pct >= 0 ? `+${t.pnl_pct}%` : `${t.pnl_pct}%`})`}
                    </td>
                    <td style={{ padding: '7px 10px', color: 'hsl(var(--foreground))' }}>{t.reason}</td>
                    <td style={{ padding: '7px 10px', fontWeight: 600, color: '#2563eb' }}>¥{t.cash_after.toLocaleString()}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* 万元实盘操作纪律与小资金生存法则 */}
      <div style={{
        background: 'rgba(37, 99, 235, 0.04)',
        border: '1px solid rgba(37, 99, 235, 0.18)',
        borderRadius: 8,
        padding: '12px 16px',
        fontSize: '0.78rem',
        color: 'hsl(var(--foreground))',
        lineHeight: 1.5
      }}>
        <div style={{ fontWeight: 700, color: '#2563eb', marginBottom: 4 }}>
          💡 10,000元小资金实战核心法则 (Small-Capital Execution Rules)
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 8 }}>
          <div>
            <strong>1. 各周期期初整手立起:</strong> 每次切换周期均由 ¥10,000 初始本金在期初买入首选标的，真实检验该周期内的持仓与波段胜率。
          </div>
          <div>
            <strong>2. 规避5元佣金陷阱:</strong> 单笔买入不得低于3,000元。若只买1,000元，5元佣金占比高达0.5%，买卖一次摩擦达1.1%，极易吃光利润。
          </div>
          <div>
            <strong>3. 空闲资金利息保护:</strong> 任何空闲资金（如剩余6,000元或全盘防御期）收盘前自动参与国债逆回购（GC001/R-001），年化2.5%按日计息，零回撤累积安全垫。
          </div>
        </div>
      </div>
    </div>
  )
}
