import React, { useState, useMemo, useEffect } from 'react'
import { RESCUE_BACKTEST_DATABASE } from '../data/rescueBacktestData'
import { fetchLiveStockQuotes } from '../lib/realtimeQuotes'
import { fetchTradesFromCloud } from '../lib/supabaseTrades'
import { SectionCard } from './cards'
import { Compass, RefreshCw, Copy, Check, ShieldAlert, TrendingUp } from 'lucide-react'

// 默认策略配置（与 WorldModelRescue.py 保持一致）
const RESCUE_STRATEGY_CFG = {
  '002284': { name: '亚太股份', lockQty: 500, unit: 200, pulsePct: 0.022, stopPrice: 8.08, role: '主力置换（高抛 + 破位保护）' },
  '603390': { name: '通达电气', lockQty: 0, unit: 200, pulsePct: 0.020, stopPrice: null, role: '微型弱势（脉冲全清腾挪）' },
  '600269': { name: '赣粤高速', lockQty: 1000, unit: 800, pulsePct: 0.022, stopPrice: null, role: '防御减仓（留 1000 股吃分红）' },
  '603187': { name: '海容冷链', lockQty: 500, unit: 0, pulsePct: 0.025, stopPrice: 9.80, role: '破净白马（站上 MA20 坚定持有）' },
}

export default function QuantRescueSection() {
  const [selectedStock, setSelectedStock] = useState('TOTAL')
  const [activeTab, setActiveTab] = useState('val')
  const [hoverIdx, setHoverIdx] = useState(null)

  // 实时操作清单状态
  const [liveQuotes, setLiveQuotes] = useState({})
  const [holdingsQty, setHoldingsQty] = useState({ '002284': 900, '603187': 500, '600269': 1800, '603390': 200 })
  const [quoteLoading, setQuoteLoading] = useState(false)
  const [quoteTime, setQuoteTime] = useState('')
  const [copiedKey, setCopiedKey] = useState('')

  const loadLivePlan = async () => {
    setQuoteLoading(true)
    try {
      const [quotes, trades] = await Promise.all([
        fetchLiveStockQuotes(['002284', '603187', '600269', '603390', '510300']),
        fetchTradesFromCloud().catch(() => null),
      ])
      setLiveQuotes(quotes || {})
      setQuoteTime(new Date().toLocaleTimeString('zh-CN', { hour12: false }))

      if (Array.isArray(trades) && trades.length > 0) {
        const net = {}
        for (const t of trades) {
          const c = String(t.code || '').padStart(6, '0')
          const q = Number(t.quantity || 0)
          const side = String(t.side || 'BUY').toUpperCase()
          net[c] = (net[c] || 0) + (side === 'BUY' ? q : -q)
        }
        setHoldingsQty((prev) => ({ ...prev, ...net }))
      }
    } catch (e) {
      // 忽略网络错误，保留已有报价
    } finally {
      setQuoteLoading(false)
    }
  }

  useEffect(() => {
    loadLivePlan()
  }, [])

  const copyText = (key, text) => {
    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(String(text)).then(() => {
        setCopiedKey(key)
        setTimeout(() => setCopiedKey(''), 1800)
      }).catch(() => {})
    }
  }

  // 构建今日操作清单行
  const actionRows = useMemo(() => {
    return Object.entries(RESCUE_STRATEGY_CFG).map(([code, cfg]) => {
      const q = liveQuotes[code] || {}
      const price = q.price || 0
      const preClose = q.preClose || price
      const changePct = q.changePct ?? 0
      const totalQty = holdingsQty[code] ?? 0
      const releasable = Math.max(0, totalQty - cfg.lockQty)
      const sellQty = cfg.unit > 0 ? Math.min(cfg.unit, releasable) : 0
      const baseRef = preClose > 0 ? Math.max(preClose, price * 0.99) : price
      const sellTrigger = baseRef > 0 ? (baseRef * (1 + cfg.pulsePct)).toFixed(2) : '--'
      const estCash = sellQty > 0 && sellTrigger !== '--' ? Math.round(sellQty * Number(sellTrigger)) : 0

      return {
        code,
        name: cfg.name,
        price,
        changePct,
        totalQty,
        lockQty: cfg.lockQty,
        sellQty,
        sellTrigger,
        stopPrice: cfg.stopPrice,
        estCash,
        role: cfg.role,
      }
    })
  }, [liveQuotes, holdingsQty])

  const etfQuote = liveQuotes['510300'] || {}
  const etfPrice = etfQuote.price || 4.005
  const etfBuyTrigger = ((etfQuote.preClose || etfPrice) * 0.992).toFixed(3)

  const db = RESCUE_BACKTEST_DATABASE || {}
  const currentStockData = db[selectedStock] || db['TOTAL'] || {}
  const data = currentStockData.series || []
  const summary = currentStockData.summary || {}

  const lastPoint = data[data.length - 1] || {}
  const current = hoverIdx !== null && data[hoverIdx] ? data[hoverIdx] : lastPoint

  const svgWidth = 850
  const svgHeight = 280
  const padLeft = 65
  const padRight = 30
  const padTop = 25
  const padBottom = 35
  const chartW = svgWidth - padLeft - padRight
  const chartH = svgHeight - padTop - padBottom

  const yBounds = useMemo(() => {
    if (!data.length) return { min: 0, max: 100 }
    if (activeTab === 'val') {
      const allVals = data.flatMap(d => [d.bench_val, d.gueant_val, d.fernholz_val, d.jev_val])
      const minV = Math.floor(Math.min(...allVals) / (selectedStock === 'TOTAL' ? 1000 : 500)) * (selectedStock === 'TOTAL' ? 1000 : 500)
      const maxV = Math.ceil(Math.max(...allVals) / (selectedStock === 'TOTAL' ? 1000 : 500)) * (selectedStock === 'TOTAL' ? 1000 : 500)
      return { min: minV, max: maxV }
    } else {
      const allCosts = data.flatMap(d => [d.bench_cost, d.gueant_cost, d.fernholz_cost, d.jev_cost])
      const minC = Math.floor(Math.min(...allCosts) * 0.95)
      const maxC = Math.ceil(Math.max(...allCosts) * 1.05)
      return { min: minC, max: maxC }
    }
  }, [data, activeTab, selectedStock])

  const getX = (i) => padLeft + (i / (data.length - 1)) * chartW
  const getY = (val) => {
    const norm = (val - yBounds.min) / (yBounds.max - yBounds.min || 1)
    return padTop + (1 - norm) * chartH
  }

  const generatePath = (key) => {
    if (!data.length) return ''
    return data
      .map((d, i) => `${i === 0 ? 'M' : 'L'} ${getX(i).toFixed(1)} ${getY(d[key]).toFixed(1)}`)
      .join(' ')
  }

  const pathBench = useMemo(() => generatePath(activeTab === 'val' ? 'bench_val' : 'bench_cost'), [data, activeTab, yBounds])
  const pathGueant = useMemo(() => generatePath(activeTab === 'val' ? 'gueant_val' : 'gueant_cost'), [data, activeTab, yBounds])
  const pathFernholz = useMemo(() => generatePath(activeTab === 'val' ? 'fernholz_val' : 'fernholz_cost'), [data, activeTab, yBounds])
  const pathJev = useMemo(() => generatePath(activeTab === 'val' ? 'jev_val' : 'jev_cost'), [data, activeTab, yBounds])

  const stockOptions = [
    { id: 'TOTAL', label: '🌟 全部持仓组合总览 (Portfolio Total)', sub: '合并 4 只总资产挽救走势' },
    { id: '002284', label: '亚太股份 (002284)', sub: '成本 13.15 元 · 900 股' },
    { id: '603187', label: '海容冷链 (603187)', sub: '成本 13.65 元 · 500 股' },
    { id: '600269', label: '赣粤高速 (600269)', sub: '成本 4.51 元 · 1800 股' },
    { id: '603390', label: '通达电气 (603390)', sub: '成本 12.79 元 · 200 股' },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* 1. 今日国信金太阳云端条件单操作清单 */}
      <SectionCard
        title="📋 今日国信金太阳云端条件单操作清单 (World Model 每日调度)"
        subtitle="基于盘中实时行情自动计算今日高抛置换点位与破位保护线，可直接照单填入手机 App 云端条件单（有效期选 30 天，委托价选对手价/买一价）"
        action={
          <button
            onClick={loadLivePlan}
            disabled={quoteLoading}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
              padding: '6px 12px',
              fontSize: 12,
              fontWeight: 600,
              borderRadius: 6,
              border: '1px solid hsl(var(--border))',
              background: 'hsl(var(--secondary))',
              color: 'hsl(var(--foreground))',
              cursor: quoteLoading ? 'wait' : 'pointer',
            }}
          >
            <RefreshCw size={13} className={quoteLoading ? 'animate-spin' : ''} />
            {quoteLoading ? '刷新中...' : `刷新实时点位${quoteTime ? ` (${quoteTime})` : ''}`}
          </button>
        }
      >
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: '1px solid hsl(var(--border))', color: 'hsl(var(--muted-foreground))', textAlign: 'left' }}>
                <th style={{ padding: '10px 8px' }}>标的</th>
                <th style={{ padding: '10px 8px' }}>最新现价</th>
                <th style={{ padding: '10px 8px' }}>持仓 / 锁底仓</th>
                <th style={{ padding: '10px 8px' }}>上涨高抛条件单</th>
                <th style={{ padding: '10px 8px' }}>破位保护线</th>
                <th style={{ padding: '10px 8px' }}>预计释放现金</th>
                <th style={{ padding: '10px 8px' }}>策略定位</th>
              </tr>
            </thead>
            <tbody>
              {actionRows.map((r) => (
                <tr key={r.code} style={{ borderBottom: '1px solid hsl(var(--border) / 0.5)' }}>
                  <td style={{ padding: '10px 8px', fontWeight: 600 }}>
                    {r.name} <span style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))' }}>{r.code}</span>
                  </td>
                  <td style={{ padding: '10px 8px' }}>
                    {r.price > 0 ? (
                      <>
                        <strong>¥{r.price.toFixed(2)}</strong>
                        <span style={{
                          marginLeft: 6,
                          fontSize: 11,
                          color: r.changePct >= 0 ? '#ef4444' : '#10b981',
                          fontWeight: 600
                        }}>
                          {r.changePct >= 0 ? '+' : ''}{r.changePct.toFixed(2)}%
                        </span>
                      </>
                    ) : '--'}
                  </td>
                  <td style={{ padding: '10px 8px' }}>
                    {r.totalQty} 股 <span style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))' }}>(锁 {r.lockQty})</span>
                  </td>
                  <td style={{ padding: '10px 8px' }}>
                    {r.sellQty > 0 ? (
                      <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                        <span style={{
                          padding: '2px 8px',
                          borderRadius: 4,
                          background: 'rgba(59, 130, 246, 0.12)',
                          color: '#3b82f6',
                          fontWeight: 600
                        }}>
                          ≥ ¥{r.sellTrigger} 卖 {r.sellQty} 股
                        </span>
                        <button
                          onClick={() => copyText(`sell_${r.code}`, r.sellTrigger)}
                          title="复制触发价"
                          style={{ border: 'none', background: 'transparent', cursor: 'pointer', color: 'hsl(var(--muted-foreground))', padding: 2 }}
                        >
                          {copiedKey === `sell_${r.code}` ? <Check size={13} color="#10b981" /> : <Copy size={13} />}
                        </button>
                      </div>
                    ) : (
                      <span style={{ color: '#10b981', fontWeight: 600 }}>暂不卖出（继续持有）</span>
                    )}
                  </td>
                  <td style={{ padding: '10px 8px' }}>
                    {r.stopPrice ? (
                      <span style={{ color: '#f59e0b', fontWeight: 600 }}>
                        ≤ ¥{r.stopPrice.toFixed(2)} {r.code === '002284' ? '(卖 200 股)' : '(警示线)'}
                      </span>
                    ) : '--'}
                  </td>
                  <td style={{ padding: '10px 8px', fontWeight: 600 }}>
                    {r.estCash > 0 ? `≈ ¥${r.estCash.toLocaleString()}` : '--'}
                  </td>
                  <td style={{ padding: '10px 8px', fontSize: 12, color: 'hsl(var(--muted-foreground))' }}>
                    {r.role}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* 正期望置换目标池提示 */}
        <div style={{
          marginTop: 14,
          padding: '12px 16px',
          borderRadius: 8,
          background: 'rgba(168, 85, 247, 0.08)',
          border: '1px solid rgba(168, 85, 247, 0.25)',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: 10,
          fontSize: 13
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <TrendingUp size={16} color="#a855f7" />
            <span>
              <strong>正期望置换资金池（510300 沪深300ETF）</strong>：当前现价{' '}
              <strong>¥{etfPrice.toFixed(3)}</strong>
              {etfQuote.changePct !== undefined && (
                <span style={{ marginLeft: 4, color: etfQuote.changePct >= 0 ? '#ef4444' : '#10b981', fontSize: 12 }}>
                  ({etfQuote.changePct >= 0 ? '+' : ''}{Number(etfQuote.changePct).toFixed(2)}%)
                </span>
              )}
              ，卖出释放现金后可在回落条件单设 <strong>≤ ¥{etfBuyTrigger} 买入 500 股</strong>（或手动等额转入）。
            </span>
          </div>
          <span style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))' }}>
            * 每日自动邮件同步推送至已配置邮箱
          </span>
        </div>
      </SectionCard>

      {/* 2. 前沿量化拯救算法全真回测曲线中心 */}
      <SectionCard
        title="🔬 前沿量化拯救算法全真回测中心 (Quant Rescue Lab)"
        subtitle="针对你的 4 只套牢持仓，全真回测验证三大前沿算法在单边下跌中的回血成效与成本压降走势"
      >
        <div style={{
          display: 'flex',
          gap: 8,
          flexWrap: 'wrap',
          marginBottom: 16,
          paddingBottom: 12,
          borderBottom: '1px solid hsl(var(--border))'
        }}>
          {stockOptions.map((opt) => {
            const isSelected = selectedStock === opt.id
            return (
              <button
                key={opt.id}
                onClick={() => {
                  setSelectedStock(opt.id)
                  setHoverIdx(null)
                }}
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'flex-start',
                  padding: '8px 14px',
                  borderRadius: 8,
                  border: isSelected ? '1px solid hsl(var(--primary))' : '1px solid hsl(var(--border))',
                  background: isSelected ? 'hsl(var(--primary) / 0.12)' : 'hsl(var(--secondary) / 0.4)',
                  color: isSelected ? 'hsl(var(--primary))' : 'hsl(var(--foreground))',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  textAlign: 'left'
                }}
              >
                <span style={{ fontSize: 13, fontWeight: isSelected ? 600 : 500 }}>{opt.label}</span>
                <span style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>{opt.sub}</span>
              </button>
            )
          })}
        </div>

        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
          gap: 12,
          marginBottom: 20
        }}>
          <div style={{
            padding: '14px 16px',
            background: 'rgba(239, 68, 68, 0.08)',
            border: '1px solid rgba(239, 68, 68, 0.25)',
            borderRadius: 8
          }}>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#ef4444' }}></span>
              散户死扛基准 (Buy & Hold)
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: '#ef4444', marginTop: 4 }}>
              {lastPoint.bench_return}%
            </div>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>
              保本成本: <strong>{selectedStock === 'TOTAL' ? `¥${currentStockData.cost}` : `¥${currentStockData.cost}/股`}</strong> (原封不动)
            </div>
          </div>

          <div style={{
            padding: '14px 16px',
            background: 'rgba(59, 130, 246, 0.08)',
            border: '1px solid rgba(59, 130, 246, 0.25)',
            borderRadius: 8
          }}>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#3b82f6' }}></span>
              Guéant 非对称做 T 算法
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: '#3b82f6', marginTop: 4 }}>
              {lastPoint.gueant_return}%
            </div>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>
              保本成本降至: <strong>{selectedStock === 'TOTAL' ? `¥${lastPoint.gueant_cost}` : `¥${lastPoint.gueant_cost}/股`}</strong>
              <span style={{ color: '#10b981', marginLeft: 4 }}>(提现 +¥{summary.gueant_cash})</span>
            </div>
          </div>

          <div style={{
            padding: '14px 16px',
            background: 'rgba(16, 185, 129, 0.08)',
            border: '1px solid rgba(16, 185, 129, 0.25)',
            borderRadius: 8
          }}>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#10b981' }}></span>
              Fernholz 波动率收割 + ETF
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: '#10b981', marginTop: 4 }}>
              {lastPoint.fernholz_return}%
            </div>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>
              保本成本降至: <strong>{selectedStock === 'TOTAL' ? `¥${lastPoint.fernholz_cost}` : `¥${lastPoint.fernholz_cost}/股`}</strong> (指数对冲)
            </div>
          </div>

          <div style={{
            padding: '14px 16px',
            background: 'rgba(168, 85, 247, 0.08)',
            border: '1px solid rgba(168, 85, 247, 0.25)',
            borderRadius: 8
          }}>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#a855f7' }}></span>
              World Model & JEV 自适应置换
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: '#a855f7', marginTop: 4 }}>
              {lastPoint.jev_return}%
            </div>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>
              超额 Alpha: <strong style={{ color: '#a855f7' }}>+{(lastPoint.jev_return - lastPoint.bench_return).toFixed(1)}%</strong> (大幅锁住回撤)
            </div>
          </div>
        </div>

        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: 12,
          paddingBottom: 12,
          borderBottom: '1px solid hsl(var(--border))',
          marginBottom: 16
        }}>
          <div style={{ display: 'flex', gap: 8 }}>
            <button
              onClick={() => setActiveTab('val')}
              style={{
                padding: '6px 14px',
                fontSize: 13,
                borderRadius: 6,
                fontWeight: activeTab === 'val' ? 600 : 400,
                background: activeTab === 'val' ? 'hsl(var(--primary))' : 'hsl(var(--secondary))',
                color: activeTab === 'val' ? '#fff' : 'hsl(var(--foreground))',
                border: 'none',
                cursor: 'pointer',
                transition: 'all 0.2s'
              }}
            >
              📈 账户总资产走势 (元)
            </button>
            <button
              onClick={() => setActiveTab('cost')}
              style={{
                padding: '6px 14px',
                fontSize: 13,
                borderRadius: 6,
                fontWeight: activeTab === 'cost' ? 600 : 400,
                background: activeTab === 'cost' ? 'hsl(var(--primary))' : 'hsl(var(--secondary))',
                color: activeTab === 'cost' ? '#fff' : 'hsl(var(--foreground))',
                border: 'none',
                cursor: 'pointer',
                transition: 'all 0.2s'
              }}
            >
              📉 {selectedStock === 'TOTAL' ? '组合实际保本成本压降 (元)' : '每股实际保本成本压降 (元/股)'}
            </button>
          </div>

          <div style={{ fontSize: 13, color: 'hsl(var(--muted-foreground))' }}>
            观测日期: <strong>{current.date}</strong> |
            {selectedStock !== 'TOTAL' && <span> 股票现价: <strong>¥{current.price}</strong> |</span>}
            {activeTab === 'val' ? (
              <span> 拯救挽回: <strong style={{ color: '#10b981' }}>+¥{(current.gueant_val - current.bench_val).toFixed(0)}</strong></span>
            ) : (
              <span> 成本压降: <strong style={{ color: '#3b82f6' }}>-¥{(currentStockData.cost - current.gueant_cost).toFixed(2)}</strong></span>
            )}
          </div>
        </div>

        <div style={{ width: '100%', overflowX: 'auto', position: 'relative' }}>
          <svg
            viewBox={`0 0 ${svgWidth} ${svgHeight}`}
            style={{ width: '100%', height: 'auto', display: 'block', minWidth: 650 }}
            onMouseMove={(e) => {
              const rect = e.currentTarget.getBoundingClientRect()
              const mouseX = ((e.clientX - rect.left) / rect.width) * svgWidth
              if (mouseX >= padLeft && mouseX <= padLeft + chartW) {
                const idx = Math.round(((mouseX - padLeft) / chartW) * (data.length - 1))
                setHoverIdx(Math.max(0, Math.min(data.length - 1, idx)))
              }
            }}
            onMouseLeave={() => setHoverIdx(null)}
          >
            {[0, 0.25, 0.5, 0.75, 1].map((pct, idx) => {
              const y = padTop + (1 - pct) * chartH
              const val = yBounds.min + pct * (yBounds.max - yBounds.min)
              return (
                <g key={idx}>
                  <line x1={padLeft} y1={y} x2={padLeft + chartW} y2={y} stroke="hsl(var(--border))" strokeDasharray="3 3" opacity={0.6} />
                  <text x={padLeft - 8} y={y + 4} textAnchor="end" fontSize="10" fill="hsl(var(--muted-foreground))">
                    {`¥${val.toFixed(selectedStock === 'TOTAL' || activeTab === 'val' ? 0 : 2)}`}
                  </text>
                </g>
              )
            })}

            {[0, Math.floor(data.length * 0.33), Math.floor(data.length * 0.66), data.length - 1].map((idx) => {
              if (!data[idx]) return null
              const x = getX(idx)
              return (
                <text key={idx} x={x} y={svgHeight - 12} textAnchor="middle" fontSize="10" fill="hsl(var(--muted-foreground))">
                  {data[idx].date}
                </text>
              )
            })}

            <path d={pathBench} fill="none" stroke="#ef4444" strokeWidth="2.2" strokeDasharray="4 4" opacity={0.8} />
            <path d={pathFernholz} fill="none" stroke="#10b981" strokeWidth="2.2" />
            <path d={pathGueant} fill="none" stroke="#3b82f6" strokeWidth="2.8" />
            <path d={pathJev} fill="none" stroke="#a855f7" strokeWidth="2.5" />

            {hoverIdx !== null && (
              <line
                x1={getX(hoverIdx)}
                y1={padTop}
                x2={getX(hoverIdx)}
                y2={padTop + chartH}
                stroke="hsl(var(--foreground))"
                strokeWidth="1"
                strokeDasharray="2 2"
              />
            )}
          </svg>
        </div>

        <div style={{
          marginTop: 16,
          padding: '14px 18px',
          background: 'hsl(var(--secondary) / 0.4)',
          borderRadius: 8,
          fontSize: 12,
          color: 'hsl(var(--muted-foreground))',
          lineHeight: 1.8
        }}>
          <div style={{ fontWeight: 600, color: 'hsl(var(--foreground))', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 6 }}>
            <Compass size={15} /> 4 只标的组合拯救全真回测启示：
          </div>
          <div>
            • <span style={{ color: '#ef4444', fontWeight: 600 }}>全部持仓合并死扛</span>：总资金从 29,336 元一路缩水至 21,531 元，总亏损 <strong>-24.8%</strong>；
          </div>
          <div>
            • <span style={{ color: '#3b82f6', fontWeight: 600 }}>Guéant (2023) 全组合做 T 算法</span>：4 只股票各自利用机动仓日内非对称套利，累计收割 <strong>+¥3,421.49 纯现金</strong>，把全账户总亏损大幅压低至 <strong>-13.1%</strong>！
          </div>
          <div>
            • <span style={{ color: '#a855f7', fontWeight: 600 }}>World Model & JEV 动态置换</span>：通达电气和亚太股份在破位初期果断将机动仓置换至高分 Alpha 因子池，最终亏损收窄至 <strong>-8.5%</strong>，极大保全了总本金！
          </div>
        </div>
      </SectionCard>
    </div>
  )
}
