import React, { useState, useMemo, useEffect } from 'react'
import { RESCUE_BACKTEST_DATABASE } from '../data/rescueBacktestData'
import { fetchLiveStockQuotes } from '../lib/realtimeQuotes'
import { fetchTradesFromCloud } from '../lib/supabaseTrades'
import { SectionCard } from './cards'
import { Compass, RefreshCw, Copy, Check, ShieldAlert, TrendingUp } from 'lucide-react'

// Guéant (2023) 非对称网格做 T 策略配置（7 战 7 胜方案）
const RESCUE_STRATEGY_CFG = {
  '002284': { name: '亚太股份', lockQty: 500, unit: 200, atrPct: 0.0500 },
  '603187': { name: '海容冷链', lockQty: 300, unit: 100, atrPct: 0.0298 },
  '600269': { name: '赣粤高速', lockQty: 1000, unit: 400, atrPct: 0.0206 },
  '603390': { name: '通达电气', lockQty: 100, unit: 100, atrPct: 0.0444 },
}
const AZURE_ACK_API = 'https://receives-duplicate-cleaners-recognition.trycloudflare.com'

export default function QuantRescueSection({ jevLive, activeRegime }) {
  const [selectedStock, setSelectedStock] = useState('TOTAL')
  const [selectedPeriod, setSelectedPeriod] = useState('all')
  const [activeTab, setActiveTab] = useState('val')
  const [hoverIdx, setHoverIdx] = useState(null)

  // 实时操作清单状态
  const [liveQuotes, setLiveQuotes] = useState({})
  const [holdingsQty, setHoldingsQty] = useState({ '002284': 900, '603187': 500, '600269': 1800, '603390': 200 })
  const [quoteLoading, setQuoteLoading] = useState(false)
  const [quoteTime, setQuoteTime] = useState('')
  const [copiedKey, setCopiedKey] = useState('')
  const [ackInfo, setAckInfo] = useState(null)
  const [acking, setAcking] = useState(false)

  const checkAckStatus = async () => {
    try {
      const res = await fetch(`${AZURE_ACK_API}/status`)
      if (res.ok) {
        const d = await res.json()
        if (d.ack && d.ack.confirmed_at) setAckInfo(d.ack)
      }
    } catch {}
  }

  const handleConfirmGrid = async () => {
    setAcking(true)
    try {
      const res = await fetch(`${AZURE_ACK_API}/ack?token=sm_rescue_2026&format=json`)
      if (res.ok) {
        const d = await res.json()
        if (d.ack) setAckInfo(d.ack)
      }
    } catch {} finally {
      setAcking(false)
    }
  }

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
    checkAckStatus()
  }, [])

  const copyText = (key, text) => {
    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(String(text)).then(() => {
        setCopiedKey(key)
        setTimeout(() => setCopiedKey(''), 1800)
      }).catch(() => {})
    }
  }

  // 构建今日 Guéant 非对称做 T 清单行
  const actionRows = useMemo(() => {
    return Object.entries(RESCUE_STRATEGY_CFG).map(([code, cfg]) => {
      const q = liveQuotes[code] || {}
      const price = q.price || 0
      const preClose = q.preClose || price
      const changePct = q.changePct ?? 0
      const totalQty = holdingsQty[code] ?? 0
      const baseRef = preClose > 0 ? preClose : price
      const sellTrigger = baseRef > 0 ? (baseRef * (1 + 0.8 * cfg.atrPct)).toFixed(2) : '--'
      const buyTrigger = baseRef > 0 ? (baseRef * (1 - 1.3 * cfg.atrPct)).toFixed(2) : '--'
      const estProfit = sellTrigger !== '--' && buyTrigger !== '--'
        ? Math.round((Number(sellTrigger) - Number(buyTrigger)) * cfg.unit)
        : 0

      return {
        code,
        name: cfg.name,
        price,
        changePct,
        totalQty,
        lockQty: cfg.lockQty,
        unit: cfg.unit,
        atrPct: cfg.atrPct,
        sellTrigger,
        buyTrigger,
        estProfit,
      }
    })
  }, [liveQuotes, holdingsQty])

  const db = RESCUE_BACKTEST_DATABASE || {}
  const currentStockData = db[selectedStock] || db['TOTAL'] || {}
  const rawSeries = currentStockData.series || []

  const periodRanges = [
    { id: 'all', label: '🏆 全周期 (2020~2026 穿越牛熊)', start: '2020-01-01', end: '2099-12-31' },
    { id: 'bull20_21', label: '📈 2020~2021 结构牛市', start: '2020-01-01', end: '2021-12-31' },
    { id: 'grind22_24', label: '〰️ 2022~2024 震荡 + 924快牛', start: '2022-01-01', end: '2024-12-31' },
    { id: 'bear25_26', label: '📉 2025-08~至今 实盘套牢期', start: '2025-08-27', end: '2099-12-31' },
  ]

  // 根据所选周期切片并按区间起点归一化收益率
  const data = useMemo(() => {
    if (!rawSeries.length) return []
    const rng = periodRanges.find((p) => p.id === selectedPeriod) || periodRanges[0]
    const sliced = rawSeries.filter((pt) => pt.date >= rng.start && pt.date <= rng.end)
    const active = sliced.length >= 2 ? sliced : rawSeries
    const basePt = active[0]
    const keys = ['bench', 'gueant', 'fernholz', 'jev', 'avellaneda', 'almgren', 'ppo']
    return active.map((pt) => {
      const next = { ...pt }
      keys.forEach((k) => {
        const bVal = Number(basePt[`${k}_val`] || 1)
        const cVal = Number(pt[`${k}_val`] || bVal)
        next[`${k}_return`] = +(((cVal / bVal) - 1) * 100).toFixed(2)
      })
      return next
    })
  }, [rawSeries, selectedPeriod])

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
      const allVals = data.flatMap(d => [d.bench_val, d.gueant_val, d.fernholz_val, d.jev_val, d.almgren_val, d.avellaneda_val, d.ppo_val])
      const minV = Math.floor(Math.min(...allVals) / (selectedStock === 'TOTAL' ? 1000 : 500)) * (selectedStock === 'TOTAL' ? 1000 : 500)
      const maxV = Math.ceil(Math.max(...allVals) / (selectedStock === 'TOTAL' ? 1000 : 500)) * (selectedStock === 'TOTAL' ? 1000 : 500)
      return { min: minV, max: maxV }
    } else {
      const allCosts = data.flatMap(d => [d.bench_cost, d.gueant_cost, d.fernholz_cost, d.jev_cost, d.almgren_cost, d.avellaneda_cost, d.ppo_cost])
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
  const pathAlmgren = useMemo(() => generatePath(activeTab === 'val' ? 'almgren_val' : 'almgren_cost'), [data, activeTab, yBounds])
  const pathAvellaneda = useMemo(() => generatePath(activeTab === 'val' ? 'avellaneda_val' : 'avellaneda_cost'), [data, activeTab, yBounds])
  const pathPpo = useMemo(() => generatePath(activeTab === 'val' ? 'ppo_val' : 'ppo_cost'), [data, activeTab, yBounds])

  const stockOptions = [
    { id: 'TOTAL', label: '🌟 全部持仓组合总览 (Portfolio Total)', sub: '合并 4 只总资产挽救走势' },
    { id: '002284', label: '亚太股份 (002284)', sub: '成本 13.15 元 · 900 股' },
    { id: '603187', label: '海容冷链 (603187)', sub: '成本 13.65 元 · 500 股' },
    { id: '600269', label: '赣粤高速 (600269)', sub: '成本 4.51 元 · 1800 股' },
    { id: '603390', label: '通达电气 (603390)', sub: '成本 12.79 元 · 200 股' },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* 0. JEV 实时市场时期联动状态条 */}
      {jevLive && jevLive.regimeObj && (
        <div style={{
          padding: '12px 16px',
          borderRadius: 10,
          background: `${jevLive.regimeObj.color}12`,
          border: `1px solid ${jevLive.regimeObj.color}40`,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: 10,
          fontSize: 13
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
            <span style={{ fontWeight: 700, color: jevLive.regimeObj.color }}>
              🧭 JEV 实时联动：{jevLive.regimeObj.label}
            </span>
            {jevLive.livePrice > 0 && (
              <span style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))' }}>
                (沪深300现价 <strong>{jevLive.livePrice}</strong> | MA20: {jevLive.ma20} | MA60: {jevLive.ma60})
              </span>
            )}
          </div>
          <div style={{ fontSize: 12, fontWeight: 600, color: 'hsl(var(--foreground))' }}>
            {jevLive.regimeObj.tradeAdvice}
          </div>
        </div>
      )}

      {/* 1. 今日 Guéant 非对称做 T 网格操作清单 */}
      <SectionCard
        title="📋 今日 Guéant 非对称做 T 网格操作清单 (实测 7 战 7 胜)"
        subtitle="高抛间距窄 (+0.8×ATR)、低吸间距宽 (-1.3×ATR)，严格锁死核心底仓防卖飞；可直接照单在手机 App 设置双向网格条件单"
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
                <th style={{ padding: '10px 8px' }}>高抛卖出线 (+0.8 ATR)</th>
                <th style={{ padding: '10px 8px' }}>低吸买回线 (-1.3 ATR)</th>
                <th style={{ padding: '10px 8px' }}>单次做 T 股数</th>
                <th style={{ padding: '10px 8px' }}>预计单笔差价</th>
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
                    <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                      <span style={{
                        padding: '2px 8px',
                        borderRadius: 4,
                        background: 'rgba(239, 68, 68, 0.12)',
                        color: '#ef4444',
                        fontWeight: 600
                      }}>
                        ≥ ¥{r.sellTrigger}
                      </span>
                      <button
                        onClick={() => copyText(`sell_${r.code}`, r.sellTrigger)}
                        title="复制高抛价"
                        style={{ border: 'none', background: 'transparent', cursor: 'pointer', color: 'hsl(var(--muted-foreground))', padding: 2 }}
                      >
                        {copiedKey === `sell_${r.code}` ? <Check size={13} color="#10b981" /> : <Copy size={13} />}
                      </button>
                    </div>
                  </td>
                  <td style={{ padding: '10px 8px' }}>
                    <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                      <span style={{
                        padding: '2px 8px',
                        borderRadius: 4,
                        background: 'rgba(59, 130, 246, 0.12)',
                        color: '#3b82f6',
                        fontWeight: 600
                      }}>
                        ≤ ¥{r.buyTrigger}
                      </span>
                      <button
                        onClick={() => copyText(`buy_${r.code}`, r.buyTrigger)}
                        title="复制低吸价"
                        style={{ border: 'none', background: 'transparent', cursor: 'pointer', color: 'hsl(var(--muted-foreground))', padding: 2 }}
                      >
                        {copiedKey === `buy_${r.code}` ? <Check size={13} color="#10b981" /> : <Copy size={13} />}
                      </button>
                    </div>
                  </td>
                  <td style={{ padding: '10px 8px', fontWeight: 600 }}>
                    {r.unit} 股
                  </td>
                  <td style={{ padding: '10px 8px', color: '#10b981', fontWeight: 600 }}>
                    {r.estProfit > 0 ? `+¥${r.estProfit}` : '--'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div style={{
          marginTop: 14,
          padding: '12px 16px',
          borderRadius: 8,
          background: ackInfo ? 'rgba(16, 185, 129, 0.08)' : 'rgba(59, 130, 246, 0.08)',
          border: ackInfo ? '1px solid rgba(16, 185, 129, 0.3)' : '1px solid rgba(59, 130, 246, 0.25)',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: 12,
          fontSize: 12
        }}>
          <span>
            {ackInfo ? (
              <>✅ <strong>已确认最新网格参数</strong>（确认于 {ackInfo.confirmed_at}）——在波动率变化超 0.8% 前，Azure 每天 15:05 检测后将保持静默不再提醒。</>
            ) : (
              <>💡 <strong>手机按比例设置幅度</strong>：亚太 +4.0%/-6.5% · 海容 +2.4%/-3.9% · 赣粤 +1.6%/-2.7% · 通达 +3.5%/-5.8%。设好后请点右侧按钮确认不再提醒。</>
            )}
          </span>
          <button
            onClick={handleConfirmGrid}
            disabled={acking}
            style={{
              padding: '7px 14px',
              borderRadius: 6,
              border: 'none',
              background: ackInfo ? '#10b981' : '#2563eb',
              color: '#fff',
              fontWeight: 600,
              fontSize: 12,
              cursor: acking ? 'wait' : 'pointer',
              whiteSpace: 'nowrap',
            }}
          >
            {acking ? '确认中...' : ackInfo ? '✅ 已确认（点此重新确认）' : '✅ 我已在手机设好（不再提醒）'}
          </button>
        </div>
      </SectionCard>

      {/* 2. 前沿量化拯救算法全真回测曲线中心 */}
      <SectionCard
        title="🔬 前沿量化拯救算法全周期回测中心 (Quant Rescue Lab · 2020~2026 穿越牛熊)"
        subtitle="覆盖 2020~2026 全周期（1671 交易日），已按 A 股真实规费实扣（佣金万2.5最低5元 + 卖出印花税0.05%/0.1% + 过户费）"
      >
        <div style={{
          display: 'flex',
          gap: 8,
          flexWrap: 'wrap',
          marginBottom: 12,
        }}>
          {periodRanges.map((pr) => {
            const active = selectedPeriod === pr.id
            return (
              <button
                key={pr.id}
                type="button"
                onClick={() => {
                  setSelectedPeriod(pr.id)
                  setHoverIdx(null)
                }}
                style={{
                  padding: '6px 12px',
                  borderRadius: 6,
                  fontSize: 12,
                  fontWeight: active ? 600 : 400,
                  border: active ? '1px solid hsl(var(--primary))' : '1px solid hsl(var(--border))',
                  background: active ? 'hsl(var(--primary))' : 'hsl(var(--secondary) / 0.5)',
                  color: active ? '#fff' : 'hsl(var(--foreground))',
                  cursor: 'pointer',
                  transition: 'all 0.15s'
                }}
              >
                {pr.label}
              </button>
            )
          })}
        </div>
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

          <div style={{
            padding: '14px 16px',
            background: 'rgba(245, 158, 11, 0.08)',
            border: '1px solid rgba(245, 158, 11, 0.25)',
            borderRadius: 8
          }}>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#f59e0b' }}></span>
              Avellaneda-Stoikov 库存做市
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: '#f59e0b', marginTop: 4 }}>
              {lastPoint.avellaneda_return}%
            </div>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>
              保本成本降至: <strong>{selectedStock === 'TOTAL' ? `¥${lastPoint.avellaneda_cost}` : `¥${lastPoint.avellaneda_cost}/股`}</strong> (库存偏移)
            </div>
          </div>

          <div style={{
            padding: '14px 16px',
            background: 'rgba(6, 182, 212, 0.08)',
            border: '1px solid rgba(6, 182, 212, 0.25)',
            borderRadius: 8
          }}>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#06b6d4' }}></span>
              Almgren-Chriss 最优冲击执行
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: '#06b6d4', marginTop: 4 }}>
              {lastPoint.almgren_return}%
            </div>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>
              保本成本降至: <strong>{selectedStock === 'TOTAL' ? `¥${lastPoint.almgren_cost}` : `¥${lastPoint.almgren_cost}/股`}</strong> (分批低冲击)
            </div>
          </div>

          <div style={{
            padding: '14px 16px',
            background: 'rgba(236, 72, 153, 0.08)',
            border: '1px solid rgba(236, 72, 153, 0.25)',
            borderRadius: 8
          }}>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#ec4899' }}></span>
              PPO 深度强化学习做 T
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: '#ec4899', marginTop: 4 }}>
              {lastPoint.ppo_return}%
            </div>
            <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 2 }}>
              保本成本降至: <strong>{selectedStock === 'TOTAL' ? `¥${lastPoint.ppo_cost}` : `¥${lastPoint.ppo_cost}/股`}</strong> (高胜率门控)
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
            {(() => {
              const baseVal = Number(data[0]?.bench_val || 1)
              const periodRescue = Math.round(baseVal * ((current.gueant_return - current.bench_return) / 100))
              const costDrop = Number((currentStockData.cost - current.gueant_cost).toFixed(2))
              return activeTab === 'val' ? (
                <span> 区间相对死扛挽回: <strong style={{ color: periodRescue >= 0 ? '#10b981' : '#ef4444' }}>
                  {periodRescue >= 0 ? `+¥${periodRescue}` : `-¥${Math.abs(periodRescue)}`}
                </strong></span>
              ) : (
                <span> 成本压降: <strong style={{ color: costDrop >= 0 ? '#3b82f6' : '#ef4444' }}>
                  {costDrop >= 0 ? `-¥${costDrop}` : `+¥${Math.abs(costDrop)}`}
                </strong></span>
              )
            })()}
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

            <path d={pathBench} fill="none" stroke="#ef4444" strokeWidth="2.0" strokeDasharray="4 4" opacity={0.75} />
            <path d={pathFernholz} fill="none" stroke="#10b981" strokeWidth="2.0" />
            <path d={pathAlmgren} fill="none" stroke="#06b6d4" strokeWidth="2.0" strokeDasharray="6 3" />
            <path d={pathGueant} fill="none" stroke="#3b82f6" strokeWidth="2.6" />
            <path d={pathAvellaneda} fill="none" stroke="#f59e0b" strokeWidth="2.4" />
            <path d={pathPpo} fill="none" stroke="#ec4899" strokeWidth="2.3" />
            <path d={pathJev} fill="none" stroke="#a855f7" strokeWidth="2.4" />

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
          lineHeight: 1.7
        }}>
          <div style={{ fontWeight: 700, color: 'hsl(var(--foreground))', marginBottom: 10, display: 'flex', alignItems: 'center', gap: 6, fontSize: 13 }}>
            <Compass size={15} /> 📖 6 大拯救策略具体操作手册（怎么买 / 怎么卖 / 锁多少仓 / 适用行情）
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 10 }}>
            <div style={{ padding: '10px 12px', borderRadius: 6, background: 'hsl(var(--card))', border: '1px solid rgba(59, 130, 246, 0.25)' }}>
              <div style={{ fontWeight: 700, color: '#3b82f6', marginBottom: 4 }}>1. Guéant 非对称网格做 T</div>
              <div>• <strong>仓位：</strong>锁死 50% 核心底仓，其余作为机动做 T 仓位。</div>
              <div>• <strong>操作：</strong>手机设网格条件单，涨 <strong>+0.8×ATR</strong> 高抛卖出、跌 <strong>-1.3×ATR</strong> 低吸买回。</div>
              <div>• <strong>注意：</strong>阴跌/震荡市降本极强，但牛市主升需暂停卖出防卖飞。</div>
            </div>
            <div style={{ padding: '10px 12px', borderRadius: 6, background: 'hsl(var(--card))', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
              <div style={{ fontWeight: 700, color: '#10b981', marginBottom: 4 }}>2. Fernholz 波动率收割 + ETF（全周期最优）</div>
              <div>• <strong>仓位：</strong>目标维持 82% 股票 + 18% 现金（或宽基 ETF）动态平衡。</div>
              <div>• <strong>操作：</strong>每当股价上涨导致持仓占比偏离超 1 手就自动卖 1 手落袋，下跌偏离超 1 手就自动低吸补回。</div>
              <div>• <strong>注意：</strong>涨跌都能自动收割波动率，穿越牛熊表现最稳。</div>
            </div>
            <div style={{ padding: '10px 12px', borderRadius: 6, background: 'hsl(var(--card))', border: '1px solid rgba(236, 72, 153, 0.25)' }}>
              <div style={{ fontWeight: 700, color: '#ec4899', marginBottom: 4 }}>3. PPO 深度强化学习做 T（门控防卖飞）</div>
              <div>• <strong>仓位：</strong>同样保留 50% 底仓，但加入均线趋势门控开关。</div>
              <div>• <strong>操作：</strong>多头主升期只买不卖；非主升期偏离 MA20 超 <strong>+1.6×ATR</strong> 才高抛、跌破 <strong>-1.4×ATR</strong> 才低吸。</div>
              <div>• <strong>注意：</strong>大幅减少无效手续费磨损，兼顾牛市不踏空与熊市降本。</div>
            </div>
            <div style={{ padding: '10px 12px', borderRadius: 6, background: 'hsl(var(--card))', border: '1px solid rgba(245, 158, 11, 0.25)' }}>
              <div style={{ fontWeight: 700, color: '#f59e0b', marginBottom: 4 }}>4. Avellaneda-Stoikov 库存做市</div>
              <div>• <strong>仓位：</strong>根据当前库存占初始仓位比例动态调节挂单价。</div>
              <div>• <strong>操作：</strong>满仓深套时自动下移卖出触发价加速减仓降成本，轻仓时上移买回价积极回补；牛市关闭卖出。</div>
              <div>• <strong>注意：</strong>适合仓位过重、急需腾出现金缓冲的账户。</div>
            </div>
            <div style={{ padding: '10px 12px', borderRadius: 6, background: 'hsl(var(--card))', border: '1px solid rgba(6, 182, 212, 0.25)' }}>
              <div style={{ fontWeight: 700, color: '#06b6d4', marginBottom: 4 }}>5. Almgren-Chriss 最优冲击执行</div>
              <div>• <strong>仓位：</strong>每次仅用最小整手（100 股）分批微调。</div>
              <div>• <strong>操作：</strong>平时静默不交易，仅在日内振幅 ≥ <strong>1.5×ATR</strong> 的大波动日按 +1.05×ATR 卖 / -1.15×ATR 买。</div>
              <div>• <strong>注意：</strong>滑点与规费磨损最低，套牢期回撤控制极佳。</div>
            </div>
            <div style={{ padding: '10px 12px', borderRadius: 6, background: 'hsl(var(--card))', border: '1px solid rgba(168, 85, 247, 0.25)' }}>
              <div style={{ fontWeight: 700, color: '#a855f7', marginBottom: 4 }}>6. World Model & JEV 自适应置换</div>
              <div>• <strong>仓位：</strong>牛市 100% 满仓持有；破位 MA20 & MA60 时将机动仓砍至 60%。</div>
              <div>• <strong>操作：</strong>不纠结日内做 T，跌破均线直接减仓转入国债逆回购或置换为高评分 Alpha 标的。</div>
              <div>• <strong>注意：</strong>专防个股基本面恶化导致的长期阴跌深套。</div>
            </div>
          </div>
        </div>
      </SectionCard>
    </div>
  )
}
