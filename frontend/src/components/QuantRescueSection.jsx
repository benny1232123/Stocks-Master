import React, { useState, useMemo, useEffect } from 'react'
import { RESCUE_BACKTEST_DATABASE } from '../data/rescueBacktestData'
import { fetchLiveStockQuotes } from '../lib/realtimeQuotes'
import { fetchTradesFromCloud } from '../lib/supabaseTrades'
import { SectionCard } from './cards'
import { Compass, RefreshCw, Copy, Check, ShieldAlert, TrendingUp } from 'lucide-react'

// 一票一策 · 按实扣5元最低佣金后的全周期/套牢期实测最优策略配置
const RESCUE_STRATEGY_CFG = {
  '002284': {
    name: '亚太股份',
    strategy: 'Almgren-Chriss 宽幅',
    strategyColor: '#06b6d4',
    lockQty: 500,
    unit: 200,
    atrPct: 0.0500,
    sellMult: 1.05,
    buyMult: 1.15,
    tip: '大波动(≥1.5×ATR)再成交，防5元佣金磨损'
  },
  '603187': {
    name: '海容冷链',
    strategy: 'Avellaneda 库存偏移',
    strategyColor: '#f59e0b',
    lockQty: 300,
    unit: 100,
    atrPct: 0.0298,
    sellMult: 0.70,
    buyMult: 1.30,
    tip: '重仓先高抛腾现金，转强站稳MA20即停卖'
  },
  '600269': {
    name: '赣粤高速',
    strategy: 'Fernholz 低频平衡',
    strategyColor: '#10b981',
    lockQty: 1000,
    unit: 400,
    atrPct: 0.0206,
    sellMult: 1.50,
    buyMult: 1.50,
    tip: '低波红利压舱收息，仅极端偏离调仓一次'
  },
  '603390': {
    name: '通达电气',
    strategy: 'PPO 极值反抽减仓',
    strategyColor: '#ec4899',
    lockQty: 100,
    unit: 100,
    atrPct: 0.0444,
    sellMult: 1.60,
    buyMult: 1.60,
    noBuy: true,
    tip: '❌ 停高频网格！仅放量急拉高抛100股减仓'
  },
}
const AZURE_ACK_API = 'https://receives-duplicate-cleaners-recognition.trycloudflare.com'

export default function QuantRescueSection({ jevLive, activeRegime, refreshTick = 0 }) {
  const [selectedStock, setSelectedStock] = useState('TOTAL')
  const [selectedPeriod, setSelectedPeriod] = useState('all')
  const [activeTab, setActiveTab] = useState('val')
  const [hoverIdx, setHoverIdx] = useState(null)

  // 实时操作清单状态
  const [liveQuotes, setLiveQuotes] = useState({})
  const [stockTech, setStockTech] = useState({})
  const [latestFills, setLatestFills] = useState({})
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
      const codes = ['002284', '603187', '600269', '603390']
      const fetchTech = async (c) => {
        const sym = c.startsWith('6') ? `sh${c}` : `sz${c}`
        try {
          const r = await fetch(`https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=${sym},day,,,65,qfq`)
          if (!r.ok) return null
          const json = await r.json()
          const bars = json?.data?.[sym]?.qfqday || json?.data?.[sym]?.day || []
          if (bars.length < 20) return null
          const closes = bars.map((b) => Number(b[2] || 0))
          const ma20 = closes.slice(-20).reduce((a, b) => a + b, 0) / 20
          const ma60 = closes.slice(-Math.min(60, closes.length)).reduce((a, b) => a + b, 0) / Math.min(60, closes.length)
          const trs = []
          for (let i = 1; i < bars.length; i++) {
            const h = Number(bars[i][3] || 0)
            const l = Number(bars[i][4] || 0)
            const pc = Number(bars[i - 1][2] || 0)
            trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)))
          }
          const atr14 = trs.slice(-14).reduce((a, b) => a + b, 0) / Math.min(14, trs.length)
          const lastC = closes[closes.length - 1] || 1
          return {
            ma20: +ma20.toFixed(2),
            ma60: +ma60.toFixed(2),
            atrPct: Math.max(0.015, Math.min(0.055, atr14 / lastC)),
          }
        } catch {
          return null
        }
      }

      const [quotes, trades, techList] = await Promise.all([
        fetchLiveStockQuotes([...codes, '510300']),
        fetchTradesFromCloud().catch(() => null),
        Promise.all(codes.map(fetchTech)),
      ])
      setLiveQuotes(quotes || {})
      const techMap = {}
      codes.forEach((c, idx) => {
        if (techList[idx]) techMap[c] = techList[idx]
      })
      if (Object.keys(techMap).length > 0) setStockTech(techMap)
      setQuoteTime(new Date().toLocaleTimeString('zh-CN', { hour12: false }))

      if (Array.isArray(trades) && trades.length > 0) {
        const net = {}
        const latest = {}
        for (const t of trades) {
          const c = String(t.code || '').padStart(6, '0')
          const q = Number(t.quantity || 0)
          const p = Number(t.price || 0)
          const d = String(t.trade_date || t.date || '')
          const side = String(t.side || 'BUY').toUpperCase()
          net[c] = (net[c] || 0) + (side === 'BUY' ? q : -q)
          if (d >= '2026-11-24' && p > 0) {
            if (!latest[c] || d >= latest[c].date) {
              latest[c] = { date: d, price: p, side }
            }
          }
        }
        setHoldingsQty((prev) => ({ ...prev, ...net }))
        setLatestFills(latest)
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
  }, [refreshTick])

  const copyText = (key, text) => {
    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(String(text)).then(() => {
        setCopiedKey(key)
        setTimeout(() => setCopiedKey(''), 1800)
      }).catch(() => {})
    }
  }

  // 构建今日「一票一策」自动状态机切换清单行（实时 MA20/MA60/ATR + 市值驱动 + 成交/昨收基准价）
  const actionRows = useMemo(() => {
    return Object.entries(RESCUE_STRATEGY_CFG).map(([code, cfg]) => {
      const q = liveQuotes[code] || {}
      const tech = stockTech[code] || null
      const fill = latestFills[code] || null
      const price = q.price || 0
      const preClose = q.preClose || price
      const changePct = q.changePct ?? 0
      const totalQty = holdingsQty[code] ?? 0
      const baseRef = fill ? fill.price : (preClose > 0 ? preClose : price)
      const baseSource = fill ? `成交锚定 (${fill.date})` : '昨收锚定 (盘中固定)'
      const atrPct = tech?.atrPct || cfg.atrPct
      const mktVal = (price > 0 ? price : baseRef) * totalQty

      // 自动判定每只股票当前最优策略
      let strategy = cfg.strategy
      let strategyColor = cfg.strategyColor
      let sellMult = cfg.sellMult
      let buyMult = cfg.buyMult
      let noBuy = !!cfg.noBuy
      let noSell = false
      let tip = cfg.tip

      if (tech && baseRef > 0) {
        const isBull = baseRef > tech.ma20 && tech.ma20 > tech.ma60
        if (isBull) {
          strategy = '🔒 多头主升锁仓'
          strategyColor = '#7c3aed'
          noSell = true
          noBuy = true
          tip = `已站稳MA20(${tech.ma20})&MA60(${tech.ma60})多头，锁仓不卖防踏空`
        } else if (mktVal > 0 && mktVal < 3000) {
          strategy = 'PPO 极值反抽减仓'
          strategyColor = '#ec4899'
          sellMult = 1.60
          buyMult = 1.60
          noBuy = true
          tip = '市值<3000停高频网格！仅放量急拉高抛100股减仓'
        } else if (atrPct < 0.024) {
          strategy = 'Fernholz 低频平衡'
          strategyColor = '#10b981'
          sellMult = 1.50
          buyMult = 1.50
          noBuy = false
          tip = `实时ATR ${(atrPct * 100).toFixed(1)}% 偏低，仅极端偏离调仓防5元磨损`
        } else if (atrPct < 0.038) {
          strategy = 'Avellaneda 库存偏移'
          strategyColor = '#f59e0b'
          sellMult = 0.70
          buyMult = 1.30
          noBuy = false
          tip = `弱势重仓先高抛腾现金，站稳MA20(${tech.ma20})即停卖`
        } else {
          strategy = 'Almgren-Chriss 宽幅'
          strategyColor = '#06b6d4'
          sellMult = 1.05
          buyMult = 1.15
          noBuy = false
          tip = `高波(ATR ${(atrPct * 100).toFixed(1)}%)大振幅再成交，防5元磨损`
        }
      }

      const sellTrigger = noSell ? null : (baseRef > 0 ? (baseRef * (1 + sellMult * atrPct)).toFixed(2) : '--')
      const buyTrigger = noBuy ? null : (baseRef > 0 ? (baseRef * (1 - buyMult * atrPct)).toFixed(2) : '--')
      const estProfit = !noBuy && !noSell && sellTrigger && sellTrigger !== '--' && buyTrigger && buyTrigger !== '--'
        ? Math.round((Number(sellTrigger) - Number(buyTrigger)) * cfg.unit - 10)
        : 0

      return {
        code,
        name: cfg.name,
        strategy,
        strategyColor,
        noBuy,
        noSell,
        tip,
        baseRef,
        baseSource,
        sellPctStr: `+${(sellMult * atrPct * 100).toFixed(1)}%`,
        buyPctStr: `-${(buyMult * atrPct * 100).toFixed(1)}%`,
        price,
        changePct,
        totalQty,
        lockQty: cfg.lockQty,
        unit: cfg.unit,
        atrPct,
        sellTrigger,
        buyTrigger,
        estProfit,
      }
    })
  }, [liveQuotes, stockTech, holdingsQty])

  const db = RESCUE_BACKTEST_DATABASE || {}
  const currentStockData = db[selectedStock] || db['TOTAL'] || {}
  const rawSeries = currentStockData.series || []

  const periodRanges = [
    { id: 'day', label: '日', title: '日频 (近10个交易日)', count: 10 },
    { id: 'week', label: '周', title: '周度 (近1周5个交易日)', count: 5 },
    { id: 'month', label: '月', title: '月度 (近1月22个交易日)', count: 22 },
    { id: 'quarter', label: '季', title: '季度 (近1季66个交易日)', count: 66 },
    { id: 'year', label: '年', title: '年度 (近1年250个交易日)', count: 250 },
    { id: 'all', label: '全部', title: '全部 (2020~至今全景)', count: null },
  ]

  // 根据所选周期切片并按区间起点归一化收益率
  const data = useMemo(() => {
    if (!rawSeries.length) return []
    const rng = periodRanges.find((p) => p.id === selectedPeriod) || periodRanges[periodRanges.length - 1]
    const sliced = rng.count ? rawSeries.slice(-Math.min(rng.count, rawSeries.length)) : rawSeries
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

      {/* 1. 今日「一票一策」专属做 T / 减仓操作清单 */}
      <SectionCard
        title="📋 今日「一票一策」专属条件单清单 (已扣最低5元佣金净差价)"
        subtitle="基准价规则：有近期录入成交则以「最新成交价」为锚，否则以「昨收价」盘中固定锚定；录入新成交后自动刷新基准与持仓，策略变动同步发邮件提醒"
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
                <th style={{ padding: '10px 8px' }}>标的 / 专属策略</th>
                <th style={{ padding: '10px 8px' }}>最新现价</th>
                <th style={{ padding: '10px 8px' }}>网格基准价</th>
                <th style={{ padding: '10px 8px' }}>持仓 / 锁底仓</th>
                <th style={{ padding: '10px 8px' }}>高抛卖出线</th>
                <th style={{ padding: '10px 8px' }}>低吸买回线</th>
                <th style={{ padding: '10px 8px' }}>单次股数</th>
                <th style={{ padding: '10px 8px' }}>扣费后净差价</th>
              </tr>
            </thead>
            <tbody>
              {actionRows.map((r) => (
                <tr key={r.code} style={{ borderBottom: '1px solid hsl(var(--border) / 0.5)' }}>
                  <td style={{ padding: '10px 8px', fontWeight: 600 }}>
                    <div>{r.name} <span style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))' }}>{r.code}</span></div>
                    <span style={{
                      display: 'inline-block',
                      marginTop: 3,
                      padding: '1px 6px',
                      borderRadius: 4,
                      fontSize: 11,
                      fontWeight: 600,
                      background: `${r.strategyColor}15`,
                      color: r.strategyColor,
                      border: `1px solid ${r.strategyColor}35`
                    }}>
                      {r.strategy}
                    </span>
                    <div style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))', fontWeight: 400, marginTop: 2 }}>{r.tip}</div>
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
                    {r.baseRef > 0 ? (
                      <div>
                        <strong>¥{r.baseRef.toFixed(2)}</strong>
                        <div style={{ fontSize: 10.5, color: r.baseSource.startsWith('成交') ? '#2563eb' : 'hsl(var(--muted-foreground))' }}>
                          {r.baseSource}
                        </div>
                      </div>
                    ) : '--'}
                  </td>
                  <td style={{ padding: '10px 8px' }}>
                    {r.totalQty} 股 <span style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))' }}>(锁 {r.lockQty})</span>
                  </td>
                  <td style={{ padding: '10px 8px' }}>
                    {r.noSell ? (
                      <span style={{
                        padding: '2px 8px',
                        borderRadius: 4,
                        background: 'rgba(124, 58, 237, 0.12)',
                        color: '#7c3aed',
                        fontSize: 12,
                        fontWeight: 600
                      }}>
                        🔒 锁仓不卖（拿住主升）
                      </span>
                    ) : (
                      <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                        <span style={{
                          padding: '2px 8px',
                          borderRadius: 4,
                          background: 'rgba(239, 68, 68, 0.12)',
                          color: '#ef4444',
                          fontWeight: 600
                        }}>
                          ≥ ¥{r.sellTrigger} ({r.sellPctStr})
                        </span>
                        <button
                          onClick={() => copyText(`sell_${r.code}`, r.sellTrigger)}
                          title="复制高抛价"
                          style={{ border: 'none', background: 'transparent', cursor: 'pointer', color: 'hsl(var(--muted-foreground))', padding: 2 }}
                        >
                          {copiedKey === `sell_${r.code}` ? <Check size={13} color="#10b981" /> : <Copy size={13} />}
                        </button>
                      </div>
                    )}
                  </td>
                  <td style={{ padding: '10px 8px' }}>
                    {r.noBuy ? (
                      <span style={{
                        padding: '2px 8px',
                        borderRadius: 4,
                        background: 'hsla(var(--muted)/0.12)',
                        color: 'hsl(var(--muted-foreground))',
                        fontSize: 12,
                        fontWeight: 500
                      }}>
                        {r.noSell ? '⏸ 暂停低吸（拿住底仓）' : '⏸ 暂停低吸（只减不加）'}
                      </span>
                    ) : (
                      <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                        <span style={{
                          padding: '2px 8px',
                          borderRadius: 4,
                          background: 'rgba(59, 130, 246, 0.12)',
                          color: '#3b82f6',
                          fontWeight: 600
                        }}>
                          ≤ ¥{r.buyTrigger} ({r.buyPctStr})
                        </span>
                        <button
                          onClick={() => copyText(`buy_${r.code}`, r.buyTrigger)}
                          title="复制低吸价"
                          style={{ border: 'none', background: 'transparent', cursor: 'pointer', color: 'hsl(var(--muted-foreground))', padding: 2 }}
                        >
                          {copiedKey === `buy_${r.code}` ? <Check size={13} color="#10b981" /> : <Copy size={13} />}
                        </button>
                      </div>
                    )}
                  </td>
                  <td style={{ padding: '10px 8px', fontWeight: 600 }}>
                    {r.unit} 股
                  </td>
                  <td style={{ padding: '10px 8px', color: (r.noBuy || r.noSell) ? 'hsl(var(--muted-foreground))' : '#10b981', fontWeight: 600 }}>
                    {r.noSell ? '主升锁仓吃波段' : r.noBuy ? '单向减仓兑现' : (r.estProfit > 0 ? `+¥${r.estProfit}` : '--')}
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
              <>💡 <strong>手机按比例设置幅度（一票一策）</strong>：亚太 +5.3%/-5.8% (Almgren宽幅) · 海容 +2.1%/-3.9% (Avellaneda减仓) · 赣粤 +3.1%/-3.1% (低频平衡) · 通达停网格，仅急拉 +7.1% 高抛减仓。</>
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
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 14 }}>
          <span style={{ fontSize: '0.82rem', color: 'hsl(var(--muted))', fontWeight: 500, marginRight: 2 }}>
            周期切换:
          </span>
          <div style={{ display: 'inline-flex', padding: 2, background: 'hsla(var(--border)/0.4)', borderRadius: 8, gap: 4 }}>
            {periodRanges.map((pr) => {
              const active = selectedPeriod === pr.id
              return (
                <button
                  key={pr.id}
                  type="button"
                  title={pr.title}
                  onClick={() => {
                    setSelectedPeriod(pr.id)
                    setHoverIdx(null)
                  }}
                  style={{
                    border: 'none',
                    padding: '6px 16px',
                    borderRadius: 6,
                    fontSize: '0.82rem',
                    fontWeight: active ? 600 : 400,
                    background: active ? 'hsl(var(--primary))' : 'transparent',
                    color: active ? '#fff' : 'hsl(var(--foreground))',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease'
                  }}
                >
                  {pr.label}
                </button>
              )
            })}
          </div>
          <span style={{ fontSize: '0.76rem', color: 'hsl(var(--muted))', marginLeft: 6 }}>
            (已更新至最近收盘 · {rawSeries[rawSeries.length - 1]?.date || '最新'})
          </span>
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

        {/* 拯救策略核心风控与性价比排行榜 (按当前所选股票与周期动态重算) */}
        {data.length >= 2 && (() => {
          const RESCUE_META = [
            { id: 'fernholz', label: 'Fernholz 波动率收割 + ETF', color: '#10b981' },
            { id: 'ppo', label: 'PPO 深度强化学习做 T', color: '#ec4899' },
            { id: 'almgren', label: 'Almgren-Chriss 最优冲击', color: '#06b6d4' },
            { id: 'avellaneda', label: 'Avellaneda-Stoikov 库存做市', color: '#f59e0b' },
            { id: 'gueant', label: 'Guéant 非对称做 T', color: '#3b82f6' },
            { id: 'jev', label: 'World Model & JEV 自适应', color: '#a855f7' },
            { id: 'bench', label: '散户死扛基准 (Buy & Hold)', color: '#ef4444' },
          ]
          const days = data.length
          const benchRet = Number(lastPoint.bench_return || 0)
          const benchRmb = Math.round(Number(lastPoint.bench_val || 0) - Number(data[0]?.bench_val || 0))
          const rows = RESCUE_META.map((m) => {
            const vals = data.map((d) => Number(d[`${m.id}_val`] || 0))
            let peak = vals[0] || 1
            let maxDd = 0
            let upDays = 0
            for (let i = 0; i < vals.length; i++) {
              if (vals[i] > peak) peak = vals[i]
              const dd = peak > 0 ? ((vals[i] - peak) / peak) * 100 : 0
              if (dd < maxDd) maxDd = dd
              if (i > 0 && vals[i] > vals[i - 1]) upDays++
            }
            const totalRet = Number(lastPoint[`${m.id}_return`] || 0)
            const rmbPnl = Math.round((vals[vals.length - 1] || 0) - (vals[0] || 0))
            const excessRmb = rmbPnl - benchRmb
            const cagr = days >= 20
              ? +((Math.pow(Math.max(0.01, 1 + totalRet / 100), 252 / days) - 1) * 100).toFixed(2)
              : totalRet
            const ddRounded = +maxDd.toFixed(2)
            const calmar = Math.abs(ddRounded) > 0.1 ? +(cagr / Math.abs(ddRounded)).toFixed(2) : (cagr > 0 ? 9.99 : 0)
            const excess = +(totalRet - benchRet).toFixed(2)
            const winRate = +((upDays / Math.max(1, days - 1)) * 100).toFixed(1)
            return { ...m, totalRet, rmbPnl, excessRmb, cagr, maxDd: ddRounded, calmar, excess, winRate }
          }).sort((a, b) => b.calmar - a.calmar)

          return (
            <div style={{ marginTop: 16, padding: '14px 16px', borderRadius: 8, background: 'hsla(var(--muted)/0.04)', border: '1px solid hsl(var(--border))' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8, marginBottom: 10 }}>
                <span style={{ fontSize: 13, fontWeight: 700, color: 'hsl(var(--foreground))' }}>
                  📊 当前区间综合指标排行榜（按卡玛比率降序 · 同步随标的与周期重算）
                </span>
                <span style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))' }}>
                  卡玛越高代表“抗跌性价比”越高 · 相对死扛为正则跑赢躺平
                </span>
              </div>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, textAlign: 'left' }}>
                  <thead>
                    <tr style={{ borderBottom: '1px solid hsl(var(--border))', color: 'hsl(var(--muted-foreground))' }}>
                      <th style={{ padding: '7px 8px' }}>排名 / 策略</th>
                      <th style={{ padding: '7px 8px' }}>卡玛比率</th>
                      <th style={{ padding: '7px 8px' }}>年化收益 (CAGR)</th>
                      <th style={{ padding: '7px 8px' }}>最大回撤</th>
                      <th style={{ padding: '7px 8px' }}>相对死扛超额</th>
                      <th style={{ padding: '7px 8px' }}>日胜率</th>
                      <th style={{ padding: '7px 8px' }}>区间盈亏 (元 / %)</th>
                      <th style={{ padding: '7px 8px' }}>相对死扛多赚 (元)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((r, idx) => (
                      <tr key={r.id} style={{ borderBottom: '1px solid hsl(var(--border) / 0.4)' }}>
                        <td style={{ padding: '7px 8px', fontWeight: 600 }}>
                          <span style={{ color: 'hsl(var(--muted-foreground))', marginRight: 6 }}>#{idx + 1}</span>
                          <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: '50%', background: r.color, marginRight: 6 }} />
                          {r.label}
                        </td>
                        <td style={{ padding: '7px 8px', fontWeight: 700, color: r.calmar >= 0.5 ? '#10b981' : '#d97706' }}>{r.calmar}</td>
                        <td style={{ padding: '7px 8px', color: r.cagr >= 0 ? '#10b981' : '#ef4444' }}>{r.cagr >= 0 ? '+' : ''}{r.cagr}%</td>
                        <td style={{ padding: '7px 8px', color: '#ef4444' }}>{r.maxDd}%</td>
                        <td style={{ padding: '7px 8px', fontWeight: 600, color: r.excess >= 0 ? '#2563eb' : '#ef4444' }}>
                          {r.id === 'bench' ? '基准 0.00%' : `${r.excess >= 0 ? '+' : ''}${r.excess}%`}
                        </td>
                        <td style={{ padding: '7px 8px' }}>{r.winRate}%</td>
                        <td style={{ padding: '7px 8px', fontWeight: 600, color: r.rmbPnl >= 0 ? '#10b981' : '#ef4444' }}>
                          {r.rmbPnl >= 0 ? '+' : '-'}¥{Math.abs(r.rmbPnl).toLocaleString()} ({r.totalRet >= 0 ? '+' : ''}{r.totalRet}%)
                        </td>
                        <td style={{ padding: '7px 8px', fontWeight: 700, color: r.excessRmb >= 0 ? '#2563eb' : '#ef4444' }}>
                          {r.id === 'bench' ? '基准 ¥0' : `${r.excessRmb >= 0 ? '+' : '-'}¥${Math.abs(r.excessRmb).toLocaleString()}`}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )
        })()}

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
