import React, { useState, useEffect, useMemo } from 'react'
import { fetchTradesFromCloud, insertTradeToCloud, deleteTradeFromCloud } from '../lib/supabaseTrades'
import { fetchLiveStockQuotes } from '../lib/realtimeQuotes'
import { Button } from './ui/button'
import { SectionCard, StatCard } from './cards'

export default function TradeManager({ onTradeChange }) {
  const [trades, setTrades] = useState([])
  const [liveQuotes, setLiveQuotes] = useState({})
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState('')

  const [form, setForm] = useState({
    date: new Date().toISOString().slice(0, 10),
    code: '',
    name: '',
    side: 'buy',
    price: '',
    qty: '',
    fee: '0',
    notes: '',
  })

  const loadTrades = async () => {
    setLoading(true)
    setError('')
    try {
      const data = await fetchTradesFromCloud()
      setTrades(data || [])
      const codes = Array.from(new Set((data || []).map((t) => String(t.code || '').padStart(6, '0')).filter((c) => /^\d{6}$/.test(c))))
      if (codes.length > 0) {
        fetchLiveStockQuotes(codes).then((q) => q && setLiveQuotes(q)).catch(() => {})
      }
    } catch (err) {
      setError(err.message || '加载交易记录失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadTrades()
  }, [])

  const handleSubmit = async (e) => {
    if (e) e.preventDefault()
    if (!form.code) {
      setError('请输入股票代码')
      return
    }
    if (!form.price || Number(form.price) <= 0) {
      setError('请输入有效价格')
      return
    }
    if (!form.qty || Number(form.qty) <= 0) {
      setError('请输入有效数量')
      return
    }

    setSaving(true)
    setError('')
    try {
      await insertTradeToCloud(form)
      setSuccess('交易记录已成功写入 Supabase 云端！')
      setForm((prev) => ({
        ...prev,
        code: '',
        name: '',
        price: '',
        qty: '',
        notes: '',
      }))
      await loadTrades()
      if (onTradeChange) onTradeChange()
      setTimeout(() => setSuccess(''), 4000)
    } catch (err) {
      setError(err.message || '写入交易失败')
    } finally {
      setSaving(false)
    }
  }

  const handleDelete = async (id, code, name) => {
    if (!window.confirm(`确定要删除 ${name || code} (ID: ${id}) 的交易记录吗？`)) return
    try {
      await deleteTradeFromCloud(id)
      setSuccess(`交易记录 ID ${id} 已删除`)
      await loadTrades()
      if (onTradeChange) onTradeChange()
      setTimeout(() => setSuccess(''), 3000)
    } catch (err) {
      setError(err.message || '删除交易失败')
    }
  }

  // 计算持仓、摊薄成本与实盘净盈亏统计（买入投入 - 卖出回笼 + 手续费 vs 实时市值）
  const buyTrades = trades.filter((t) => String(t.side).toUpperCase() === 'BUY')
  const sellTrades = trades.filter((t) => String(t.side).toUpperCase() === 'SELL')
  const totalVolume = trades.reduce((sum, t) => sum + (Number(t.price) * Number(t.quantity)), 0)

  const pnlStats = useMemo(() => {
    const byCode = {}
    const orderedTrades = [...trades].sort((a, b) =>
      String(a.trade_date || a.date || '').localeCompare(String(b.trade_date || b.date || ''))
    )
    for (const t of orderedTrades) {
      const code = String(t.code || '').padStart(6, '0')
      if (!code) continue
      if (!byCode[code]) {
        byCode[code] = { code, name: t.name || code, qty: 0, buyAmt: 0, sellAmt: 0, fee: 0, initQty: 0, initAmt: 0, tradeCount: 0 }
      }
      if (t.name) byCode[code].name = t.name
      const q = Number(t.quantity || 0)
      const p = Number(t.price || 0)
      const f = Number(t.fee || 0)
      byCode[code].fee += f
      byCode[code].tradeCount += 1
      if (String(t.side || '').toUpperCase() === 'BUY') {
        if (byCode[code].tradeCount === 1) {
          byCode[code].initQty = q
          byCode[code].initAmt = q * p
        }
        byCode[code].qty += q
        byCode[code].buyAmt += q * p
      } else {
        byCode[code].qty -= q
        byCode[code].sellAmt += q * p
      }
    }
    const rows = Object.values(byCode).map((r) => {
      const livePx = Number(liveQuotes[r.code]?.price || 0)
      const mktVal = livePx > 0 ? r.qty * livePx : 0
      const netCostTotal = r.buyAmt - r.sellAmt + r.fee
      const dilutedCost = r.qty > 0 ? netCostTotal / r.qty : 0
      const totalPnl = livePx > 0 ? (mktVal + r.sellAmt - r.buyAmt - r.fee) : null
      const pnlPct = totalPnl !== null && r.buyAmt > 0 ? (totalPnl / r.buyAmt) * 100 : null
      const holdPnl = livePx > 0 && r.initQty > 0 ? (r.initQty * livePx - r.initAmt) : null
      const stratAlphaRmb = (totalPnl !== null && holdPnl !== null && r.tradeCount > 1)
        ? +(totalPnl - holdPnl).toFixed(0)
        : null
      return { ...r, livePx, mktVal, netCostTotal, dilutedCost, totalPnl, pnlPct, stratAlphaRmb }
    })
    const totalMktVal = rows.reduce((s, r) => s + r.mktVal, 0)
    const totalFee = rows.reduce((s, r) => s + r.fee, 0)
    const totalPnl = rows.every((r) => r.totalPnl !== null || r.qty === 0)
      ? rows.reduce((s, r) => s + (r.totalPnl ?? (r.sellAmt - r.buyAmt - r.fee)), 0)
      : null
    return { rows, totalMktVal, totalFee, totalPnl }
  }, [trades, liveQuotes])

  return (
    <div className="trade-manager animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* 顶部状态与云端提示 */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '14px 18px', background: 'hsl(var(--card))', borderRadius: 12,
        border: '1px solid hsl(var(--border))', flexWrap: 'wrap', gap: 12
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{
            display: 'inline-block', width: 10, height: 10, borderRadius: '50%',
            background: '#16a34a', boxShadow: '0 0 6px #16a34a'
          }} />
          <div>
            <strong style={{ fontSize: 14 }}>Supabase 云端直连数据库</strong>
            <span style={{ fontSize: 12, color: 'hsl(var(--muted))', marginLeft: 8 }}>
              Cloudflare Pages 边缘直连 · 实时持久化
            </span>
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <Button variant="outline" onClick={loadTrades} disabled={loading}>
            {loading ? '刷新中…' : '↻ 刷新云端数据'}
          </Button>
        </div>
      </div>

      {/* 状态统计卡 */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard label="总记录笔数" value={trades.length} className="stat-neutral" />
        <StatCard label="买入 / 卖出" value={`${buyTrades.length} / ${sellTrades.length}`} className="stat-neutral" />
        <StatCard
          label="当前总持仓市值"
          value={pnlStats.totalMktVal > 0 ? `¥${pnlStats.totalMktVal.toFixed(0)}` : `¥${(totalVolume / 10000).toFixed(2)}万`}
          className="stat-neutral"
        />
        <StatCard
          label="账户净盈亏 (含做T/扣费)"
          value={pnlStats.totalPnl !== null ? `${pnlStats.totalPnl >= 0 ? '+' : '-'}¥${Math.abs(pnlStats.totalPnl).toFixed(0)}` : '计算中…'}
          className={pnlStats.totalPnl !== null && pnlStats.totalPnl >= 0 ? 'stat-good' : 'stat-bad'}
        />
      </div>

      {/* 实盘净盈亏与做 T 降本明细表 */}
      {pnlStats.rows.length > 0 && (
        <SectionCard
          title="💰 实盘净盈亏与做 T 降本统计 (实时市值 + 卖出回笼 - 买入投入 - 实扣手续费)"
          subtitle={`累计实扣手续费 ¥${pnlStats.totalFee.toFixed(2)} · 每次手机成交后在此录入一笔，摊薄成本与净盈亏自动重算`}
        >
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13, textAlign: 'left' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid hsl(var(--border))', color: 'hsl(var(--muted-foreground))' }}>
                  <th style={{ padding: '8px' }}>标的</th>
                  <th style={{ padding: '8px' }}>剩余股数</th>
                  <th style={{ padding: '8px' }}>现价 / 市值</th>
                  <th style={{ padding: '8px' }}>摊薄保本成本</th>
                  <th style={{ padding: '8px' }}>累计卖出回笼</th>
                  <th style={{ padding: '8px' }}>实扣手续费</th>
                  <th style={{ padding: '8px' }}>策略增厚 (vs 死扛)</th>
                  <th style={{ padding: '8px' }}>净盈亏 (元/%)</th>
                </tr>
              </thead>
              <tbody>
                {pnlStats.rows.map((r) => (
                  <tr key={r.code} style={{ borderBottom: '1px solid hsl(var(--border) / 0.45)' }}>
                    <td style={{ padding: '8px', fontWeight: 600 }}>{r.name} <span style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))' }}>{r.code}</span></td>
                    <td style={{ padding: '8px' }}>{r.qty} 股</td>
                    <td style={{ padding: '8px' }}>{r.livePx > 0 ? `¥${r.livePx.toFixed(2)} / ¥${r.mktVal.toFixed(0)}` : '--'}</td>
                    <td style={{ padding: '8px', fontWeight: 600 }}>{r.qty > 0 ? `¥${r.dilutedCost.toFixed(2)}` : '已清仓'}</td>
                    <td style={{ padding: '8px', color: '#2563eb' }}>{r.sellAmt > 0 ? `+¥${r.sellAmt.toFixed(0)}` : '¥0'}</td>
                    <td style={{ padding: '8px', color: 'hsl(var(--muted-foreground))' }}>¥{r.fee.toFixed(2)}</td>
                    <td style={{ padding: '8px', fontWeight: 600, color: r.stratAlphaRmb === null ? 'hsl(var(--muted-foreground))' : (r.stratAlphaRmb >= 0 ? '#16a34a' : '#d97706') }}>
                      {r.stratAlphaRmb === null ? '¥0 (未做T)' : `${r.stratAlphaRmb >= 0 ? '+' : '-'}¥${Math.abs(r.stratAlphaRmb)}`}
                    </td>
                    <td style={{ padding: '8px', fontWeight: 700, color: r.totalPnl !== null && r.totalPnl >= 0 ? '#16a34a' : '#ef4444' }}>
                      {r.totalPnl !== null ? `${r.totalPnl >= 0 ? '+' : '-'}¥${Math.abs(r.totalPnl).toFixed(0)} (${r.pnlPct !== null ? `${r.pnlPct >= 0 ? '+' : ''}${r.pnlPct.toFixed(1)}%` : '--'})` : '--'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </SectionCard>
      )}

      {/* 消息提示 */}
      {error && (
        <div style={{
          padding: '10px 16px', background: 'rgba(239, 68, 68, 0.12)', color: '#ef4444',
          borderRadius: 8, fontSize: 13, border: '1px solid rgba(239, 68, 68, 0.25)'
        }}>
          ⚠️ {error}
        </div>
      )}
      {success && (
        <div style={{
          padding: '10px 16px', background: 'rgba(34, 197, 94, 0.12)', color: '#16a34a',
          borderRadius: 8, fontSize: 13, border: '1px solid rgba(34, 197, 94, 0.25)'
        }}>
          ✅ {success}
        </div>
      )}

      {/* 录入新交易表单 */}
      <SectionCard title="录入新交易" subtitle="保存后将直接同步至 Supabase 云端数据库">
        <form onSubmit={handleSubmit} style={{
          display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))',
          gap: 12, alignItems: 'end'
        }}>
          <div>
            <label className="field-label" style={{ fontSize: 12, color: 'hsl(var(--muted))', display: 'block', marginBottom: 4 }}>交易日期</label>
            <input
              type="date"
              value={form.date}
              className="h-9 w-full rounded-lg bg-card border border-border px-3 text-foreground text-sm"
              onChange={(e) => setForm((p) => ({ ...p, date: e.target.value }))}
            />
          </div>
          <div>
            <label className="field-label" style={{ fontSize: 12, color: 'hsl(var(--muted))', display: 'block', marginBottom: 4 }}>股票代码</label>
            <input
              placeholder="如 600519"
              value={form.code}
              className="h-9 w-full rounded-lg bg-card border border-border px-3 text-foreground text-sm font-mono"
              onChange={(e) => setForm((p) => ({ ...p, code: e.target.value }))}
            />
          </div>
          <div>
            <label className="field-label" style={{ fontSize: 12, color: 'hsl(var(--muted))', display: 'block', marginBottom: 4 }}>股票名称</label>
            <input
              placeholder="选填"
              value={form.name}
              className="h-9 w-full rounded-lg bg-card border border-border px-3 text-foreground text-sm"
              onChange={(e) => setForm((p) => ({ ...p, name: e.target.value }))}
            />
          </div>
          <div>
            <label className="field-label" style={{ fontSize: 12, color: 'hsl(var(--muted))', display: 'block', marginBottom: 4 }}>方向</label>
            <select
              value={form.side}
              className="h-9 w-full rounded-lg bg-card border border-border px-3 text-foreground text-sm"
              onChange={(e) => setForm((p) => ({ ...p, side: e.target.value }))}
            >
              <option value="buy">买入 (BUY)</option>
              <option value="sell">卖出 (SELL)</option>
            </select>
          </div>
          <div>
            <label className="field-label" style={{ fontSize: 12, color: 'hsl(var(--muted))', display: 'block', marginBottom: 4 }}>价格 (元)</label>
            <input
              type="number"
              step="0.01"
              placeholder="0.00"
              value={form.price}
              className="h-9 w-full rounded-lg bg-card border border-border px-3 text-foreground text-sm font-mono"
              onChange={(e) => setForm((p) => ({ ...p, price: e.target.value }))}
            />
          </div>
          <div>
            <label className="field-label" style={{ fontSize: 12, color: 'hsl(var(--muted))', display: 'block', marginBottom: 4 }}>数量 (股)</label>
            <input
              type="number"
              step="100"
              placeholder="100"
              value={form.qty}
              className="h-9 w-full rounded-lg bg-card border border-border px-3 text-foreground text-sm font-mono"
              onChange={(e) => setForm((p) => ({ ...p, qty: e.target.value }))}
            />
          </div>
          <div>
            <label className="field-label" style={{ fontSize: 12, color: 'hsl(var(--muted))', display: 'block', marginBottom: 4 }}>手续费 (元)</label>
            <input
              type="number"
              step="0.1"
              placeholder="0.00"
              value={form.fee}
              className="h-9 w-full rounded-lg bg-card border border-border px-3 text-foreground text-sm font-mono"
              onChange={(e) => setForm((p) => ({ ...p, fee: e.target.value }))}
            />
          </div>
          <div>
            <label className="field-label" style={{ fontSize: 12, color: 'hsl(var(--muted))', display: 'block', marginBottom: 4 }}>备注</label>
            <input
              placeholder="选填"
              value={form.notes}
              className="h-9 w-full rounded-lg bg-card border border-border px-3 text-foreground text-sm"
              onChange={(e) => setForm((p) => ({ ...p, notes: e.target.value }))}
            />
          </div>
          <div>
            <Button type="submit" disabled={saving} style={{ width: '100%', height: 36 }}>
              {saving ? '保存中…' : '＋ 添加记录'}
            </Button>
          </div>
        </form>
      </SectionCard>

      {/* 交易记录列表 */}
      <SectionCard title="云端真实交易记录" subtitle={`共 ${trades.length} 笔记录`}>
        {loading ? (
          <div style={{ textAlign: 'center', padding: '36px 0', color: 'hsl(var(--muted))' }}>
            正在拉取云端记录…
          </div>
        ) : trades.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '36px 0', color: 'hsl(var(--muted))' }}>
            云端数据库暂无交易记录，可通过上方表单直接添加
          </div>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', fontSize: 13, borderCollapse: 'collapse', textAlign: 'left' }}>
              <thead>
                <tr style={{ color: 'hsl(var(--muted))', borderBottom: '1px solid hsl(var(--border))' }}>
                  <th style={{ padding: '8px 10px' }}>日期</th>
                  <th style={{ padding: '8px 10px' }}>代码</th>
                  <th style={{ padding: '8px 10px' }}>名称</th>
                  <th style={{ padding: '8px 10px' }}>方向</th>
                  <th style={{ padding: '8px 10px' }}>价格</th>
                  <th style={{ padding: '8px 10px' }}>数量</th>
                  <th style={{ padding: '8px 10px' }}>金额</th>
                  <th style={{ padding: '8px 10px' }}>手续费</th>
                  <th style={{ padding: '8px 10px' }}>备注</th>
                  <th style={{ padding: '8px 10px', textAlign: 'right' }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {trades.map((t) => {
                  const isBuy = String(t.side).toUpperCase() === 'BUY'
                  const amt = Number(t.price) * Number(t.quantity)
                  return (
                    <tr key={t.id} style={{ borderBottom: '1px solid hsl(var(--border))' }}>
                      <td style={{ padding: '10px', whiteSpace: 'nowrap' }}>{t.trade_date}</td>
                      <td style={{ padding: '10px', fontFamily: 'monospace', fontWeight: 600 }}>{t.code}</td>
                      <td style={{ padding: '10px' }}>{t.name || '--'}</td>
                      <td style={{ padding: '10px' }}>
                        <span style={{
                          display: 'inline-block', padding: '2px 8px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                          background: isBuy ? 'rgba(239, 68, 68, 0.12)' : 'rgba(34, 197, 94, 0.12)',
                          color: isBuy ? '#dc2626' : '#16a34a'
                        }}>
                          {isBuy ? '买入' : '卖出'}
                        </span>
                      </td>
                      <td style={{ padding: '10px', fontFamily: 'monospace' }}>¥{Number(t.price).toFixed(2)}</td>
                      <td style={{ padding: '10px', fontFamily: 'monospace' }}>{t.quantity}</td>
                      <td style={{ padding: '10px', fontFamily: 'monospace' }}>¥{amt.toFixed(2)}</td>
                      <td style={{ padding: '10px', color: 'hsl(var(--muted))' }}>¥{Number(t.fee || 0).toFixed(2)}</td>
                      <td style={{ padding: '10px', color: 'hsl(var(--muted))', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {t.notes || '--'}
                      </td>
                      <td style={{ padding: '10px', textAlign: 'right' }}>
                        <button
                          onClick={() => handleDelete(t.id, t.code, t.name)}
                          style={{
                            background: 'transparent', border: '1px solid hsl(var(--border))',
                            color: '#ef4444', padding: '4px 10px', borderRadius: 6,
                            cursor: 'pointer', fontSize: 12
                          }}
                        >
                          删除
                        </button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </SectionCard>
    </div>
  )
}
