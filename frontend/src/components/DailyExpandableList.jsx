import { useState } from 'react'
import { cn } from '../lib/utils'

function DailyExpandableList({ rows, onCodeClick }) {
  const [expanded, setExpanded] = useState(new Set())
  const num = (r, k) => { const v = r[k]; if (v == null || v === '') return null; const n = Number(v); return (isNaN(n) || !isFinite(n)) ? null : n }
  // hasNum: 兼容 num() 返回 null 的安全守卫（!isNaN(null)===true 是 JS 坑）
  const hasNum = (v) => v != null && !isNaN(v)

  // 策略颜色映射
  const STRAT_COLORS = {
    'Boll': { bg: 'hsla(229, 87%, 56%, 0.10)', text: '#6366F1', border: 'hsla(229, 87%, 56%, 0.35)' },
    'Relativity': { bg: 'hsla(157, 81%, 37%, 0.09)', text: '#30A46C', border: 'hsla(157, 81%, 37%, 0.35)' },
    'Theme': { bg: 'hsla(38, 92%, 50%, 0.10)', text: '#F59E0B', border: 'hsla(38, 92%, 50%, 0.35)' },
    'CCTV': { bg: 'hsla(3, 80%, 50%, 0.08)', text: '#E5484D', border: 'hsla(3, 80%, 50%, 0.35)' },
  }
  const getStratColor = (s) => STRAT_COLORS[s] || { bg: 'hsl(var(--surface-2))', text: 'hsl(var(--muted))', border: 'hsl(var(--border))' }

  // 评分等级色
  const scoreGrade = (s) => {
    if (s == null || isNaN(s)) return { label: '--', cls: '', bar: 0 }
    if (s >= 45) return { label: 'A+', cls: 'ds-a-plus', bar: 100 }
    if (s >= 38) return { label: 'A', cls: 'ds-a', bar: 85 }
    if (s >= 30) return { label: 'B', cls: 'ds-b', bar: 65 }
    if (s >= 20) return { label: 'C', cls: 'ds-c', bar: 40 }
    return { label: 'D', cls: 'ds-d', bar: 20 }
  }

  // 多维评分构成：用每行已有字段（买入价/止损/止盈/MA20/策略）推导 5 个可解释子维度 + 加权综合分。
  // 相比后端单一「综合评分」数字，这里把评级拆开，让用户看到每个维度如何贡献，避免「黑箱一个数」。
  const scoreBreakdown = (row) => {
    const clamp = (v, lo = 0, hi = 100) => Math.max(lo, Math.min(hi, v))
    const buy = num(row, '建议买入价')
    const latest = num(row, '最新价')
    const lower = num(row, '止损价(下轨)')
    const tp = num(row, '止盈价(上轨)')
    const ma20 = num(row, 'MA20')
    const stratStr = String(row['来源策略'] ?? '')
    const hit = Number(row['命中策略数'] ?? stratStr.split('/').filter(Boolean).length) || 1
    const hasRS = stratStr.includes('Relativity')

    // 1) 超卖深度：价格越贴近/跌破下轨，反弹赔率越好
    let oversold = 50
    if (hasNum(buy) && hasNum(lower) && lower > 0) {
      const dist = (buy - lower) / lower * 100
      if (dist <= 0) oversold = 100
      else if (dist <= 5) oversold = 100 - dist * 4        // 100 → 80
      else if (dist <= 15) oversold = 80 - (dist - 5) * 4  // 80 → 40
      else oversold = Math.max(20, 40 - (dist - 15) * 1.5)
    }
    // 2) 盈亏比：止盈空间 / 止损空间
    let rr = 50
    if (hasNum(buy) && hasNum(lower) && hasNum(tp) && lower > 0 && tp > lower) {
      const denom = buy - lower
      if (denom > 0) {
        const ratio = (tp - buy) / denom
        rr = ratio >= 3 ? 100 : ratio >= 2 ? 80 : ratio >= 1.5 ? 65 : ratio >= 1 ? 45 : 25
      }
    }
    // 3) 趋势强度：现价相对 MA20（站上均线上方更稳）
    let trend = 50
    if (hasNum(latest) && hasNum(ma20) && ma20 > 0) {
      const p = (latest - ma20) / ma20 * 100
      trend = p >= 0 ? clamp(70 + p * 2, 70, 92) : clamp(50 + p * 2, 30, 70)
    }
    // 4) 策略共振：命中策略越多越可信
    const reso = hit >= 3 ? 96 : hit === 2 ? 85 : 50
    // 5) 相对强弱：跑赢指数（Relativity）额外加分
    const rs = hasRS ? 88 : 60

    const composite = Math.round(
      oversold * 0.30 + rr * 0.25 + trend * 0.20 + reso * 0.15 + rs * 0.10
    )
    const rows = [
      { key: '超卖深度', val: Math.round(oversold), hint: '贴近下轨' },
      { key: '盈亏比', val: Math.round(rr), hint: '止盈/止损' },
      { key: '趋势强度', val: Math.round(trend), hint: '价 vs MA20' },
      { key: '策略共振', val: Math.round(reso), hint: `${hit} 策略命中` },
      { key: '相对强弱', val: Math.round(rs), hint: hasRS ? '跑赢指数' : '仅价格信号' },
    ]
    return [rows, composite]
  }

  const toggle = (i) => setExpanded((prev) => {
    const next = new Set(prev)
    if (next.has(i)) next.delete(i)
    else next.add(i)
    return next
  })
  return (
    <div className="daily-list">
      {rows.map((row, i) => {
        const code = String(row['股票代码'] ?? row['代码'] ?? '--').padStart(6, '0')
        const name = (row['股票名称'] ?? '--').trim()
        const displayName = (!name || name === '--' || name.toLowerCase() === 'nan') ? '--' : name
        const score = num(row, '综合评分')
        const strategies = row['来源策略'] ?? '--'
        const stratList = strategies.split('/').map(s => s.trim()).filter(Boolean)
        const primaryStrat = stratList[0] || '--'
        const buyPrice = num(row, '建议买入价')
        const latestP = num(row, '最新价')
        const stopP = num(row, '止损价(下轨)')
        const tpP = num(row, '止盈价(上轨)')
        const ma20V = num(row, 'MA20')
        const posPct = num(row, '建议仓位%')
        const amt = num(row, '建议金额')
        const hitCount = row['命中策略数'] ?? stratList.length

        const sg = scoreGrade(score)
        const sc = getStratColor(primaryStrat)
        const [sbRows, sbComposite] = scoreBreakdown(row)
        const sbCls = sbComposite >= 80 ? 'sb-excellent' : sbComposite >= 60 ? 'sb-good' : sbComposite >= 40 ? 'sb-mid' : 'sb-weak'
        const pnlPct = (hasNum(latestP) && hasNum(buyPrice) && buyPrice > 0) ? ((latestP / buyPrice - 1) * 100) : NaN
        const isOpen = expanded.has(i)

        // 排名奖牌
        const rankBadge = (idx) => {
          if (idx === 0) return <span className="rank-medal rank-gold">🥇</span>
          if (idx === 1) return <span className="rank-medal rank-silver">🥈</span>
          if (idx === 2) return <span className="rank-medal rank-bronze">🥉</span>
          return <span className="rank-num">{idx + 1}</span>
        }

        return (
          <div key={i} className={cn('daily-item', isOpen && 'open')} style={{ borderLeft: `3px solid ${sc.border}` }}>
            <div className="daily-summary" onClick={() => toggle(i)}>
              <div className="daily-summary-left">
                <span className="daily-expander">{isOpen ? '▼' : '▶'}</span>
                {rankBadge(i)}
                <span className="daily-code" onClick={(e) => { e.stopPropagation(); onCodeClick(code) }}>{code}</span>
                <span className="daily-name">{displayName}</span>
                {/* 策略彩色标签 */}
                <div className="strat-badges">
                  {stratList.map((s, si) => {
                    const c = getStratColor(s)
                    return <span key={si} className="strat-badge" style={{ background: c.bg, color: c.text, borderColor: c.border }}>{s}</span>
                  })}
                </div>
              </div>

              <div className="daily-summary-right">
                {/* 评级药丸 */}
                <div className={cn('score-pill', sg.cls)} title={`综合评分: ${hasNum(score) ? score.toFixed(1) : '--'}`}>
                  <span className="score-label">{sg.label}</span>
                  {!isNaN(score) && <span className="score-bar-track"><span className="score-bar-fill" style={{ width: `${sg.bar}%` }} /></span>}
                </div>

                {/* 买入价 + 盈亏 */}
                <div className="price-group">
                  <span className="daily-buy">¥{hasNum(buyPrice) ? buyPrice.toFixed(2) : '--'}</span>
                  {!isNaN(pnlPct) && (
                    <span className={cn('pnl-mini', pnlPct >= 0 ? 'pnl-up' : 'pnl-down')}>
                      {pnlPct >= 0 ? '+' : ''}{pnlPct.toFixed(2)}%
                    </span>
                  )}
                </div>

                {/* 仓位 & 金额 */}
                <div className="pos-group">
                  {!isNaN(posPct) && <span className="pos-tag">{hasNum(posPct) ? posPct.toFixed(0) : '--'}%</span>}
                  {!isNaN(amt) && <span className="amt-tag">{hasNum(amt) ? (amt >= 10000 ? `${(amt/10000).toFixed(1)}万` : `${amt.toFixed(0)}`) : '--'}</span>}
                </div>

                {/* 止损距离 */}
                {hasNum(latestP) && hasNum(stopP) && stopP > 0 && (
                  <span className={cn('stop-dist', ((latestP - stopP) / stopP * 100) < 5 ? 'stop-close' : '')}>
                    距止损 {((latestP - stopP) / stopP * 100).toFixed(1)}%
                  </span>
                )}
              </div>
            </div>
            {isOpen ? (
              <div className="daily-details">
                <div className="dd-cell"><span>命中策略数</span><strong>{hitCount}</strong></div>
                <div className="dd-cell"><span>建议仓位%</span><strong>{hasNum(posPct) ? `${posPct.toFixed(0)}%` : '--'}</strong></div>
                <div className="dd-cell"><span>建议金额</span><strong>{hasNum(amt) ? `¥${amt.toFixed(0)}` : '--'}</strong></div>
                <div className="dd-cell"><span>最新价</span><strong className={!isNaN(latestP) && !isNaN(buyPrice) && buyPrice > 0 ? (latestP >= buyPrice ? 'text-up' : 'text-down') : ''}>{latestP != null && !isNaN(latestP) ? latestP.toFixed(2) : '--'}</strong></div>
                <div className="dd-cell"><span>建议买入价</span><strong>{hasNum(buyPrice) ? buyPrice.toFixed(2) : '--'}</strong></div>
                <div className="dd-cell"><span>止损价(下轨)</span><strong className={stopP != null && !isNaN(stopP) ? 'text-down' : ''}>{stopP != null && !isNaN(stopP) ? stopP.toFixed(2) : '--'}</strong></div>
                <div className="dd-cell"><span>止盈价(上轨)</span><strong className={tpP != null && !isNaN(tpP) ? 'text-up' : ''}>{tpP != null && !isNaN(tpP) ? tpP.toFixed(2) : '--'}</strong></div>
                <div className="dd-cell"><span>MA20</span><strong>{ma20V != null && !isNaN(ma20V) ? ma20V.toFixed(2) : '--'}</strong></div>
                {/* 智能解读行 */}
                <div className="daily-insight">
                  {hasNum(latestP) && hasNum(buyPrice) && buyPrice > 0 ? (
                    <span className={cn('di-tag', latestP >= buyPrice ? 'di-profit' : 'di-loss')}>
                      相对买入价 {latestP >= buyPrice ? `+${((latestP/buyPrice-1)*100).toFixed(2)}% 盈` : `${((latestP/buyPrice-1)*100).toFixed(2)}% 亏`}
                    </span>
                  ) : null}
                  {hasNum(latestP) && hasNum(stopP) && stopP > 0 ? (
                    <span className={cn('di-tag', (latestP - stopP) / stopP * 100 < 3 ? 'di-danger' : '')}>
                      距止损 {((latestP - stopP) / stopP * 100).toFixed(2)}%
                    </span>
                  ) : null}
                  {hasNum(latestP) && latestP > 0 && hasNum(tpP) && tpP > 0 ? (
                    <span className="di-tag">距止盈 +{((tpP - latestP) / latestP * 100).toFixed(2)}%</span>
                  ) : null}
                  {hasNum(buyPrice) && hasNum(stopP) && hasNum(tpP) && buyPrice > stopP && tpP > buyPrice ? (
                    <span className="di-tag">盈亏比 {((tpP - buyPrice) / (buyPrice - stopP)).toFixed(2)}:1</span>
                  ) : null}
                  {/* 综合风险评级 */}
                  {(() => {
                    const toStop = (hasNum(latestP) && hasNum(stopP) && stopP > 0) ? (latestP - stopP) / stopP * 100 : null
                    if (toStop != null && toStop < 2) return <span className="di-risk di-danger">🔴 极高风险 — 接近止损位</span>
                    if (toStop != null && toStop < 5) return <span className="di-risk di-warn">⚠️ 高风险 — 止损较近</span>
                    if (toStop != null && toStop < 10) return <span className="di-risk di-caution">🟡 中等风险</span>
                    if (toStop != null) return <span className="di-risk di-safe">🟢 低风险 — 止损空间充足</span>
                    return null
                  })()}
                </div>
                {/* 多维评分构成 */}
                <div className="score-breakdown">
                  <div className="sb-head">
                    <span className="sb-title">评分构成（多维推导）</span>
                    <span className={cn('sb-composite', sbCls)}>加权综合 {sbComposite}</span>
                  </div>
                  {sbRows.map((d) => {
                    const vCls = d.val >= 80 ? 'sb-excellent' : d.val >= 60 ? 'sb-good' : d.val >= 40 ? 'sb-mid' : 'sb-weak'
                    return (
                      <div className="sb-row" key={d.key}>
                        <span className="sb-label">{d.key}</span>
                        <span className="sb-track"><span className={cn('sb-fill', vCls)} style={{ width: `${d.val}%` }} /></span>
                        <span className="sb-val">{d.val}</span>
                        <span className="sb-hint">{d.hint}</span>
                      </div>
                    )
                  })}
                </div>
              </div>
            ) : null}
          </div>
        )
      })}
    </div>
  )
}

export default DailyExpandableList
