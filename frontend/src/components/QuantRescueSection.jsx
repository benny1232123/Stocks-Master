import React, { useState, useMemo } from 'react'
import { RESCUE_BACKTEST_DATA } from '../data/rescueBacktestData'
import { SectionCard, StatCard } from './cards'
import { TrendingUp, ShieldAlert, Award, Compass, ArrowDownRight, Layers } from 'lucide-react'

export default function QuantRescueSection() {
  const [activeTab, setActiveTab] = useState('val') // 'val' (净值曲线) | 'cost' (成本压降曲线)
  const [hoverIdx, setHoverIdx] = useState(null)

  const data = RESCUE_BACKTEST_DATA || []
  const lastPoint = data[data.length - 1] || {}
  const firstPoint = data[0] || {}

  // 悬停或者最新的点
  const current = hoverIdx !== null && data[hoverIdx] ? data[hoverIdx] : lastPoint

  // SVG 宽高
  const svgWidth = 850
  const svgHeight = 280
  const padLeft = 65
  const padRight = 30
  const padTop = 25
  const padBottom = 35

  const chartW = svgWidth - padLeft - padRight
  const chartH = svgHeight - padTop - padBottom

  // 计算 Y 轴范围
  const yBounds = useMemo(() => {
    if (!data.length) return { min: 0, max: 100 }
    if (activeTab === 'val') {
      const allVals = data.flatMap(d => [d.bench_val, d.gueant_val, d.fernholz_val, d.jev_val])
      const minV = Math.floor(Math.min(...allVals) / 500) * 500
      const maxV = Math.ceil(Math.max(...allVals) / 500) * 500
      return { min: minV, max: maxV }
    } else {
      const allCosts = data.flatMap(d => [d.bench_cost, d.gueant_cost, d.fernholz_cost, d.jev_cost])
      const minC = Math.floor(Math.min(...allCosts))
      const maxC = Math.ceil(Math.max(...allCosts)) + 0.5
      return { min: minC, max: maxC }
    }
  }, [data, activeTab])

  const getX = (i) => padLeft + (i / (data.length - 1)) * chartW
  const getY = (val) => {
    const norm = (val - yBounds.min) / (yBounds.max - yBounds.min || 1)
    return padTop + (1 - norm) * chartH
  }

  // 生成 SVG 路径
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

  return (
    <SectionCard
      title="🔬 前沿量化拯救算法全真回测中心 (Quant Rescue Lab)"
      subtitle="以被套最深的【亚太股份 002284】(成本 13.15 元) 为全真标的，对比三大前沿算法在 283 天单边下跌中的回血成效"
    >
      {/* 顶部核心指标对比卡片 */}
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
            保本成本: <strong>13.15 元</strong> (回本需大涨 +55.8%)
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
            保本成本降至: <strong>{lastPoint.gueant_cost} 元</strong> (挽回 +1,654 元)
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
            保本成本降至: <strong>{lastPoint.fernholz_cost} 元</strong> (收益持续增强)
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
            回撤大幅锁死: <strong>仅亏 -10.3%</strong> (超额 +25.5%)
          </div>
        </div>
      </div>

      {/* 视图切换按钮与当前点位指示器 */}
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
            📉 每股实际保本成本压降 (元/股)
          </button>
        </div>

        <div style={{ fontSize: 13, color: 'hsl(var(--muted-foreground))' }}>
          观测日期: <strong>{current.date}</strong> | 股票现价: <strong>¥{current.price}</strong> |
          {activeTab === 'val' ? (
            <span> 拯救挽回: <strong style={{ color: '#10b981' }}>+¥{(current.gueant_val - current.bench_val).toFixed(0)}</strong></span>
          ) : (
            <span> 成本压降: <strong style={{ color: '#3b82f6' }}>-¥{(13.15 - current.gueant_cost).toFixed(2)}/股</strong></span>
          )}
        </div>
      </div>

      {/* SVG 曲线图表 */}
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
          {/* 背景参考线 */}
          {[0, 0.25, 0.5, 0.75, 1].map((pct, idx) => {
            const y = padTop + (1 - pct) * chartH
            const val = yBounds.min + pct * (yBounds.max - yBounds.min)
            return (
              <g key={idx}>
                <line x1={padLeft} y1={y} x2={padLeft + chartW} y2={y} stroke="hsl(var(--border))" strokeDasharray="3 3" opacity={0.6} />
                <text x={padLeft - 8} y={y + 4} textAnchor="end" fontSize="10" fill="hsl(var(--muted-foreground))">
                  {activeTab === 'val' ? `¥${val.toFixed(0)}` : `¥${val.toFixed(2)}`}
                </text>
              </g>
            )
          })}

          {/* X 轴日期标注 */}
          {[0, Math.floor(data.length * 0.33), Math.floor(data.length * 0.66), data.length - 1].map((idx) => {
            if (!data[idx]) return null
            const x = getX(idx)
            return (
              <text key={idx} x={x} y={svgHeight - 12} textAnchor="middle" fontSize="10" fill="hsl(var(--muted-foreground))">
                {data[idx].date}
              </text>
            )
          })}

          {/* 曲线 */}
          <path d={pathBench} fill="none" stroke="#ef4444" strokeWidth="2.2" strokeDasharray="4 4" opacity={0.8} />
          <path d={pathFernholz} fill="none" stroke="#10b981" strokeWidth="2.2" />
          <path d={pathGueant} fill="none" stroke="#3b82f6" strokeWidth="2.8" />
          <path d={pathJev} fill="none" stroke="#a855f7" strokeWidth="2.5" />

          {/* 悬停光标十字线 */}
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

      {/* 图例说明与前沿论文解读 */}
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
          <Compass size={15} /> 论文算法与实战启示说明：
        </div>
        <div>
          • <span style={{ color: '#ef4444', fontWeight: 600 }}>散户死扛基准 (红色虚线)</span>：单边熊市中亏损持续放大至 -35.8%，保本成本永远锁死在 13.15 元，回本必须等待股价未来暴涨 +55.8%。
        </div>
        <div>
          • <span style={{ color: '#3b82f6', fontWeight: 600 }}>Guéant (2023) 非对称最优做 T 算法 (蓝色实线)</span>：根据真实波幅 ATR 非对称挂单（卖单窄 +0.8 ATR，买单宽 -1.3 ATR），从日内震荡中累计榨取 <strong>1,654 元纯现金</strong>，把实际保本点从 13.15 降至 <strong>11.31 元</strong>，回本门槛直接降低 40%！
        </div>
        <div>
          • <span style={{ color: '#10b981', fontWeight: 600 }}>Fernholz SPT 波动率收割 + ETF 对冲 (绿色实线)</span>：套利现金不闲置，定投 510300 沪深300 ETF，享受指数稳健复利。
        </div>
        <div>
          • <span style={{ color: '#a855f7', fontWeight: 600 }}>World Model & JEV 强化学习自适应置换 (紫色实线)</span>：在破位初期果断将 400 股机动仓在反弹高点置换至高 Alpha 因子池，最终亏损仅 -10.3%，最大程度保全本金。
        </div>
      </div>
    </SectionCard>
  )
}
