import React, { useState, useMemo } from 'react'
import { RESCUE_BACKTEST_DATABASE } from '../data/rescueBacktestData'
import { SectionCard } from './cards'
import { Compass, BarChart3, PieChart, Layers } from 'lucide-react'

export default function QuantRescueSection() {
  const [selectedStock, setSelectedStock] = useState('TOTAL') // 'TOTAL' | '002284' | '603187' | '600269' | '603390'
  const [activeTab, setActiveTab] = useState('val') // 'val' (净值曲线) | 'cost' (成本压降曲线)
  const [hoverIdx, setHoverIdx] = useState(null)

  const db = RESCUE_BACKTEST_DATABASE || {}
  const currentStockData = db[selectedStock] || db['TOTAL'] || {}
  const data = currentStockData.series || []
  const summary = currentStockData.summary || {}

  const lastPoint = data[data.length - 1] || {}
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

  // 标的选项
  const stockOptions = [
    { id: 'TOTAL', label: '🌟 全部持仓组合总览 (Portfolio Total)', sub: '合并 4 只总资产挽救走势' },
    { id: '002284', label: '亚太股份 (002284)', sub: '成本 13.15 元 · 900 股' },
    { id: '603187', label: '海容冷链 (603187)', sub: '成本 13.65 元 · 500 股' },
    { id: '600269', label: '赣粤高速 (600269)', sub: '成本 4.51 元 · 1800 股' },
    { id: '603390', label: '通达电气 (603390)', sub: '成本 12.79 元 · 200 股' },
  ]

  return (
    <SectionCard
      title="🔬 前沿量化拯救算法全真回测中心 (Quant Rescue Lab)"
      subtitle="针对你的 4 只套牢持仓，全真回测验证三大前沿算法在单边下跌中的回血成效与成本压降走势"
    >
      {/* 标的切换 Tab 栏 */}
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

      {/* 顶部 4 大核心指标对比卡片 */}
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

      {/* 维度切换按钮与当前点位指示器 */}
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
                  {`¥${val.toFixed(selectedStock === 'TOTAL' || activeTab === 'val' ? 0 : 2)}`}
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

      {/* 说明卡片 */}
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
  )
}
