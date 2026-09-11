import { ArrowUpRight, ArrowDownRight } from 'lucide-react'
import { cn } from '../lib/utils'

function StatCard({ label, value, trend, className: cnExtra }) {
  return (
    <div className={cn("stat-card", cnExtra)}>
      <span className="label">{label}</span>
      <span className="value">{value}</span>
      {trend != null ? (
        <span className={cn('text-xs font-medium', trend >= 0 ? 'text-up' : 'text-down')}>
          {trend >= 0 ? <ArrowUpRight className="inline w-3 h-3" /> : <ArrowDownRight className="inline w-3 h-3" />}
          {Math.abs(trend)}%
        </span>
      ) : null}
    </div>
  )
}

function Field({ label, children, hint }) {
  return (
    <label className="field-card">
      <span className="field-label">{label}</span>
      {children}
      {hint ? <span className="field-hint">{hint}</span> : null}
    </label>
  )
}

function SectionCard({ title, subtitle, children, className = '' }) {
  return (
    <section className={cn('glass-card animate-fade-in', className)}>
      <div className="section-head">
        <h3>{title}</h3>
        {subtitle ? <span>{subtitle}</span> : null}
      </div>
      {children}
    </section>
  )
}

function MacroCard({ label, value, hint, format = 'number', threshold, annotation, src }) {
  const display = value == null ? '--'
    : format === 'price' ? Number(value).toFixed(4)
    : format === 'percent' ? Number(value).toFixed(2) + '%'
    : format === 'pmi' ? Number(value).toFixed(1)
    : Number(value).toFixed(2)

  let colorClass = 'text-muted'
  if (value != null) {
    if (threshold != null) {
      // PMI: >=50 绿(扩张), <50 红(收缩)
      colorClass = Number(value) >= threshold ? 'text-up' : 'text-down'
    } else if (format === 'percent') {
      // 利率/收益率：中性显示（不高不低），不染色
      colorClass = ''
    } else if (format === 'price') {
      // 汇率：不染色
      colorClass = ''
    }
  }

  return (
    <div className="stat-card macro-card">
      <div className="macro-card-header">
        <span className="macro-card-label">{label}</span>
        {src && src !== '中行折算价' && (
          <span className={cn('macro-src-tag', (src.startsWith('缓存') || src.startsWith('静态')) ? 'stale' : '')}>{src}</span>
        )}
      </div>
      <div className={cn('macro-card-value', colorClass)}>{display}</div>
      {hint && <div className="macro-card-hint">{hint}</div>}
      {annotation && <div className="macro-card-annotation">{annotation}</div>}
    </div>
  )
}

export { StatCard, Field, SectionCard, MacroCard }
