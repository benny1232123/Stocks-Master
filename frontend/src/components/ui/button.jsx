import { cva } from 'class-variance-authority'
import { cn } from '../../lib/utils'

const buttonVariants = cva(
  'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-lg text-sm font-semibold transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-40',
  {
    variants: {
      variant: {
        default: 'bg-primary text-primary-foreground hover:bg-primary/90 shadow-[0_2px_8px_hsla(0,72%,51%,0.3)]',
        secondary: 'bg-card text-foreground border border-border hover:bg-card/80 hover:border-primary/20',
        ghost: 'text-muted hover:bg-white/[0.04] hover:text-foreground',
        outline: 'border border-border bg-transparent text-foreground hover:bg-white/[0.04]',
        destructive: 'bg-destructive text-destructive-foreground hover:bg-destructive/90 shadow-[0_2px_8px_hsla(152,60%,42%,0.25)]',
        gold: 'bg-accent text-accent-foreground hover:bg-accent/90 shadow-[0_2px_8px_hsla(38,80%,55%,0.3)]',
      },
      size: {
        default: 'h-10 px-4 py-2',
        sm: 'h-8 rounded-md px-3 text-xs',
        lg: 'h-11 rounded-lg px-6',
        icon: 'h-9 w-9',
      },
    },
    defaultVariants: {
      variant: 'default',
      size: 'default',
    },
  }
)

function Button({ className, variant, size, ...props }) {
  return (
    <button
      className={cn(buttonVariants({ variant, size, className }))}
      {...props}
    />
  )
}

export { Button }
