import { cva } from 'class-variance-authority'
import { cn } from '../../lib/utils'

const buttonVariants = cva(
  'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-lg text-sm font-semibold transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-40',
  {
    variants: {
      variant: {
        default: 'bg-primary text-primary-foreground hover:bg-primary/90 shadow-[0_4px_12px_rgba(0,0,0,0.08)]',
        secondary: 'bg-white text-foreground border border-[rgba(0,0,0,0.1)] hover:bg-[#F5F5F5] hover:border-[rgba(0,0,0,0.18)]',
        ghost: 'text-muted hover:bg-black/[0.04] hover:text-foreground',
        outline: 'border border-[rgba(0,0,0,0.12)] bg-white text-foreground hover:border-[#1a1a1a]',
        destructive: 'bg-destructive text-destructive-foreground hover:bg-destructive/90 shadow-[0_4px_12px_rgba(0,0,0,0.08)]',
        gold: 'bg-primary text-primary-foreground hover:bg-primary/90',
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
