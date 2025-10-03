import * as React from "react"

interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: "default" | "outline"
}

export function Badge({ variant = "default", className = "", ...props }: BadgeProps) {
  const base = "inline-flex items-center rounded px-2 py-0.5 text-xs font-medium"
  const variants = {
    default: "bg-gray-200 text-gray-800",
    outline: "border border-gray-300 text-gray-700"
  }
  return <span className={[base, variants[variant], className].join(" ")} {...props} />
}

export default Badge



