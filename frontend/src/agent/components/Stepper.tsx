import React from 'react'

export type StepperProps = {
  phases: Array<{ id: number; name: string }>
  current?: number
  onSelect?: (id: number) => void
}

export default function Stepper({ phases, current, onSelect }: StepperProps) {
  return (
    <div className="flex flex-wrap gap-2">
      {phases.map((p) => (
        <button
          key={p.id}
          onClick={() => onSelect?.(p.id)}
          className={`px-3 py-1 rounded text-sm border ${current === p.id ? 'bg-blue-600 text-white' : 'bg-white'} hover:bg-blue-50`}
          title={p.name}
        >
          {p.id}
        </button>
      ))}
    </div>
  )
}
