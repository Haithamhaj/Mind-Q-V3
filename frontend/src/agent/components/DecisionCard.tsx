type Decision = {
  title: string
  impact: string
  options: Array<{ label: string; next: string }>
}

export default function DecisionCard({ decision }: { decision: Decision }) {
  return (
    <div className="border rounded p-3 bg-white">
      <div className="font-semibold">{decision.title}</div>
      <div className="text-xs text-gray-600 mb-2">Impact: {decision.impact}</div>
      <div className="flex gap-2 flex-wrap">
        {decision.options?.map((o, i) => (
          <span key={i} className="px-2 py-1 text-xs bg-gray-100 rounded border" title={o.next}>
            {o.label}
          </span>
        ))}
      </div>
    </div>
  )
}
