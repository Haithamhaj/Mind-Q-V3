import { useEffect, useState } from 'react'
import type { PhaseNode } from '../api/agent'
import { askQA, fetchPhase } from '../api/agent'
import DecisionCard from './DecisionCard'

export default function PhaseModal({ open, onClose, phase }: { open: boolean; onClose: () => void; phase: number }) {
  const [data, setData] = useState<PhaseNode | null>(null)
  const [tab, setTab] = useState<'logic' | 'best' | 'executed' | 'score' | 'qa'>('logic')
  const [q, setQ] = useState('')
  const [qa, setQa] = useState<{ answer: string; sources: string[] } | null>(null)

  useEffect(() => {
    if (!open) return
    fetchPhase(phase).then(setData).catch(console.error)
  }, [open, phase])

  if (!open) return null
  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50">
      <div className="bg-white w-[900px] max-w-[95vw] rounded shadow-lg p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="font-semibold">Phase {data?.phase} — {data?.name}</div>
          <div className={`text-xs px-2 py-1 rounded border ${statusColor(data?.status)}`}>{data?.status}</div>
        </div>

        <div className="flex gap-2 text-sm mb-3">
          {['logic','best','executed','score','qa'].map((t) => (
            <button key={t} onClick={() => setTab(t as any)} className={`px-2 py-1 border rounded ${tab===t?'bg-blue-600 text-white':''}`}>{t}</button>
          ))}
        </div>

        {tab === 'logic' && (
          <div className="prose max-w-none whitespace-pre-wrap text-sm">{data?.logic?.rules}</div>
        )}
        {tab === 'best' && (
          <ul className="list-disc ml-6 text-sm">
            {data?.best_practices?.map((b, i) => <li key={i}>{b}</li>)}
          </ul>
        )}
        {tab === 'executed' && (
          <div className="text-sm">
            <div className="font-medium mb-2">KPIs</div>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
              {Object.entries(data?.executed?.kpis || {}).map(([k, v]) => (
                <div key={k} className="border rounded p-2">{k}: <span className="font-mono">{Number(v).toFixed(3)}</span></div>
              ))}
            </div>
          </div>
        )}
        {tab === 'score' && (
          <div className="text-sm">
            <div className="text-3xl font-bold mb-2">{data?.goal_score?.score_pct ?? 0}%</div>
            <ul className="list-disc ml-6">
              {data?.goal_score?.components?.map((c, i) => (
                <li key={i}>{c.metric}: {c.value.toFixed(2)} × {c.weight}</li>
              ))}
            </ul>
          </div>
        )}
        {tab === 'qa' && (
          <div className="space-y-2">
            <input value={q} onChange={(e)=>setQ(e.target.value)} placeholder="Ask about this phase..." className="w-full border rounded px-2 py-1" />
            <button onClick={async ()=>{const r=await askQA(q, data?.phase); setQa(r)}} className="px-3 py-1 rounded bg-blue-600 text-white">Ask</button>
            {qa && (
              <div className="text-sm">
                <div className="mb-2">{qa.answer}</div>
                <div className="text-xs text-gray-600">Sources:</div>
                <ul className="text-xs list-disc ml-6">
                  {qa.sources.map((s,i)=>(<li key={i}><code>{s}</code></li>))}
                </ul>
              </div>
            )}
          </div>
        )}

        <div className="mt-4 flex items-center justify-between">
          <div className="flex gap-2 flex-wrap">
            {data?.decisions?.map((d, i) => (<DecisionCard key={i} decision={d as any} />))}
          </div>
          <button className="px-3 py-1 rounded border" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  )
}

function statusColor(s?: string) {
  switch (s) {
    case 'PASS': return 'bg-green-50 text-green-700 border-green-200'
    case 'WARN': return 'bg-yellow-50 text-yellow-700 border-yellow-200'
    case 'STOP': return 'bg-red-50 text-red-700 border-red-200'
    default: return 'bg-gray-50 text-gray-700 border-gray-200'
  }
}
