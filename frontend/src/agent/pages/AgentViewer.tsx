import { useEffect, useState } from 'react'
import Stepper from '../components/Stepper'
import PhaseModal from '../components/PhaseModal'
import { fetchGraph } from '../api/agent'

export default function AgentViewer() {
  const [phases, setPhases] = useState<Array<{ id: number; name: string }>>([])
  const [sel, setSel] = useState<number | null>(null)

  useEffect(() => {
    fetchGraph().then((g) => {
      const list = g.phases || []
      setPhases(list.map((p: any) => ({ id: p.id, name: p.name })))
    }).catch(console.error)
  }, [])

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-xl font-semibold">Agent Read‑only Phase Viewer</h1>
      <Stepper phases={phases} current={sel ?? undefined} onSelect={setSel} />

      <div className="grid md:grid-cols-2 gap-3">
        {phases.map((p) => (
          <div key={p.id} className="border rounded p-3 bg-white flex items-center justify-between">
            <div>
              <div className="font-medium">{p.id} — {p.name}</div>
            </div>
            <button className="px-3 py-1 rounded bg-blue-600 text-white" onClick={() => setSel(p.id)}>Open</button>
          </div>
        ))}
      </div>

      <PhaseModal open={sel !== null} phase={sel ?? 0} onClose={() => setSel(null)} />
    </div>
  )
}
