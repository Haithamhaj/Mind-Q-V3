import axios from 'axios'

const API_BASE_URL = (import.meta as any).env?.VITE_API_URL || 'http://127.0.0.1:8000'

export const agentApi = axios.create({
  baseURL: `${API_BASE_URL}/agent`,
  headers: { 'Content-Type': 'application/json' },
  timeout: 120000,
})

export type PhaseNode = {
  phase: number
  name: string
  status: 'READY' | 'PASS' | 'WARN' | 'STOP' | 'RUNNING'
  timer_s: number
  requires: Array<any>
  unlocks: Array<any>
  logic: { purpose?: string; rules?: string }
  best_practices: string[]
  executed: { kpis?: Record<string, number>; details?: any; artifacts?: string[] }
  goal_score?: { score_pct: number; components: Array<{ metric: string; value: number; weight: number }> }
  decisions: Array<any>
}

export async function fetchGraph() {
  const { data } = await agentApi.get('/graph')
  return data
}

export async function fetchPhase(phase: number) {
  const { data } = await agentApi.get<PhaseNode>('/phase', { params: { phase } })
  return data
}

export async function askQA(question: string, phase?: number) {
  const { data } = await agentApi.post('/qa', { question, phase })
  return data as { answer: string; sources: string[] }
}
