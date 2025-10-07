import { useState, useEffect } from 'react'
import { AlertCircle, Brain, TrendingUp, MessageSquare } from 'lucide-react'
import InsightsPanel from '../components/InsightsPanel'
import ChatInterface from '../components/ChatInterface'

interface Recommendation {
  id: string
  title: string
  description: string
  rationale: string
  impact: string
  effort: string
  priority: number
  action_type: string
  expected_improvement: string
}

interface ExecutiveSummary {
  executive_summary: string
  key_findings: string[]
  next_steps: string[]
}

export default function LLMAnalysis() {
  const [loading, setLoading] = useState(false)
  const [analyzing, setAnalyzing] = useState(false)
  const [recommendations, setRecommendations] = useState<Recommendation[]>([])
  const [summary, setSummary] = useState<ExecutiveSummary | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'insights' | 'chat'>('insights')

  useEffect(() => {
    loadExistingData()
  }, [])

  const loadExistingData = async () => {
    try {
      setLoading(true)
      const base = (import.meta as any).env?.VITE_API_URL || 'http://localhost:8000'

      const recsResponse = await fetch(`${base}/api/v1/llm-analysis/recommendations`)
      if (recsResponse.ok) {
        const recsData = await recsResponse.json()
        setRecommendations(recsData)
      }

      const summaryResponse = await fetch(`${base}/api/v1/llm-analysis/executive-summary`)
      if (summaryResponse.ok) {
        const summaryData = await summaryResponse.json()
        setSummary(summaryData)
      }
    } catch (err) {
      // ignore
    } finally {
      setLoading(false)
    }
  }

  const runAnalysis = async () => {
    try {
      setAnalyzing(true)
      setError(null)
      const base = (import.meta as any).env?.VITE_API_URL || 'http://localhost:8000'
      const response = await fetch(`${base}/api/v1/llm-analysis/run-analysis`, { method: 'POST' })
      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || 'Analysis failed')
      }
      await loadExistingData()
      setAnalyzing(false)
    } catch (err: any) {
      setError(err.message)
      setAnalyzing(false)
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
      <div className="max-w-7xl mx-auto">
        <div className="bg-white rounded-lg shadow-lg p-6 mb-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Brain className="w-8 h-8 text-purple-600" />
              <div>
                <h1 className="text-3xl font-bold text-gray-900">LLM-Assisted Analysis</h1>
                <p className="text-gray-600 mt-1">AI-powered insights and recommendations</p>
              </div>
            </div>
            {!summary && (
              <button
                onClick={runAnalysis}
                disabled={analyzing}
                className="px-6 py-3 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:bg-gray-400 font-semibold flex items-center gap-2"
              >
                {analyzing ? (
                  <>
                    <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                    Analyzing...
                  </>
                ) : (
                  <>
                    <Brain className="w-5 h-5" />
                    Run LLM Analysis
                  </>
                )}
              </button>
            )}
          </div>
          {error && (
            <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-red-600 mt-0.5" />
              <div>
                <p className="text-red-800 font-semibold">Analysis Error</p>
                <p className="text-red-700 text-sm mt-1">{error}</p>
              </div>
            </div>
          )}
        </div>
        {loading ? (
          <div className="bg-white rounded-lg shadow-lg p-12 text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-purple-600 mx-auto"></div>
            <p className="text-gray-600 mt-4">Loading analysis...</p>
          </div>
        ) : summary ? (
          <>
            <div className="bg-white rounded-lg shadow-lg mb-6">
              <div className="flex border-b">
                <button
                  onClick={() => setActiveTab('insights')}
                  className={`flex-1 px-6 py-4 font-semibold flex items-center justify-center gap-2 ${
                    activeTab === 'insights' ? 'border-b-2 border-purple-600 text-purple-600' : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  <TrendingUp className="w-5 h-5" />
                  Insights & Recommendations
                </button>
                <button
                  onClick={() => setActiveTab('chat')}
                  className={`flex-1 px-6 py-4 font-semibold flex items-center justify-center gap-2 ${
                    activeTab === 'chat' ? 'border-b-2 border-purple-600 text-purple-600' : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  <MessageSquare className="w-5 h-5" />
                  Chat with AI
                </button>
              </div>
            </div>
            {activeTab === 'insights' ? (
              <InsightsPanel summary={summary} recommendations={recommendations} />
            ) : (
              <ChatInterface />
            )}
          </>
        ) : (
          <div className="bg-white rounded-lg shadow-lg p-12 text-center">
            <Brain className="w-16 h-16 text-gray-400 mx-auto mb-4" />
            <p className="text-gray-600 text-lg">
              No analysis available yet. Run Phase 14 first, then click "Run LLM Analysis".
            </p>
          </div>
        )}
      </div>
    </div>
  )
}


