import React from 'react'
import { TrendingUp, AlertTriangle, CheckCircle2 } from 'lucide-react'
import RecommendationCard from './RecommendationCard'

interface InsightsPanelProps {
  summary: { executive_summary: string; key_findings: string[]; next_steps: string[] }
  recommendations: any[]
}

export default function InsightsPanel({ summary, recommendations }: InsightsPanelProps) {
  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg shadow-lg p-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-4 flex items-center gap-2">
          <TrendingUp className="w-6 h-6 text-purple-600" />
          Executive Summary
        </h2>
        <div className="prose max-w-none">
          <div
            className="text-gray-700 whitespace-pre-wrap"
            dangerouslySetInnerHTML={{ __html: summary.executive_summary.replace(/\n/g, '<br/>') }}
          />
        </div>
      </div>

      <div className="bg-white rounded-lg shadow-lg p-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-4 flex items-center gap-2">
          <CheckCircle2 className="w-6 h-6 text-green-600" />
          Key Findings
        </h2>
        <ul className="space-y-3">
          {summary.key_findings.map((finding, idx) => (
            <li key={idx} className="flex items-start gap-3">
              <div className="w-6 h-6 rounded-full bg-green-100 flex items-center justify-center flex-shrink-0 mt-0.5">
                <span className="text-green-700 text-sm font-semibold">{idx + 1}</span>
              </div>
              <p className="text-gray-700">{finding}</p>
            </li>
          ))}
        </ul>
      </div>

      <div className="bg-white rounded-lg shadow-lg p-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-4 flex items-center gap-2">
          <AlertTriangle className="w-6 h-6 text-orange-600" />
          Recommendations ({recommendations.length})
        </h2>
        <div className="space-y-4">
          {recommendations.map((rec) => (
            <RecommendationCard key={rec.id} recommendation={rec} />
          ))}
        </div>
      </div>

      <div className="bg-gradient-to-r from-purple-600 to-blue-600 rounded-lg shadow-lg p-6 text-white">
        <h2 className="text-2xl font-bold mb-4">Next Steps</h2>
        <ol className="space-y-3">
          {summary.next_steps.map((step, idx) => (
            <li key={idx} className="flex items-start gap-3">
              <div className="w-8 h-8 rounded-full bg-white/20 flex items-center justify-center flex-shrink-0">
                <span className="font-bold">{idx + 1}</span>
              </div>
              <p className="text-white/90 pt-1">{step}</p>
            </li>
          ))}
        </ol>
      </div>
    </div>
  )
}


