import { useState } from 'react'
import { ChevronDown, ChevronUp, Target, Zap, TrendingUp } from 'lucide-react'

interface RecommendationCardProps {
  recommendation: {
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
}

export default function RecommendationCard({ recommendation }: RecommendationCardProps) {
  const [expanded, setExpanded] = useState(false)

  const getImpactColor = (impact: string) => {
    switch (impact.toLowerCase()) {
      case 'high':
        return 'bg-red-100 text-red-800 border-red-200'
      case 'medium':
        return 'bg-yellow-100 text-yellow-800 border-yellow-200'
      case 'low':
        return 'bg-green-100 text-green-800 border-green-200'
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const getEffortColor = (effort: string) => {
    switch (effort.toLowerCase()) {
      case 'high':
        return 'bg-purple-100 text-purple-800'
      case 'medium':
        return 'bg-blue-100 text-blue-800'
      case 'low':
        return 'bg-green-100 text-green-800'
      default:
        return 'bg-gray-100 text-gray-800'
    }
  }

  const getPriorityIcon = (priority: number) => {
    if (priority <= 2) return <Target className="w-5 h-5 text-red-600" />
    if (priority <= 4) return <Zap className="w-5 h-5 text-yellow-600" />
    return <TrendingUp className="w-5 h-5 text-green-600" />
  }

  return (
    <div className="border rounded-lg hover:shadow-md transition-shadow">
      <div className="p-4 cursor-pointer" onClick={() => setExpanded(!expanded)}>
        <div className="flex items-start justify-between">
          <div className="flex items-start gap-3 flex-1">
            <div className="mt-1">{getPriorityIcon(recommendation.priority)}</div>
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-gray-900">{recommendation.title}</h3>
              <p className="text-gray-600 text-sm mt-1">{recommendation.description}</p>
              <div className="flex items-center gap-2 mt-3 flex-wrap">
                <span
                  className={`px-3 py-1 rounded-full text-xs font-semibold border ${getImpactColor(
                    recommendation.impact
                  )}`}
                >
                  Impact: {recommendation.impact}
                </span>
                <span className={`px-3 py-1 rounded-full text-xs font-semibold ${getEffortColor(recommendation.effort)}`}>
                  Effort: {recommendation.effort}
                </span>
                <span className="px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                  {recommendation.action_type.replace('_', ' ')}
                </span>
                <span className="px-3 py-1 rounded-full text-xs font-semibold bg-green-100 text-green-800">
                  {recommendation.expected_improvement}
                </span>
              </div>
            </div>
          </div>
          <button className="ml-4 text-gray-400 hover:text-gray-600">
            {expanded ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
          </button>
        </div>
      </div>
      {expanded && (
        <div className="border-t bg-gray-50 p-4">
          <div className="space-y-3">
            <div>
              <h4 className="font-semibold text-gray-900 mb-1">Rationale:</h4>
              <p className="text-gray-700 text-sm">{recommendation.rationale}</p>
            </div>
            <div>
              <h4 className="font-semibold text-gray-900 mb-1">Expected Improvement:</h4>
              <p className="text-gray-700 text-sm">{recommendation.expected_improvement}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}


