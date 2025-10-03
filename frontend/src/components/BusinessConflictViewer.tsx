import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { AlertTriangle } from "lucide-react"

interface BusinessConflict {
  feature1: string
  feature2: string
  observed_correlation: number
  expected_relationship: string
  conflict_severity: string
  llm_hypothesis?: string
  resolution: string
}

interface BusinessValidationData {
  conflicts_detected: BusinessConflict[]
  llm_hypotheses_generated: number
  status: string
}

export default function BusinessConflictViewer({ data }: { data: BusinessValidationData }) {
  const severityColor: Record<string, string> = {
    high: "border-red-500",
    medium: "border-yellow-500",
    low: "border-blue-500"
  }

  return (
    <div className="space-y-4">
      <Alert className={data.status === "STOP" ? "border-red-500" : data.status === "WARN" ? "border-yellow-500" : "border-green-500"}>
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>
          Business Validation Status: <strong>{data.status}</strong>
          {data.conflicts_detected.length > 0 && (
            <span> - {data.conflicts_detected.length} conflict(s) detected</span>
          )}
        </AlertDescription>
      </Alert>

      {data.conflicts_detected.map((conflict, idx) => (
        <Card key={idx} className={`border-2 ${severityColor[conflict.conflict_severity]}`}>
          <CardHeader>
            <CardTitle className="text-base">
              Conflict {idx + 1}: {conflict.feature1} ↔ {conflict.feature2}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-gray-600">Observed Correlation:</span>
                <p className="font-bold text-lg">{conflict.observed_correlation.toFixed(3)}</p>
              </div>
              <div>
                <span className="text-gray-600">Expected Relationship:</span>
                <p className="font-bold capitalize">{conflict.expected_relationship}</p>
              </div>
            </div>

            {conflict.llm_hypothesis && (
              <div className="bg-blue-50 p-4 rounded">
                <p className="text-xs font-semibold text-blue-900 mb-2">
                  AI-Generated Hypothesis:
                </p>
                <p className="text-sm text-gray-700 whitespace-pre-line">
                  {conflict.llm_hypothesis}
                </p>
              </div>
            )}

            <div className="flex justify-between items-center">
              <span className={`text-xs px-2 py-1 rounded ${
                conflict.conflict_severity === 'high' ? 'bg-red-100 text-red-700' :
                conflict.conflict_severity === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                'bg-blue-100 text-blue-700'
              }`}>
                Severity: {conflict.conflict_severity}
              </span>
              <span className="text-xs text-gray-500">
                Resolution: {conflict.resolution}
              </span>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}


