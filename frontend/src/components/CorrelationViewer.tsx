import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"

interface CorrelationData {
  numeric_correlations: Array<{
    feature1: string
    feature2: string
    correlation: number
    p_value: number
    method: string
    n: number
  }>
  categorical_associations: Array<{
    feature1: string
    feature2: string
    correlation: number
    p_value: number
    method: string
    n: number
  }>
  fdr_applied: boolean
  total_tests: number
}

export default function CorrelationViewer({ data }: { data: CorrelationData }) {
  const getStrengthLabel = (corr: number) => {
    const abs = Math.abs(corr)
    if (abs >= 0.7) return { label: "Strong", color: "bg-red-500" }
    if (abs >= 0.4) return { label: "Moderate", color: "bg-yellow-500" }
    return { label: "Weak", color: "bg-blue-500" }
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>Correlation Analysis</span>
            {data.fdr_applied && (
              <Badge variant="outline">FDR Corrected ({data.total_tests} tests)</Badge>
            )}
          </CardTitle>
        </CardHeader>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Numeric Correlations (Top 10)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {data.numeric_correlations
              .sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation))
              .slice(0, 10)
              .map((corr, idx) => {
                const strength = getStrengthLabel(corr.correlation)
                return (
                  <div key={idx} className="flex items-center justify-between p-3 border rounded">
                    <div>
                      <span className="font-mono text-sm">{corr.feature1}</span>
                      <span className="mx-2">↔</span>
                      <span className="font-mono text-sm">{corr.feature2}</span>
                    </div>
                    <div className="flex items-center gap-3">
                      <Badge className={strength.color}>{strength.label}</Badge>
                      <span className="font-bold text-lg">
                        {corr.correlation.toFixed(3)}
                      </span>
                      <span className="text-xs text-gray-500">
                        (p={corr.p_value.toFixed(4)})
                      </span>
                    </div>
                  </div>
                )
              })}
          </div>
        </CardContent>
      </Card>

      {data.categorical_associations.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Categorical Associations (Cramér's V)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {data.categorical_associations.slice(0, 5).map((assoc, idx) => (
                <div key={idx} className="flex items-center justify-between p-3 border rounded">
                  <div>
                    <span className="font-mono text-sm">{assoc.feature1}</span>
                    <span className="mx-2">↔</span>
                    <span className="font-mono text-sm">{assoc.feature2}</span>
                  </div>
                  <span className="font-bold">{assoc.correlation.toFixed(3)}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}









