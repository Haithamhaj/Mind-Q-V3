import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { CheckCircle2, AlertTriangle, XCircle } from "lucide-react"

interface ImputationData {
  decisions: Array<{
    column: string
    method: string
    reason: string
    missing_before: number
    missing_after: number
  }>
  validation: Record<string, {
    psi: number
    ks_statistic: number
    passed: boolean
  }>
  record_completeness: number
  status: string
  warnings: string[]
}

export default function ImputationViewer({ data }: { data: ImputationData }) {
  const StatusIcon = {
    PASS: <CheckCircle2 className="h-5 w-5 text-green-500" />,
    WARN: <AlertTriangle className="h-5 w-5 text-yellow-500" />,
    STOP: <XCircle className="h-5 w-5 text-red-500" />
  }[data.status]

  return (
    <div className="space-y-4">
      <Card className={`border-2 ${
        data.status === 'PASS' ? 'border-green-500' : 
        data.status === 'WARN' ? 'border-yellow-500' : 
        'border-red-500'
      }`}>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            {StatusIcon}
            Imputation Status: {data.status}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-lg">
            Record Completeness: <span className="font-bold">{(data.record_completeness * 100).toFixed(1)}%</span>
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Imputation Decisions</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {data.decisions.map((decision, idx) => (
              <div key={idx} className="border-l-4 border-blue-500 pl-4 py-2">
                <div className="flex items-center justify-between">
                  <span className="font-mono font-semibold">{decision.column}</span>
                  <Badge variant="outline">{decision.method}</Badge>
                </div>
                <p className="text-sm text-gray-600 mt-1">{decision.reason}</p>
                <p className="text-xs text-gray-500 mt-1">
                  Missing: {decision.missing_before} → {decision.missing_after}
                </p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {Object.keys(data.validation).length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Validation Metrics</CardTitle>
          </CardHeader>
          <CardContent>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-2">Column</th>
                  <th className="text-right py-2">PSI</th>
                  <th className="text-right py-2">KS Stat</th>
                  <th className="text-center py-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(data.validation).map(([col, metrics]) => (
                  <tr key={col} className="border-b">
                    <td className="py-2 font-mono">{col}</td>
                    <td className="text-right py-2">{metrics.psi.toFixed(3)}</td>
                    <td className="text-right py-2">{metrics.ks_statistic.toFixed(3)}</td>
                    <td className="text-center py-2">
                      {metrics.passed ? 
                        <CheckCircle2 className="h-4 w-4 text-green-500 inline" /> : 
                        <XCircle className="h-4 w-4 text-red-500 inline" />
                      }
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>
      )}

      {data.warnings.length > 0 && (
        <Card className="border-yellow-500">
          <CardHeader>
            <CardTitle className="text-yellow-600">Warnings</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="list-disc pl-5 space-y-1">
              {data.warnings.map((warn, idx) => (
                <li key={idx} className="text-sm">{warn}</li>
              ))}
            </ul>
          </CardContent>
        </Card>
      )}
    </div>
  )
}


