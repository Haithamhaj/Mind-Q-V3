import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { CheckCircle2, AlertTriangle, XCircle } from "lucide-react"

interface QCResult {
  status: string
  missing_report: Record<string, number>
  date_issues: Record<string, any>
  key_issues: Record<string, any>
  warnings: string[]
  errors: string[]
}

export default function QualityControlViewer({ result }: { result: QCResult }) {
  const StatusIcon = {
    PASS: <CheckCircle2 className="h-6 w-6 text-green-500" />,
    WARN: <AlertTriangle className="h-6 w-6 text-yellow-500" />,
    STOP: <XCircle className="h-6 w-6 text-red-500" />
  }[result.status]

  const StatusColor = {
    PASS: "border-green-500",
    WARN: "border-yellow-500",
    STOP: "border-red-500"
  }[result.status]

  return (
    <div className="space-y-4">
      <Card className={`border-2 ${StatusColor}`}>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            {StatusIcon}
            Quality Control: {result.status}
          </CardTitle>
        </CardHeader>
      </Card>

      {result.errors.length > 0 && (
        <Alert variant="destructive">
          <AlertDescription>
            <ul className="list-disc pl-4">
              {result.errors.map((err, i) => <li key={i}>{err}</li>)}
            </ul>
          </AlertDescription>
        </Alert>
      )}

      {result.warnings.length > 0 && (
        <Alert>
          <AlertDescription>
            <ul className="list-disc pl-4">
              {result.warnings.map((warn, i) => <li key={i}>{warn}</li>)}
            </ul>
          </AlertDescription>
        </Alert>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Missing Data Report</CardTitle>
        </CardHeader>
        <CardContent>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b">
                <th className="text-left py-2">Column</th>
                <th className="text-right py-2">Missing %</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(result.missing_report).map(([col, pct]) => (
                <tr key={col} className="border-b">
                  <td className="py-2">{col}</td>
                  <td className="text-right py-2 font-mono">
                    {(pct * 100).toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>
    </div>
  )
}
