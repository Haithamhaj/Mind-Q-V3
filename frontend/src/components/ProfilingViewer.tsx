import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'

interface ProfilingData {
  total_rows: number
  total_columns: number
  memory_usage_mb: number
  missing_summary: Record<string, { count: number; percentage: number }>
  top_issues: Array<{
    severity: string
    column: string
    issue_type: string
    description: string
    affected_pct: number
  }>
}

export default function ProfilingViewer({ data }: { data: ProfilingData }) {
  const missingData = Object.entries(data.missing_summary).map(([col, info]) => ({
    column: col,
    percentage: info.percentage * 100
  }))

  const severityColor: Record<string, string> = {
    critical: "text-red-600",
    high: "text-orange-600",
    medium: "text-yellow-600",
    low: "text-blue-600"
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-4">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Rows</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{data.total_rows.toLocaleString()}</p>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Columns</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{data.total_columns}</p>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Memory</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{data.memory_usage_mb.toFixed(1)} MB</p>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Missing Data by Column</CardTitle>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={missingData}>
              <XAxis dataKey="column" angle={-45} textAnchor="end" height={100} />
              <YAxis label={{ value: 'Missing %', angle: -90, position: 'insideLeft' }} />
              <Tooltip />
              <Bar dataKey="percentage" fill="#ef4444" />
            </BarChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Top Data Quality Issues</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {data.top_issues.map((issue, idx) => (
              <Alert key={idx}>
                <AlertDescription>
                  <span className={`font-semibold ${severityColor[issue.severity]}`}>
                    {issue.severity.toUpperCase()}
                  </span>
                  {' - '}
                  <span className="font-mono">{issue.column}</span>
                  {': '}
                  {issue.description}
                  {' '}
                  <span className="text-gray-500">
                    ({(issue.affected_pct * 100).toFixed(1)}% affected)
                  </span>
                </AlertDescription>
              </Alert>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}


