import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Alert, AlertDescription } from '@/components/ui/alert'
import FileUpload from '@/components/FileUpload'
import QualityControlViewer from '@/components/QualityControlViewer'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Loader2, Play } from 'lucide-react'
import { apiClient } from '@/api/client'

interface QCResult {
  status: string
  missing_report: Record<string, number>
  date_issues: Record<string, any>
  key_issues: Record<string, any>
  warnings: string[]
  errors: string[]
  timestamp: string
}

export default function QualityControl() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [keyColumns, setKeyColumns] = useState('')
  const [result, setResult] = useState<QCResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleFileSelect = (file: File) => {
    setSelectedFile(file)
    setResult(null)
    setError(null)
  }

  const handleFileRemove = () => {
    setSelectedFile(null)
    setResult(null)
    setError(null)
  }

  const runQualityControl = async () => {
    if (!selectedFile) return

    setLoading(true)
    setError(null)

    try {
      const formData = new FormData()
      formData.append('file', selectedFile)
      if (keyColumns.trim()) {
        formData.append('key_columns', keyColumns.trim())
      }

      const response = await apiClient.post('/phases/quality-control', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      })

      setResult(response.data)
    } catch (err: any) {
      setError(err.response?.data?.detail || 'An error occurred while running quality control')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="text-center">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          Quality Control Analysis
        </h1>
        <p className="text-gray-600">
          Upload your dataset to perform automated quality control checks
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Upload Dataset</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <FileUpload
            onFileSelect={handleFileSelect}
            onFileRemove={handleFileRemove}
            selectedFile={selectedFile}
          />

          <div className="space-y-2">
            <Label htmlFor="key-columns">Key Columns (Optional)</Label>
            <Input
              id="key-columns"
              placeholder="e.g., order_id, customer_id"
              value={keyColumns}
              onChange={(e) => setKeyColumns(e.target.value)}
            />
            <p className="text-sm text-gray-500">
              Comma-separated list of key columns for uniqueness checks
            </p>
          </div>

          <Button
            onClick={runQualityControl}
            disabled={!selectedFile || loading}
            className="w-full"
          >
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Running Quality Control...
              </>
            ) : (
              <>
                <Play className="mr-2 h-4 w-4" />
                Run Quality Control
              </>
            )}
          </Button>
        </CardContent>
      </Card>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {result && (
        <QualityControlViewer result={result} />
      )}

      <Card>
        <CardHeader>
          <CardTitle>Decision Rules</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="border-l-4 border-red-500 pl-4">
              <h4 className="font-semibold text-red-700">STOP Conditions</h4>
              <ul className="text-sm text-gray-600 mt-1 space-y-1">
                <li>• Any field has more than 20% missing data</li>
                <li>• Key columns have more than 10% nulls or duplicates</li>
              </ul>
            </div>
            <div className="border-l-4 border-yellow-500 pl-4">
              <h4 className="font-semibold text-yellow-700">WARN Conditions</h4>
              <ul className="text-sm text-gray-600 mt-1 space-y-1">
                <li>• Date inversions exceed 0.5%</li>
              </ul>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
