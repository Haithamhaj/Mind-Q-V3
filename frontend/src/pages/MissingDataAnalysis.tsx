import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Loader2, Play, ArrowLeft } from 'lucide-react'
import { Link } from 'react-router-dom'
import { apiClient } from '@/api/client'

interface MissingDataResult {
  status: string
  missing_patterns: Record<string, any>
  imputation_strategy: Record<string, string>
  imputed_data_path: string
  quality_metrics: Record<string, number>
  warnings: string[]
  timestamp: string
}

export default function MissingDataAnalysis() {
  const [isRunning, setIsRunning] = useState(false)
  const [result, setResult] = useState<MissingDataResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [groupColumn, setGroupColumn] = useState<string>('')

  const runPhase5 = async () => {
    setIsRunning(true)
    setError(null)
    
    try {
      const formData = new FormData()
      if (groupColumn.trim()) {
        formData.append('group_column', groupColumn.trim())
      }
      
      const response = await apiClient.post('/api/v1/phases/phase5-missing-data', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      })
      
      setResult(response.data)
    } catch (err: any) {
      setError(err.response?.data?.detail || 'حدث خطأ أثناء تحليل البيانات المفقودة')
    } finally {
      setIsRunning(false)
    }
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-6 flex items-center gap-4">
          <Link to="/">
            <Button variant="outline" size="sm">
              <ArrowLeft className="h-4 w-4 mr-2" />
              العودة للرئيسية
            </Button>
          </Link>
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Missing Data Analysis</h1>
            <p className="text-gray-600 mt-2">Phase 5: تحليل ومعالجة البيانات المفقودة</p>
          </div>
        </div>

        {/* Configuration */}
        <Card className="mb-6">
          <CardHeader>
            <CardTitle>إعدادات التحليل</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div>
                <Label htmlFor="groupColumn">Group Column (Optional)</Label>
                <Input
                  id="groupColumn"
                  placeholder="e.g., PatientId, Gender"
                  value={groupColumn}
                  onChange={(e) => setGroupColumn(e.target.value)}
                />
                <p className="text-sm text-gray-500 mt-1">
                  عمود لتجميع البيانات أثناء تطبيق استراتيجيات الملء
                </p>
              </div>
              
              <Button 
                onClick={runPhase5}
                disabled={isRunning}
                className="w-full"
              >
                {isRunning ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    جاري التحليل...
                  </>
                ) : (
                  <>
                    <Play className="h-4 w-4 mr-2" />
                    تشغيل تحليل البيانات المفقودة
                  </>
                )}
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Error Display */}
        {error && (
          <Alert className="mb-6 border-red-200 bg-red-50">
            <AlertDescription className="text-red-700">
              {error}
            </AlertDescription>
          </Alert>
        )}

        {/* Results */}
        {result && (
          <div className="space-y-6">
            {/* Status */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  حالة التحليل
                  <span className={`px-2 py-1 rounded text-sm ${
                    result.status === 'success' ? 'bg-green-100 text-green-800' : 
                    result.status === 'warning' ? 'bg-yellow-100 text-yellow-800' :
                    'bg-red-100 text-red-800'
                  }`}>
                    {result.status.toUpperCase()}
                  </span>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {result.warnings && result.warnings.length > 0 && (
                  <div className="space-y-2">
                    <h4 className="font-semibold text-yellow-700">تحذيرات:</h4>
                    <ul className="list-disc list-inside space-y-1">
                      {result.warnings.map((warning, index) => (
                        <li key={index} className="text-yellow-700">{warning}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Imputation Strategies */}
            {result.imputation_strategy && (
              <Card>
                <CardHeader>
                  <CardTitle>استراتيجيات الملء المطبقة</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b">
                          <th className="text-right py-2">العمود</th>
                          <th className="text-right py-2">الاستراتيجية</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(result.imputation_strategy).map(([column, strategy]) => (
                          <tr key={column} className="border-b">
                            <td className="py-2 font-medium">{column}</td>
                            <td className="py-2">{strategy}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Quality Metrics */}
            {result.quality_metrics && (
              <Card>
                <CardHeader>
                  <CardTitle>مقاييس الجودة</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    {Object.entries(result.quality_metrics).map(([metric, value]) => (
                      <div key={metric} className="text-center p-4 bg-blue-50 rounded-lg">
                        <div className="text-2xl font-bold text-blue-600">
                          {typeof value === 'number' ? value.toFixed(2) : value}
                        </div>
                        <div className="text-sm text-gray-600">{metric}</div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Success Message */}
            {result.imputed_data_path && (
              <Card className="border-green-200 bg-green-50">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-2 text-green-700">
                    <span className="font-semibold">✅ تم إنشاء ملف البيانات المعالج:</span>
                    <code className="bg-green-100 px-2 py-1 rounded text-sm">
                      {result.imputed_data_path}
                    </code>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        )}
      </div>
    </div>
  )
}


