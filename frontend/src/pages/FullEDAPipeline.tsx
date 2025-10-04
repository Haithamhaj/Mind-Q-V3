import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Alert, AlertDescription } from '@/components/ui/alert'
import FileUpload from '@/components/FileUpload'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import { Loader2, Play, CheckCircle, XCircle, Clock } from 'lucide-react'
import { apiClient } from '@/api/client'

interface PhaseResult {
  status: string
  data?: any
  error?: string
  timestamp?: string
}

interface PhaseInfo {
  id: string
  name: string
  endpoint: string
  description: string
  requiresFile: boolean
  requiresTarget?: boolean
}

const PHASES: PhaseInfo[] = [
  { id: 'phase0', name: 'Phase 0: Quality Control', endpoint: '/phases/quality-control', description: 'Upload and validate data quality', requiresFile: true },
  { id: 'phase1', name: 'Phase 1: Goal & KPIs', endpoint: '/phases/phase1-goal-kpis', description: 'Define business objectives', requiresFile: true },
  { id: 'phase2', name: 'Phase 2: Data Ingestion', endpoint: '/phases/phase2-ingestion', description: 'Process and store data', requiresFile: true },
  { id: 'phase3', name: 'Phase 3: Schema Discovery', endpoint: '/phases/phase3-schema', description: 'Analyze data structure', requiresFile: false },
  { id: 'phase4', name: 'Phase 4: Data Profiling', endpoint: '/phases/phase4-profiling', description: 'Generate comprehensive statistics', requiresFile: false },
  { id: 'phase5', name: 'Phase 5: Missing Data', endpoint: '/phases/phase5-missing-data', description: 'Analyze and handle missing values', requiresFile: false },
  { id: 'phase6', name: 'Phase 6: Standardization', endpoint: '/phases/phase6-standardization', description: 'Clean and standardize formats', requiresFile: false },
  { id: 'phase7', name: 'Phase 7: Feature Engineering', endpoint: '/phases/phase7-features', description: 'Create derived features', requiresFile: false },
  { id: 'phase7.5', name: 'Phase 7.5: Encoding & Scaling', endpoint: '/phases/phase7-5-encoding', description: 'Encode categorical variables', requiresFile: false },
  { id: 'phase8', name: 'Phase 8: Data Merging', endpoint: '/phases/phase8-merging', description: 'Combine multiple datasets', requiresFile: false },
  { id: 'phase9', name: 'Phase 9: Correlation Analysis', endpoint: '/phases/phase9-correlations', description: 'Analyze variable relationships', requiresFile: false },
  { id: 'phase9.5', name: 'Phase 9.5: Business Validation', endpoint: '/phases/phase9-5-business-validation', description: 'Validate against business rules', requiresFile: false },
  { id: 'phase10', name: 'Phase 10: Data Packaging', endpoint: '/phases/phase10-packaging', description: 'Prepare final dataset', requiresFile: false },
  { id: 'phase10.5', name: 'Phase 10.5: Train/Test Split', endpoint: '/phases/phase10-5-split', description: 'Split data for modeling', requiresFile: false },
  { id: 'phase11', name: 'Phase 11: Advanced Analytics', endpoint: '/phases/phase11-advanced', description: 'Perform advanced statistical analysis', requiresFile: false },
  { id: 'phase11.5', name: 'Phase 11.5: Feature Selection', endpoint: '/phases/phase11-5-selection', description: 'Select optimal features', requiresFile: false, requiresTarget: true },
  { id: 'phase12', name: 'Phase 12: Text Analysis', endpoint: '/phases/phase12-text-features', description: 'NLP analysis for text data', requiresFile: false },
  { id: 'phase13', name: 'Phase 13: Monitoring', endpoint: '/phases/phase13-monitoring', description: 'Generate reports and monitoring', requiresFile: false }
]

export default function FullEDAPipeline() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [isRunning, setIsRunning] = useState(false)
  const [currentPhase, setCurrentPhase] = useState<string | null>(null)
  const [phaseResults, setPhaseResults] = useState<Record<string, PhaseResult>>({})
  const [progress, setProgress] = useState(0)
  const [targetColumn, setTargetColumn] = useState('')
  const [domain, setDomain] = useState('healthcare')

  const handleFileSelect = (file: File) => {
    setSelectedFile(file)
    setPhaseResults({})
    setProgress(0)
  }

  const handleFileRemove = () => {
    setSelectedFile(null)
    setPhaseResults({})
    setProgress(0)
  }

  const runFullPipeline = async () => {
    if (!selectedFile) {
      alert('Please select a file first!')
      return
    }

    console.log('🚀 Starting REAL EDA pipeline analysis...')
    setIsRunning(true)
    setPhaseResults({})
    setProgress(0)
    
    const results: Record<string, PhaseResult> = {}

    for (let i = 0; i < PHASES.length; i++) {
      const phase = PHASES[i]
      setCurrentPhase(phase.id)
      setProgress((i / PHASES.length) * 100)

      try {
        let response
        console.log(`🔄 Running ${phase.name}...`)
        
        if (phase.requiresFile) {
          // Phases that need the original file
          const formData = new FormData()
          formData.append('file', selectedFile)
          if (domain) formData.append('domain', domain)
          
          response = await apiClient.post(phase.endpoint, formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          })
          
        } else if (phase.requiresTarget && targetColumn) {
          // Phases that need target column
          const formData = new FormData()
          formData.append('target_column', targetColumn)
          if (domain) formData.append('domain', domain)
          
          response = await apiClient.post(phase.endpoint, formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          })
          
        } else {
          // Phases that work on processed data
          try {
            // Try POST request first
            response = await apiClient.post(phase.endpoint)
          } catch (postError) {
            // If POST fails, try with form data and domain
            const formData = new FormData()
            if (domain) formData.append('domain', domain)
            
            response = await apiClient.post(phase.endpoint, formData, {
              headers: { 'Content-Type': 'multipart/form-data' }
            })
          }
        }

        results[phase.id] = {
          status: 'success',
          data: response.data,
          timestamp: new Date().toISOString()
        }
        
        console.log(`✅ ${phase.name} completed successfully`)
        
      } catch (err: any) {
        const errorMsg = err.response?.data?.detail || `Failed to run ${phase.name}`
        console.error(`❌ ${phase.name} failed:`, errorMsg)
        
        results[phase.id] = {
          status: 'error',
          error: errorMsg,
          timestamp: new Date().toISOString()
        }
        
        // Continue with the pipeline even if one phase fails
        console.log(`⏭️ Continuing pipeline despite ${phase.name} failure`)
      }

      // Update results after each phase
      setPhaseResults(prev => ({...prev, [phase.id]: results[phase.id]}))
      
      // Small delay for better UX
      await new Promise(resolve => setTimeout(resolve, 300))
    }

    setProgress(100)
    setCurrentPhase(null)
    setIsRunning(false)
    
    console.log('🎉 Real EDA pipeline completed!')
    console.log('Results summary:', {
      total: PHASES.length,
      successful: Object.values(results).filter(r => r.status === 'success').length,
      failed: Object.values(results).filter(r => r.status === 'error').length
    })
  }

  const getPhaseIcon = (phaseId: string) => {
    const result = phaseResults[phaseId]
    if (!result) {
      return currentPhase === phaseId ? 
        <Loader2 className="h-4 w-4 animate-spin text-blue-500" /> : 
        <Clock className="h-4 w-4 text-gray-400" />
    }
    if (result.status === 'success') return <CheckCircle className="h-4 w-4 text-green-500" />
    if (result.status === 'simulated') return <CheckCircle className="h-4 w-4 text-yellow-500" />
    return <XCircle className="h-4 w-4 text-red-500" />
  }

  const completedPhases = Object.values(phaseResults).filter(r => r.status === 'success' || r.status === 'simulated').length
  const failedPhases = Object.values(phaseResults).filter(r => r.status === 'error').length
  const simulatedPhases = Object.values(phaseResults).filter(r => r.status === 'simulated').length

  // Add error boundary
  try {
    return (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-6 text-center">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">
            Complete EDA Pipeline
          </h1>
          <p className="text-gray-600 mb-4">
            Automated 18-Phase Exploratory Data Analysis
          </p>
        </div>

        {/* Configuration */}
        <Card className="mb-6">
          <CardHeader>
            <CardTitle>Pipeline Configuration</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <FileUpload
              onFileSelect={handleFileSelect}
              onFileRemove={handleFileRemove}
              selectedFile={selectedFile}
            />
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium mb-2">Domain</label>
                <select 
                  value={domain} 
                  onChange={(e) => setDomain(e.target.value)}
                  className="w-full p-2 border rounded-md"
                >
                  <option value="healthcare">Healthcare</option>
                  <option value="finance">Finance</option>
                  <option value="retail">Retail</option>
                  <option value="manufacturing">Manufacturing</option>
                  <option value="general">General</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium mb-2">Target Column (Optional)</label>
                <input
                  type="text"
                  value={targetColumn}
                  onChange={(e) => setTargetColumn(e.target.value)}
                  placeholder="e.g., Showed_up, target, outcome"
                  className="w-full p-2 border rounded-md"
                />
              </div>
            </div>

            <Button
              onClick={runFullPipeline}
              disabled={!selectedFile || isRunning}
              className="w-full"
              size="lg"
            >
              {isRunning ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Running Full Pipeline...
                </>
              ) : (
                <>
                  <Play className="h-4 w-4 mr-2" />
                  Run Complete EDA Pipeline (18 Phases)
                </>
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Progress */}
        {(isRunning || Object.keys(phaseResults).length > 0) && (
          <Card className="mb-6">
            <CardContent className="pt-6">
              <div className="space-y-4">
                <div className="flex justify-between text-sm text-gray-600">
                  <span>Overall Progress</span>
                  <span>{Math.round(progress)}%</span>
                </div>
                <Progress value={progress} className="w-full" />
                
                <div className="grid grid-cols-4 gap-4 text-center">
                  <div>
                    <div className="text-2xl font-bold text-green-600">{completedPhases - simulatedPhases}</div>
                    <div className="text-sm text-gray-600">Completed</div>
                  </div>
                  <div>
                    <div className="text-2xl font-bold text-yellow-600">{simulatedPhases}</div>
                    <div className="text-sm text-gray-600">Simulated</div>
                  </div>
                  <div>
                    <div className="text-2xl font-bold text-red-600">{failedPhases}</div>
                    <div className="text-sm text-gray-600">Failed</div>
                  </div>
                  <div>
                    <div className="text-2xl font-bold text-blue-600">{PHASES.length}</div>
                    <div className="text-sm text-gray-600">Total Phases</div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Phases Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {PHASES.map((phase) => {
            const result = phaseResults[phase.id]
            const isActive = currentPhase === phase.id
            
            return (
              <Card key={phase.id}               className={`
                ${isActive ? 'border-blue-500 shadow-lg' : ''}
                ${result?.status === 'success' ? 'border-green-200 bg-green-50' : ''}
                ${result?.status === 'simulated' ? 'border-yellow-200 bg-yellow-50' : ''}
                ${result?.status === 'error' ? 'border-red-200 bg-red-50' : ''}
              `}>
                <CardContent className="p-4">
                  <div className="flex items-start justify-between mb-2">
                    <h3 className="font-semibold text-sm">{phase.name}</h3>
                    {getPhaseIcon(phase.id)}
                  </div>
                  
                  <p className="text-xs text-gray-600 mb-2">
                    {phase.description}
                  </p>
                  
                  {result?.error && (
                    <Alert className="mt-2">
                      <AlertDescription className="text-xs text-red-700">
                        {result.error}
                      </AlertDescription>
                    </Alert>
                  )}
                  
                  {result?.status === 'success' && (
                    <div className="mt-2 text-xs text-green-700">
                      ✅ Completed successfully
                    </div>
                  )}
                  
                  {result?.status === 'simulated' && (
                    <div className="mt-2 text-xs text-yellow-700">
                      ⚡ Simulated (Backend not ready)
                    </div>
                  )}
                </CardContent>
              </Card>
            )
          })}
        </div>

        {/* Results Summary */}
        {Object.keys(phaseResults).length > 0 && !isRunning && (
          <Card className="mt-6">
            <CardHeader>
              <CardTitle>
                🎉 Pipeline Execution Complete! 
                <span className="text-sm font-normal text-gray-500 ml-2">
                  ({completedPhases}/{PHASES.length} phases processed)
                </span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="mb-4 p-4 bg-blue-50 rounded-lg">
                <h3 className="font-semibold text-blue-800 mb-2">Execution Summary:</h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-sm">
                  <div className="flex items-center">
                    <span className="w-3 h-3 bg-green-500 rounded-full mr-2"></span>
                    {Object.values(phaseResults).filter(r => r.status === 'success').length} Real Analysis
                  </div>
                  <div className="flex items-center">
                    <span className="w-3 h-3 bg-yellow-500 rounded-full mr-2"></span>
                    {simulatedPhases} Demo Phases
                  </div>
                  <div className="flex items-center">
                    <span className="w-3 h-3 bg-red-500 rounded-full mr-2"></span>
                    {failedPhases} Failed
                  </div>
                  <div className="flex items-center">
                    <span className="w-3 h-3 bg-blue-500 rounded-full mr-2"></span>
                    {PHASES.length} Total Phases
                  </div>
                </div>
              </div>
              
              <div className="space-y-2 max-h-64 overflow-y-auto">
                {Object.entries(phaseResults).map(([phaseId, result]) => {
                  const phase = PHASES.find(p => p.id === phaseId)
                  if (!phase) return null
                  
                  return (
                    <div key={phaseId} className="flex justify-between items-center p-3 border rounded-lg hover:bg-gray-50">
                      <div>
                        <span className="text-sm font-medium">{phase.name}</span>
                        {result.error && (
                          <div className="text-xs text-red-600 mt-1">{result.error}</div>
                        )}
                      </div>
                      <span className={`text-xs px-3 py-1 rounded-full ${
                        result.status === 'success' ? 'bg-green-100 text-green-800' : 
                        result.status === 'simulated' ? 'bg-yellow-100 text-yellow-800' :
                        'bg-red-100 text-red-800'
                      }`}>
                        {result.status.toUpperCase()}
                      </span>
                    </div>
                  )
                })}
              </div>
              
              <div className="mt-4 p-3 bg-blue-50 rounded-lg border border-blue-200">
                <h4 className="text-blue-800 font-semibold mb-2">🎯 EDA Pipeline Demo Complete!</h4>
                <div className="text-sm text-blue-700 space-y-1">
                  <p>✅ <strong>Real Analysis:</strong> {Object.values(phaseResults).filter(r => r.status === 'success').length} phases with actual data processing</p>
                  <p>🎭 <strong>Demo Phases:</strong> {simulatedPhases} phases simulated for demonstration</p>
                  <p>📊 This demonstrates the complete 18-phase EDA workflow you would get with a fully configured backend.</p>
                  {failedPhases > 0 && (
                    <p>⚠️ <strong>Note:</strong> {failedPhases} phases failed due to backend limitations.</p>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>
        )}
        
        {/* Debug Info (only in console) */}
        {process.env.NODE_ENV === 'development' && (
          <div style={{ display: 'none' }}>
            {console.log('Debug - phaseResults:', phaseResults)}
            {console.log('Debug - isRunning:', isRunning)}
            {console.log('Debug - progress:', progress)}
          </div>
        )}
      </div>
    </div>
  )
  } catch (error) {
    console.error('Error rendering FullEDAPipeline:', error)
    return (
      <div className="min-h-screen bg-gray-50 p-6 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-red-600 mb-4">Something went wrong!</h1>
          <p className="text-gray-600 mb-4">Please refresh the page and try again.</p>
          <button 
            onClick={() => window.location.reload()} 
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            Refresh Page
          </button>
        </div>
      </div>
    )
  }
}
