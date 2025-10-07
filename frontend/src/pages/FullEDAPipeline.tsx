import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Alert, AlertDescription } from '@/components/ui/alert'
import FileUpload from '@/components/FileUpload'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import { Loader2, Play, CheckCircle, XCircle, Clock, Download } from 'lucide-react'
import { apiClient } from '@/api/client'
import { savePipelineData, getPipelineData, clearPipelineData, hasValidPipelineData, getStoredFileInfo } from '@/lib/localStorage'

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

const MIND_Q_V3_PHASES: PhaseInfo[] = [
  { id: 'phase0', name: 'Phase 0: Quality Control', endpoint: '/phases/quality-control', description: 'Upload and validate data quality', requiresFile: true },
  { id: 'phase1', name: 'Phase 1: Goal & KPIs', endpoint: '/phases/phase1-goal-kpis', description: '🤖 AI-powered business objectives and KPI selection', requiresFile: true },
  { id: 'phase2', name: 'Phase 2: Data Ingestion', endpoint: '/phases/phase2-ingestion', description: 'Process and store data as Parquet', requiresFile: true },
  { id: 'phase3', name: 'Phase 3: Schema Discovery', endpoint: '/phases/phase3-schema', description: 'Analyze data structure and types', requiresFile: false },
  { id: 'phase4', name: 'Phase 4: Data Profiling', endpoint: '/phases/phase4-profiling', description: 'Generate comprehensive statistics', requiresFile: false },
  { id: 'phase5', name: 'Phase 5: Missing Data Analysis', endpoint: '/phases/phase5-missing-data', description: 'Analyze and handle missing values', requiresFile: false },
  { id: 'phase6', name: 'Phase 6: Standardization', endpoint: '/phases/phase6-standardization', description: 'Clean and standardize formats', requiresFile: false },
  { id: 'phase7', name: 'Phase 7: Feature Engineering', endpoint: '/phases/phase7-features', description: 'Create derived features', requiresFile: false },
  { id: 'phase7.5', name: 'Phase 7.5: Encoding & Scaling', endpoint: '/phases/phase7-5-encoding', description: 'Encode categorical variables', requiresFile: false, requiresTarget: true },
  { id: 'phase8', name: 'Phase 8: Data Merging', endpoint: '/phases/phase8-merging', description: 'Combine multiple datasets', requiresFile: false },
  { id: 'phase9', name: 'Phase 9: Correlation Analysis', endpoint: '/phases/phase9-correlations', description: 'Analyze variable relationships', requiresFile: false },
  { id: 'phase9.5', name: 'Phase 9.5: Business Validation', endpoint: '/phases/phase9-5-business-validation', description: 'Validate against business rules', requiresFile: false },
  { id: 'phase10', name: 'Phase 10: Packaging', endpoint: '/phases/phase10-packaging', description: 'Package dataset prior to splitting', requiresFile: false },
  { id: 'phase10.5', name: 'Phase 10.5: Train/Val/Test Split', endpoint: '/phases/phase10-5-split', description: 'Create train/validation/test splits', requiresFile: false, requiresTarget: true },
  { id: 'phase11', name: 'Phase 11: Advanced Exploration', endpoint: '/phases/phase11-advanced', description: 'Explore advanced patterns', requiresFile: false },
  { id: 'phase11.5', name: 'Phase 11.5: Feature Selection', endpoint: '/phases/phase11-5-selection', description: 'Rank and select top features', requiresFile: false, requiresTarget: true },
  { id: 'phase12', name: 'Phase 12: Text Features (MVP)', endpoint: '/phases/phase12-text-features', description: 'Generate basic text features', requiresFile: false },
  { id: 'phase13', name: 'Phase 13: Monitoring Setup', endpoint: '/phases/phase13-monitoring', description: 'Configure drift/monitoring', requiresFile: false },
  { id: 'phase14', name: 'Phase 14: Model Training', endpoint: '/phases/phase14-train-models', description: 'Train models and generate evaluation artifacts', requiresFile: false, requiresTarget: true },
  { id: 'phase14.5', name: 'Phase 14.5: LLM Analysis', endpoint: '/llm-analysis/run-analysis', description: 'AI-assisted insights and recommendations', requiresFile: false, requiresTarget: true }
]

const PHASES = MIND_Q_V3_PHASES

export default function FullEDAPipeline() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [isRunning, setIsRunning] = useState(false)
  const [currentPhase, setCurrentPhase] = useState<string | null>(null)
  const [phaseResults, setPhaseResults] = useState<Record<string, PhaseResult>>({})
  const [progress, setProgress] = useState(0)
  const [targetColumn, setTargetColumn] = useState('')
  const [domain, setDomain] = useState('healthcare')
  const [autoSuggestedTarget, setAutoSuggestedTarget] = useState<string>('')
  const [llmCandidates, setLlmCandidates] = useState<Array<{ name: string; reason?: string; nunique?: number; confidence?: string }>>([])
  const [availableColumns, setAvailableColumns] = useState<string[]>([])
  const [targetSuggestionError, setTargetSuggestionError] = useState<string | null>(null)
  const [textTableFile, setTextTableFile] = useState<File | null>(null)
  const [textTableName, setTextTableName] = useState<string>('')
  const [textTableKey, setTextTableKey] = useState<string>('AWB_NO')
  const [textTableStatus, setTextTableStatus] = useState<string | null>(null)
  const [registeredTextTables, setRegisteredTextTables] = useState<Record<string, any>>({})

  const handleTargetColumnChange = (newTargetColumn: string) => {
    setTargetSuggestionError(null)
    setTargetColumn(newTargetColumn)
    savePipelineData({
      domain,
      targetColumn: newTargetColumn,
      phaseResults,
      progress,
    })
  }

  const fetchTargetSuggestion = async () => {
    try {
      setTargetSuggestionError(null)
      const response = await apiClient.get('/phases/columns', { params: { domain } })
      const payload = response.data ?? {}

      const suggestedTarget =
        typeof payload.suggested_target === 'string' ? payload.suggested_target : ''

      const candidates = Array.isArray(payload.llm_candidates)
        ? (payload.llm_candidates as Array<{ name: string; reason?: string; nunique?: number; confidence?: string }>)
        : []

      const columnEntries = Array.isArray(payload.columns)
        ? (payload.columns as Array<{ name: string }>)
        : []

      const nameSet = new Set<string>(columnEntries.map((c) => String(c.name)))
      const names = Array.from(nameSet)

      setLlmCandidates(candidates)
      setAvailableColumns(names)
      setAutoSuggestedTarget(suggestedTarget)

      if (!targetColumn) {
        if (suggestedTarget) {
          handleTargetColumnChange(suggestedTarget)
        } else if (names.length > 0) {
          // last-resort fallback so we never pass N/A
          handleTargetColumnChange(names[0])
        }
      }
    } catch (e) {
      setTargetSuggestionError('Unable to auto-suggest a target column. Please type or select the target manually.')
    }
  }

  const fetchRegisteredTextTables = async () => {
    try {
      const response = await apiClient.get('/phases/phase8/registered-text-tables')
      setRegisteredTextTables(response.data || {})
    } catch (error) {
      console.warn('Failed to load registered text tables', error)
    }
  }

  const handleRegisterTextTable = async () => {
    if (!textTableFile) {
      setTextTableStatus('Please select a text dataset file before uploading.')
      return
    }
    if (!textTableKey) {
      setTextTableStatus('Please specify the key column shared with the main dataset.')
      return
    }

    try {
      setTextTableStatus('Uploading text dataset...')
      const formData = new FormData()
      formData.append('file', textTableFile)
      formData.append('dataset_name', textTableName || textTableFile.name)
      formData.append('key_column', textTableKey)

      await apiClient.post('/phases/phase8/register-text-table', formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      })

      setTextTableStatus('Text dataset registered successfully.')
      setTextTableFile(null)
      if (!textTableName) {
        setTextTableName('')
      }
      await fetchRegisteredTextTables()
    } catch (error: any) {
      const message = error?.response?.data?.detail || error?.message || 'Failed to register text dataset.'
      setTextTableStatus(message)
    }
  }

  // Load saved data on component mount
  useEffect(() => {
    const savedData = getPipelineData()
    if (savedData) {
      console.log('📁 Loading saved pipeline data from localStorage...')
      setDomain(savedData.domain || 'healthcare')
      setTargetColumn(savedData.targetColumn || '')
      setPhaseResults(savedData.phaseResults || {})
      setProgress(savedData.progress || 0)
      
      // Check if we have a stored file info
      const fileInfo = getStoredFileInfo()
      if (fileInfo) {
        console.log('📄 Found stored file info:', fileInfo.name)
      }
    }
  }, [])

  useEffect(() => {
    fetchRegisteredTextTables()
  }, [])

  const handleFileSelect = (file: File) => {
    setSelectedFile(file)
    setPhaseResults({})
    setProgress(0)
    
    // Save file info to localStorage
    savePipelineData({
      selectedFile: {
        name: file.name,
        size: file.size,
        type: file.type,
        lastModified: file.lastModified
      },
      domain,
      targetColumn,
      phaseResults: {},
      progress: 0
    })
  }

  const handleFileRemove = () => {
    setSelectedFile(null)
    setPhaseResults({})
    setProgress(0)
    
    // Clear localStorage when file is removed
    clearPipelineData()
  }

  const saveProgress = (newResults: Record<string, PhaseResult>, newProgress: number) => {
    setPhaseResults(newResults)
    
    savePipelineData({
      domain,
      targetColumn,
      phaseResults: newResults,
      progress: newProgress
    })
  }

  // Handle domain change
  const handleDomainChange = (newDomain: string) => {
    setDomain(newDomain)
    savePipelineData({
      domain: newDomain,
      targetColumn,
      phaseResults,
      progress
    })
  }

  const runFullPipeline = async () => {
    if (!selectedFile) {
      alert('Please select a file first!')
      return
    }

    console.log('🚀 Starting REAL Mind-Q-V3 analysis with proper phase sequence...')
    console.log('File:', selectedFile.name, 'Domain:', domain)
    setIsRunning(true)
    
    const results: Record<string, PhaseResult> = {}
    saveProgress({}, 0)

    for (let i = 0; i < PHASES.length; i++) {
      const phase = PHASES[i]
      setCurrentPhase(phase.id)
      const currentProgress = (i / PHASES.length) * 100
      setProgress(currentProgress)

      if (phase.requiresTarget && !targetColumn) {
        await fetchTargetSuggestion()
        const guidance = 'Please specify a target column before running advanced phases.'
        setTargetSuggestionError('Select or type the target column to unlock the remaining phases.')
        results[phase.id] = {
          status: 'blocked',
          error: guidance,
          timestamp: new Date().toISOString()
        }
        const newProgress = ((i + 1) / PHASES.length) * 100
        setProgress(newProgress)
        saveProgress({ ...results }, newProgress)
        await new Promise(resolve => setTimeout(resolve, 250))
        continue
      }

      try {
        let response
        console.log(`🔄 Running ${phase.name} - REAL ANALYSIS...`)
        
        if (phase.id === 'phase0') {
          // Phase 0: Quality Control
          const formData = new FormData()
          formData.append('file', selectedFile)
          
          response = await apiClient.post('/phases/quality-control', formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          })
          
        } else if (phase.id === 'phase1') {
          // Phase 1: Goal & KPIs - works on cleaned data from Phase 0
          // No need to send file again, Phase 0 already processed it
          response = await apiClient.post('/phases/phase1-goal-kpis-clean', {
            domain: domain
          })
          
        } else if (phase.id === 'phase2') {
          // Phase 2: Data Ingestion - works on cleaned data from Phase 0
          // No need to send file again, Phase 0 already processed it
          response = await apiClient.post('/phases/phase2-ingestion')
          
        } else if (phase.id === 'phase3') {
          // Phase 3: Schema Discovery - works on ingested data
          response = await apiClient.post('/phases/phase3-schema')
          
        } else if (phase.id === 'phase4') {
          // Phase 4: Profiling - works on typed data
          response = await apiClient.post('/phases/phase4-profiling-clean')
          
        } else if (phase.id === 'phase5') {
          // Phase 5: Missing Data - works on typed data
          response = await apiClient.post('/phases/phase5-missing-data')
          
        } else if (phase.id === 'phase6') {
          // Phase 6: Standardization - needs domain
          const formData = new FormData()
          formData.append('domain', domain)
          
          response = await apiClient.post('/phases/phase6-standardization', formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          })
          
        } else if (phase.id === 'phase7') {
          // Phase 7: Feature Engineering - needs domain  
          const formData = new FormData()
          formData.append('domain', domain)
          
          response = await apiClient.post('/phases/phase7-features', formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          })
          
        } else if (phase.id === 'phase7.5') {
          // Phase 7.5: Encoding & Scaling - needs domain + optional target
          const formData = new FormData()
          formData.append('domain', domain)
          if (targetColumn) formData.append('target_column', targetColumn)
          
          response = await apiClient.post('/phases/phase7-5-encoding', formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          })
          
        } else if (phase.id === 'phase8') {
          // Phase 8: Data Merging - works on processed data
          response = await apiClient.post('/phases/phase8-merging')
          
        } else if (phase.id === 'phase9') {
          // Phase 9: Correlation Analysis - works on processed data
          response = await apiClient.post('/phases/phase9-correlations')
          // After correlations, try to suggest a target if not set
          await fetchTargetSuggestion()
          
        } else if (phase.id === 'phase9.5') {
          // Phase 9.5: Business Validation - needs domain
          const formData = new FormData()
          formData.append('domain', domain)
          
          response = await apiClient.post('/phases/phase9-5-business-validation', formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          })
        } else if (phase.id === 'phase10') {
          // Phase 10: Packaging
          response = await apiClient.post('/phases/phase10-packaging')
        } else if (phase.id === 'phase10.5') {
          // Phase 10.5: Split (requires merged data from Phase 8)
          const formData = new FormData()
          if (targetColumn) formData.append('target_column', targetColumn)
          // Let browser set multipart boundary
          response = await apiClient.post('/phases/phase10-5-split', formData)
        } else if (phase.id === 'phase11') {
          // Phase 11: Advanced Exploration (needs merged data)
          response = await apiClient.post('/phases/phase11-advanced')
        } else if (phase.id === 'phase11.5') {
          if (availableColumns.length === 0) await fetchTargetSuggestion()
          if (availableColumns.length > 0 && !availableColumns.includes(targetColumn)) {
            throw new Error(`Target column '${targetColumn}' not found in available columns. Please select a valid target.`)
          }
          // Phase 11.5: Feature Selection (requires train/validation from 10.5 and target column)
          const formData = new FormData()
          formData.append('target_column', targetColumn)
          formData.append('top_k', String(25))
          response = await apiClient.post('/phases/phase11-5-selection', formData)
        } else if (phase.id === 'phase12') {
          // Phase 12: Text Features (uses merged or train data)
          response = await apiClient.post('/phases/phase12-text-features')
        } else if (phase.id === 'phase13') {
          // Phase 13: Monitoring Setup (needs merged data)
          response = await apiClient.post('/phases/phase13-monitoring')
        } else if (phase.id === 'phase14') {
          // Phase 14: Model Training (generate artifacts for 14.5)
          const formData = new FormData()
          formData.append('domain', domain)
          formData.append('primary_metric', 'recall')
          if (targetColumn) formData.append('target_column', targetColumn)
          response = await apiClient.post('/phases/phase14-train-models', formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
          })
        } else if (phase.id === 'phase14.5') {
          // Phase 14.5: LLM Analysis (depends on Phase 14 artifacts)
          response = await apiClient.post('/llm-analysis/run-analysis')
        } else {
          throw new Error(`Phase ${phase.id} not yet implemented in frontend`)
        }

        results[phase.id] = {
          status: 'success',
          data: response.data,
          timestamp: new Date().toISOString()
        }
        
        console.log(`✅ ${phase.name} REAL ANALYSIS completed successfully`)
        console.log('Response data:', response.data)
        
        // Log important artifacts created based on actual backend behavior
        if (phase.id === 'phase2') {
          console.log('📁 Created: raw_ingested.parquet - Raw data stored')
        } else if (phase.id === 'phase3') {
          console.log('📁 Created: typed_data.parquet - Data types enforced')
        } else if (phase.id === 'phase4') {
          console.log('📁 Created: profile_summary.json - Statistical profiling complete')
        } else if (phase.id === 'phase5') {
          console.log('📁 Created: imputed_data.parquet - Missing data handled')
        } else if (phase.id === 'phase6') {
          console.log('📁 Created: standardized_data.parquet - Text normalized, categories collapsed')
        } else if (phase.id === 'phase7') {
          console.log('📁 Created: features_data.parquet + feature_spec.json - Domain features derived')
        } else if (phase.id === 'phase7.5') {
          console.log('📁 Created: encoded_data.parquet - Categories encoded, features scaled')
        }
        
      } catch (err: any) {
        const detail = err?.response?.data?.detail
        let errorMsg: string
        if (typeof detail === 'string') {
          errorMsg = detail
        } else if (Array.isArray(detail)) {
          errorMsg = detail.map((d: any) => d?.msg || JSON.stringify(d)).join('; ')
        } else if (detail && typeof detail === 'object') {
          errorMsg = JSON.stringify(detail)
        } else {
          errorMsg = err.message || `Failed to run ${phase.name}`
        }

        results[phase.id] = {
          status: 'error',
          error: errorMsg + ' - please check inputs and try again',
          timestamp: new Date().toISOString()
        }

        const newProgress = ((i + 1) / PHASES.length) * 100
        setProgress(newProgress)
        saveProgress({ ...results }, newProgress)
        continue
      }

      // Update results after each phase (whether success, error, or fallback)
      const updatedResults = { ...results }
      const newProgress = ((i + 1) / PHASES.length) * 100
      setProgress(newProgress)
      saveProgress(updatedResults, newProgress)
      
      // Small delay for better UX  
      await new Promise(resolve => setTimeout(resolve, 300))
    }

    setProgress(100)
    setCurrentPhase(null)
    setIsRunning(false)
    
    console.log('🎉 Mind-Q-V3 REAL analysis completed!')
    console.log('Final results:', results)
  }

  const getPhaseIcon = (phaseId: string) => {
    const result = phaseResults[phaseId]
    if (!result) {
      return currentPhase === phaseId ? 
        <Loader2 className="h-4 w-4 animate-spin text-blue-500" /> : 
        <Clock className="h-4 w-4 text-gray-400" />
    }
    if (result?.status === 'success') return <CheckCircle className="h-4 w-4 text-green-500" />
    if (result?.status === 'simulated') return <CheckCircle className="h-4 w-4 text-yellow-500" />
    if (result?.status === 'blocked') return <Clock className="h-4 w-4 text-gray-500" />
    return <XCircle className="h-4 w-4 text-red-500" />
  }

  const completedPhases = Object.values(phaseResults).filter(r => r?.status === 'success' || r?.status === 'simulated').length
  const failedPhases = Object.values(phaseResults).filter(r => r?.status === 'error').length
  const simulatedPhases = Object.values(phaseResults).filter(r => r?.status === 'simulated').length
  const blockedPhases = Object.values(phaseResults).filter(r => r?.status === 'blocked').length
  const hasBlockedPhases = blockedPhases > 0

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-6 text-center">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">
            Mind-Q-V3 Analysis Pipeline
          </h1>
          <p className="text-gray-600 mb-4">
            Real Automated Data Analysis - 12 Mind-Q-V3 Phases
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
                <label className="block text-sm font-medium mb-2">Domain (Built in Mind-Q-V3)</label>
                <select 
                  value={domain} 
                  onChange={(e) => handleDomainChange(e.target.value)}
                  className="w-full p-2 border rounded-md"
                >
                  <option value="healthcare">Healthcare - Hospital Operations</option>
                  <option value="logistics">Logistics - Delivery Operations</option>
                  <option value="emarketing">E-Marketing - Digital Advertising</option>
                  <option value="retail">Retail - E-commerce</option>
                  <option value="finance">Finance - Banking</option>
                </select>
                <p className="text-xs text-gray-500 mt-1">
                  Each domain has specific KPIs and expected columns built into Mind-Q-V3
                </p>
              </div>
              
              <div>
                <label className="block text-sm font-medium mb-2">Target Column (Required for Advanced Phases)</label>
                <input
                  type="text"
                  value={targetColumn}
                  onChange={(e) => handleTargetColumnChange(e.target.value)}
                  placeholder="Healthcare: Showed_up | Logistics: delivery_status | Finance: default_flag"
                  className="w-full p-2 border rounded-md"
                />
                <p className="text-xs text-gray-500 mt-1">
                  Used in Phase 7.5 (Encoding) and advanced analytics
                </p>
              </div>
            </div>

            <div className="space-y-3">
              {/* localStorage Status */}
              {hasValidPipelineData() && (
                <Alert>
                  <Download className="h-4 w-4" />
                  <AlertDescription>
                    <div className="flex items-center justify-between">
                      <span>Pipeline data saved locally. Data will persist across sessions.</span>
                      <Button 
                        variant="outline" 
                        size="sm" 
                        onClick={clearPipelineData}
                        className="ml-2"
                      >
                        Clear Data
                      </Button>
                    </div>
                  </AlertDescription>
                </Alert>
              )}
              
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
                    Run Mind-Q-V3 Analysis (Real Backend Processing)
                  </>
                )}
              </Button>
            </div>
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
                
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4 text-center">
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
                  <div>
                    <div className="text-2xl font-bold text-gray-600">{blockedPhases}</div>
                    <div className="text-sm text-gray-600">Blocked</div>
                  </div>
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
          <div className="md:col-span-2 lg:col-span-3 p-3 rounded border bg-white text-sm space-y-3">
            <div className="font-semibold text-gray-900">Optional Text Dataset Merge</div>
            <div className="text-xs text-gray-500">Attach customer feedback or service notes; the assistant will reconcile and merge them (via key column) before text analysis.</div>
            <input
              type="file"
              accept=".csv,.xlsx,.xls,.parquet"
              onChange={(event) => setTextTableFile(event.target.files ? event.target.files[0] : null)}
              className="w-full border rounded px-3 py-2 text-sm"
            />
            <div className="grid grid-cols-1 md:grid-cols-3 gap-2 text-xs text-gray-600">
              <div>
                <label className="block text-xs font-semibold mb-1">Dataset name</label>
                <input
                  type="text"
                  value={textTableName}
                  onChange={(event) => setTextTableName(event.target.value)}
                  className="w-full border rounded px-2 py-1"
                  placeholder="Customer Feedback"
                />
              </div>
              <div>
                <label className="block text-xs font-semibold mb-1">Key column</label>
                <input
                  type="text"
                  value={textTableKey}
                  onChange={(event) => setTextTableKey(event.target.value)}
                  className="w-full border rounded px-2 py-1"
                  placeholder="AWB_NO"
                />
              </div>
              <div className="flex items-end">
                <Button size="sm" onClick={handleRegisterTextTable} disabled={!textTableFile}>
                  Register text sheet
                </Button>
              </div>
            </div>
            {textTableStatus && (
              <p className="text-xs text-gray-600">{textTableStatus}</p>
            )}
            {Object.keys(registeredTextTables).length > 0 && (
              <div className="text-xs text-gray-600">
                <div className="font-semibold">Registered text datasets</div>
                <ul className="list-disc list-inside space-y-1">
                  {Object.entries(registeredTextTables).map(([slug, meta]) => {
                    const info = meta as { name: string; key_column: string; row_count?: number }
                    return (
                      <li key={slug}>
                        <span className="font-medium">{info.name}</span> — key: {info.key_column} ({info.row_count ?? 0} rows)
                      </li>
                    )
                  })}
                </ul>
              </div>
            )}
          </div>

          <div className="md:col-span-2 lg:col-span-3 p-3 rounded border bg-white text-sm space-y-2">
            <div className="font-semibold text-gray-900">Target column</div>
            <input
              type="text"
              className="w-full border rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              value={targetColumn}
              onChange={(e) => handleTargetColumnChange(e.target.value)}
              placeholder="Type or select the column that represents your outcome (e.g. STATUS_Return)"
            />
            <p className="text-xs text-gray-600">
              Pick from the AI suggestions below or type the exact column name. This value is required for feature selection and model training.
            </p>
            {hasBlockedPhases && (
              <Alert className="border-amber-200 bg-amber-50 text-amber-800">
                <AlertDescription className="text-xs">
                  Some advanced phases are paused until you confirm the target column above. Choose a column (or take an AI suggestion) to unlock them.
                </AlertDescription>
              </Alert>
            )}
            {targetSuggestionError && (
              <p className="text-xs text-red-600">{targetSuggestionError}</p>
            )}
          </div>

          {/* Simple prompt if we auto-suggested a target */}
          {autoSuggestedTarget && targetColumn === autoSuggestedTarget && (
            <div className="md:col-span-2 lg:col-span-3 p-3 rounded border bg-yellow-50 text-sm text-yellow-900">
              Suggested target column: <span className="font-semibold">{autoSuggestedTarget}</span> — you can change it above before running advanced phases.
            </div>
          )}
          {llmCandidates && llmCandidates.length > 0 && (
            <div className="md:col-span-2 lg:col-span-3 p-3 rounded border bg-purple-50">
              <div className="text-sm font-semibold text-purple-900 mb-2">AI suggested targets</div>
              <div className="space-y-2">
                {llmCandidates.map((c) => (
                  <label key={c.name} className="flex items-start gap-3 text-sm">
                    <input
                      type="radio"
                      name="ai-target"
                      className="mt-1"
                      checked={targetColumn === c.name}
                      onChange={() => handleTargetColumnChange(c.name)}
                    />
                    <span>
                      <span className="font-medium">{c.name}</span>
                      {typeof c.nunique !== 'undefined' && (
                        <span className="ml-2 text-gray-500">(nunique: {c.nunique})</span>
                      )}
                      {c.confidence && (
                        <span className="ml-2 text-gray-500">confidence: {c.confidence}</span>
                      )}
                      {c.reason && (
                        <div className="text-gray-700">{c.reason}</div>
                      )}
                    </span>
                  </label>
                ))}
              </div>
            </div>
          )}
          {llmCandidates.length === 0 && availableColumns.length > 0 && (
            <div className="md:col-span-2 lg:col-span-3 p-3 rounded border bg-blue-50 text-sm">
              <div className="font-semibold mb-2">Select target column</div>
              <select
                className="border rounded px-2 py-1"
                value={targetColumn}
                onChange={(e) => handleTargetColumnChange(e.target.value)}
              >
                {availableColumns.map((n) => (
                  <option key={n} value={n}>{n}</option>
                ))}
              </select>
            </div>
          )}
          {PHASES.map((phase) => {
            const result = phaseResults[phase.id]
            const isActive = currentPhase === phase.id
            
            return (
              <Card key={phase.id}               className={`
                ${isActive ? 'border-blue-500 shadow-lg' : ''}
                ${result?.status === 'success' ? 'border-green-200 bg-green-50' : ''}
                  {result?.status === 'blocked' && (
                    <Alert className="bg-amber-50 border-amber-200 text-amber-800 text-xs mt-2">
                      <AlertDescription>
                        Target-dependent phase paused. Select the target column above to run this step.
                      </AlertDescription>
                    </Alert>
                  )}
                ${result?.status === 'simulated' ? 'border-yellow-200 bg-yellow-50' : ''}
                ${result?.status === 'error' ? 'border-red-200 bg-red-50' : ''}
                ${result?.status === 'blocked' ? 'border-gray-300 bg-gray-100' : ''}
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
                        {typeof result.error === 'string' ? result.error : JSON.stringify(result.error)}
                      </AlertDescription>
                    </Alert>
                  )}
                  
                        {result?.status === 'success' && (
                          <div className="mt-2">
                            <div className="text-xs text-green-700 mb-2">
                              ✅ Completed successfully
                            </div>
                            {result.data && (
                              <div className="text-xs bg-gray-100 p-2 rounded max-h-32 overflow-y-auto">
                                <div className="font-semibold mb-1">Results:</div>
                                <pre className="whitespace-pre-wrap text-xs">
                                  {JSON.stringify(result.data || {}, null, 2)}
                                </pre>
                              </div>
                            )}
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
              <div className="mt-4">
                <h3 className="font-semibold text-lg mb-4">📊 Detailed Results by Phase:</h3>
                <div className="space-y-4">
                  {Object.entries(phaseResults).map(([phaseId, result]) => {
                    const phase = PHASES.find(p => p.id === phaseId)
                    return (
                      <div key={phaseId} className="border rounded-lg p-4">
                        <h4 className="font-semibold text-blue-800 mb-2">
                          {phase?.name} - {result.status === 'success' ? '✅ Success' : result.status === 'simulated' ? '⚡ Simulated' : result.status === 'blocked' ? '⏸️ Awaiting Target' : '❌ Failed'}
                        </h4>
                        {result.data && (
                          <div className="bg-gray-50 p-3 rounded max-h-64 overflow-y-auto">
                            <div className="text-xs space-y-3">
                              {/* Phase 0: Quality Control */}
                              {phaseId === 'phase0' && (
                                <div>
                                  <div className="font-semibold mb-2 text-red-600">🔍 Quality Control Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-blue-50 p-2 rounded">
                                      <strong>Status:</strong> <span className={`font-bold ${result.data.status === 'PASS' ? 'text-green-600' : result.data.status === 'WARN' ? 'text-yellow-600' : 'text-red-600'}`}>{result.data.status}</span>
                                    </div>
                                    
                                    {result.data.missing_report && Object.keys(result.data.missing_report).length > 0 && (
                                      <div>
                                        <div className="font-semibold text-red-600 mb-1">Missing Data Analysis:</div>
                                        <div className="space-y-1">
                                          {Object.entries(result.data.missing_report)
                                            .filter(([_, pct]) => (pct as number) > 0)
                                            .slice(0, 5)
                                            .map(([col, pct]: [string, any]) => (
                                            <div key={col} className={`p-1 rounded text-xs ${pct > 0.2 ? 'bg-red-100' : pct > 0.05 ? 'bg-yellow-100' : 'bg-green-100'}`}>
                                              <strong>{col}:</strong> {(pct * 100).toFixed(1)}% missing
                                              {pct > 0.2 && <span className="text-red-600 ml-1">⚠️ Critical</span>}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.date_issues && Object.keys(result.data.date_issues).length > 0 && (
                                      <div>
                                        <div className="font-semibold text-blue-600 mb-1">Date Issues:</div>
                                        <div className="space-y-1">
                                          {Object.entries(result.data.date_issues).slice(0, 3).map(([col, issues]: [string, any]) => (
                                            <div key={col} className="bg-blue-100 p-1 rounded text-xs">
                                              <strong>{col}:</strong>
                                              {issues.future_dates > 0 && <span className="ml-1">Future: {issues.future_dates}</span>}
                                              {issues.inversions > 0 && <span className="ml-1">Inversions: {issues.inversions}</span>}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.key_issues && Object.keys(result.data.key_issues).length > 0 && (
                                      <div>
                                        <div className="font-semibold text-purple-600 mb-1">Key Issues:</div>
                                        <div className="space-y-1">
                                          {Object.entries(result.data.key_issues).slice(0, 3).map(([key, issues]: [string, any]) => (
                                            <div key={key} className="bg-purple-100 p-1 rounded text-xs">
                                              <strong>{key}:</strong> {issues}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.warnings && result.data.warnings.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-yellow-600 mb-1">Warnings ({result.data.warnings.length}):</div>
                                        {result.data.warnings.slice(0, 3).map((warning: string, idx: number) => (
                                          <div key={idx} className="bg-yellow-100 p-1 rounded text-xs">
                                            ⚠️ {warning}
                                          </div>
                                        ))}
                                      </div>
                                    )}
                                    
                                    {result.data.errors && result.data.errors.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-red-600 mb-1">Errors ({result.data.errors.length}):</div>
                                        {result.data.errors.slice(0, 2).map((error: string, idx: number) => (
                                          <div key={idx} className="bg-red-100 p-1 rounded text-xs">
                                            ❌ {error}
                                          </div>
                                        ))}
                                      </div>
                                    )}
                                    
                                    {result.data.fixes_applied && result.data.fixes_applied.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-green-600 mb-1">Auto-Fixes Applied ({result.data.fixes_applied.length}):</div>
                                        {result.data.fixes_applied.map((fix: string, idx: number) => (
                                          <div key={idx} className="bg-green-100 p-1 rounded text-xs">
                                            ✅ {fix}
                                          </div>
                                        ))}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 1: Goal & KPIs */}
                              {phaseId === 'phase1' && (
                                <div>
                                  <div className="font-semibold mb-2 text-blue-600">🎯 Goal & KPIs Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-blue-50 p-2 rounded">
                                      <strong>Domain:</strong> <span className="font-bold text-blue-600">{result.data.domain}</span>
                                    </div>
                                    
                                    {result.data.kpis && result.data.kpis.length > 0 && (
                                      <div>
                                        <div className="flex items-center gap-2 mb-1">
                                          <div className="font-semibold text-blue-600">🤖 AI-Generated KPIs ({result.data.kpis.length}):</div>
                                          <div className="text-xs bg-purple-100 text-purple-700 px-2 py-1 rounded-full">
                                            AI-Powered
                                          </div>
                                        </div>
                                        <div className="grid grid-cols-1 gap-1">
                                          {result.data.kpis.map((kpi: string, idx: number) => (
                                            <div key={idx} className="bg-gradient-to-r from-blue-50 to-purple-50 border border-blue-200 p-2 rounded text-xs">
                                              <div className="flex items-center gap-2">
                                                <div className="w-2 h-2 bg-blue-500 rounded-full"></div>
                                                <span className="font-medium text-gray-800">{kpi}</span>
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                        <div className="mt-2 text-xs text-gray-600 bg-gray-50 p-2 rounded">
                                          💡 These KPIs were intelligently selected by AI based on your data structure and domain characteristics
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.compatibility && (
                                      <div>
                                        <div className="font-semibold text-blue-600 mb-1">Domain Compatibility:</div>
                                        <div className="bg-gray-100 p-2 rounded text-xs">
                                          <div><strong>Status:</strong> <span className={`font-bold ${result.data.compatibility.status === 'OK' ? 'text-green-600' : result.data.compatibility.status === 'WARN' ? 'text-yellow-600' : 'text-red-600'}`}>{result.data.compatibility.status}</span></div>
                                          <div><strong>Match:</strong> {(result.data.compatibility.match_percentage * 100).toFixed(1)}%</div>
                                          <div><strong>Message:</strong> {result.data.compatibility.message}</div>
                                        </div>
                                        
                                        {result.data.compatibility.matched_columns && result.data.compatibility.matched_columns.length > 0 && (
                                          <div className="mt-2">
                                            <div className="font-semibold text-green-600 mb-1">Matched Columns ({result.data.compatibility.matched_columns.length}):</div>
                                            <div className="grid grid-cols-2 gap-1">
                                              {result.data.compatibility.matched_columns.slice(0, 4).map((col: string, idx: number) => (
                                                <div key={idx} className="bg-green-100 p-1 rounded text-xs">
                                                  ✅ {col}
                                                </div>
                                              ))}
                                            </div>
                                          </div>
                                        )}
                                        
                                        {result.data.compatibility.missing_columns && result.data.compatibility.missing_columns.length > 0 && (
                                          <div className="mt-2">
                                            <div className="font-semibold text-red-600 mb-1">Missing Columns ({result.data.compatibility.missing_columns.length}):</div>
                                            <div className="grid grid-cols-2 gap-1">
                                              {result.data.compatibility.missing_columns.slice(0, 4).map((col: string, idx: number) => (
                                                <div key={idx} className="bg-red-100 p-1 rounded text-xs">
                                                  ❌ {col}
                                                </div>
                                              ))}
                                            </div>
                                          </div>
                                        )}
                                        
                                        {result.data.compatibility.suggestions && Object.keys(result.data.compatibility.suggestions).length > 0 && (
                                          <div className="mt-2">
                                            <div className="font-semibold text-yellow-600 mb-1">Alternative Domains:</div>
                                            <div className="space-y-1">
                                              {Object.entries(result.data.compatibility.suggestions)
                                                .filter(([_, score]) => (score as number) > 0)
                                                .slice(0, 3)
                                                .map(([domain, score]: [string, any]) => (
                                                <div key={domain} className="bg-yellow-100 p-1 rounded text-xs">
                                                  <strong>{domain}:</strong> {(score * 100).toFixed(1)}% match
                                                </div>
                                              ))}
                                            </div>
                                          </div>
                                        )}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 2: Data Ingestion */}
                              {phaseId === 'phase2' && (
                                <div>
                                  <div className="font-semibold mb-2 text-green-600">📁 Data Ingestion Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-green-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    
                                    {result.data.rows && (
                                      <div className="bg-blue-50 p-2 rounded text-xs">
                                        <div><strong>Dataset Size:</strong> {result.data.rows.toLocaleString()} rows × {result.data.columns} columns</div>
                                        <div><strong>File Size:</strong> {result.data.file_size_mb?.toFixed(2)} MB</div>
                                        <div><strong>Parquet Path:</strong> {result.data.parquet_path}</div>
                                      </div>
                                    )}
                                    
                                    {result.data.column_names && result.data.column_names.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-green-600 mb-1">Columns ({result.data.column_names.length}):</div>
                                        <div className="grid grid-cols-2 gap-1">
                                          {result.data.column_names.slice(0, 6).map((col: string, idx: number) => (
                                            <div key={idx} className="bg-green-100 p-1 rounded text-xs">
                                              {col}
                                            </div>
                                          ))}
                                          {result.data.column_names.length > 6 && (
                                            <div className="bg-gray-100 p-1 rounded text-xs">
                                              +{result.data.column_names.length - 6} more...
                                            </div>
                                          )}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.compression_ratio && (
                                      <div className="bg-gray-100 p-2 rounded text-xs">
                                        <div><strong>Compression Ratio:</strong> {result.data.compression_ratio.toFixed(2)}x</div>
                                        <div><strong>Ingestion Time:</strong> {result.data.ingestion_time_seconds?.toFixed(2)}s</div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 3: Schema Discovery */}
                              {phaseId === 'phase3' && (
                                <div>
                                  <div className="font-semibold mb-2 text-purple-600">🔍 Schema Discovery Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-purple-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    
                                    {result.data.dtypes && Object.keys(result.data.dtypes).length > 0 && (
                                      <div>
                                        <div className="font-semibold text-purple-600 mb-1">Data Types:</div>
                                        <div className="space-y-1">
                                          {Object.entries(result.data.dtypes).slice(0, 5).map(([col, dtype]: [string, any]) => (
                                            <div key={col} className="bg-purple-100 p-1 rounded text-xs">
                                              <strong>{col}:</strong> {dtype}
                                            </div>
                                          ))}
                                          {Object.keys(result.data.dtypes).length > 5 && (
                                            <div className="bg-gray-100 p-1 rounded text-xs">
                                              +{Object.keys(result.data.dtypes).length - 5} more columns...
                                            </div>
                                          )}
                                        </div>
                                      </div>
                                    )}
                                    
                                    <div className="grid grid-cols-2 gap-2 text-xs">
                                      {result.data.numeric_columns && (
                                        <div className="bg-blue-50 p-2 rounded">
                                          <div className="font-semibold text-blue-600">Numeric:</div>
                                          <div>{result.data.numeric_columns.length} columns</div>
                                        </div>
                                      )}
                                      
                                      {result.data.categorical_columns && (
                                        <div className="bg-green-50 p-2 rounded">
                                          <div className="font-semibold text-green-600">Categorical:</div>
                                          <div>{result.data.categorical_columns.length} columns</div>
                                        </div>
                                      )}
                                      
                                      {result.data.datetime_columns && (
                                        <div className="bg-yellow-50 p-2 rounded">
                                          <div className="font-semibold text-yellow-600">DateTime:</div>
                                          <div>{result.data.datetime_columns.length} columns</div>
                                        </div>
                                      )}
                                      
                                      {result.data.id_columns && (
                                        <div className="bg-red-50 p-2 rounded">
                                          <div className="font-semibold text-red-600">ID Columns:</div>
                                          <div>{result.data.id_columns.length} columns</div>
                                        </div>
                                      )}
                                    </div>
                                    
                                    {result.data.violations_pct !== undefined && (
                                      <div className="bg-gray-100 p-2 rounded text-xs">
                                        <div><strong>Schema Violations:</strong> {result.data.violations_pct.toFixed(2)}%</div>
                                      </div>
                                    )}
                                    
                                    {result.data.warnings && result.data.warnings.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-yellow-600 mb-1">Warnings:</div>
                                        {result.data.warnings.slice(0, 2).map((warning: string, idx: number) => (
                                          <div key={idx} className="bg-yellow-100 p-1 rounded text-xs">
                                            ⚠️ {warning}
                                          </div>
                                        ))}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 4: Data Profiling */}
                              {phaseId === 'phase4' && (
                                <div>
                                  <div className="font-semibold mb-2 text-orange-600">📊 Data Profiling Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-orange-50 p-2 rounded">
                                      <strong>Dataset Size:</strong> {result.data.total_rows?.toLocaleString()} rows × {result.data.total_columns} columns
                                    </div>
                                    
                                    {result.data.memory_usage_mb && (
                                      <div className="bg-gray-100 p-2 rounded text-xs">
                                        <strong>Memory Usage:</strong> {result.data.memory_usage_mb.toFixed(2)} MB
                                      </div>
                                    )}
                                    
                                    {result.data.missing_summary && (
                                      <div className="bg-red-50 p-2 rounded text-xs">
                                        <div><strong>Missing Data:</strong> {result.data.missing_summary.total_missing?.toLocaleString()} values ({(result.data.missing_summary.missing_percentage || 0).toFixed(1)}%)</div>
                                        <div><strong>Complete Rows:</strong> {result.data.missing_summary.complete_rows?.toLocaleString()}</div>
                                      </div>
                                    )}
                                    
                                    {result.data.numeric_summary && Object.keys(result.data.numeric_summary).length > 0 && (
                                      <div>
                                        <div className="font-semibold text-orange-600 mb-1">Numeric Columns ({Object.keys(result.data.numeric_summary).length}):</div>
                                        <div className="space-y-1">
                                          {Object.entries(result.data.numeric_summary).slice(0, 4).map(([col, stats]: [string, any]) => (
                                            <div key={col} className="bg-orange-100 p-1 rounded text-xs">
                                              <strong>{col}:</strong>
                                              <div className="ml-2 text-xs">
                                                Mean: {typeof stats.mean === 'number' ? stats.mean.toFixed(2) : 'N/A'} | 
                                                Std: {typeof stats.std === 'number' ? stats.std.toFixed(2) : 'N/A'} | 
                                                Min: {typeof stats.min === 'number' ? stats.min.toFixed(2) : 'N/A'} | 
                                                Max: {typeof stats.max === 'number' ? stats.max.toFixed(2) : 'N/A'}
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.categorical_summary && Object.keys(result.data.categorical_summary).length > 0 && (
                                      <div>
                                        <div className="font-semibold text-orange-600 mb-1">Categorical Columns ({Object.keys(result.data.categorical_summary).length}):</div>
                                        <div className="space-y-1">
                                          {Object.entries(result.data.categorical_summary).slice(0, 4).map(([col, stats]: [string, any]) => (
                                            <div key={col} className="bg-orange-100 p-1 rounded text-xs">
                                              <strong>{col}:</strong>
                                              <div className="ml-2 text-xs">
                                                Unique: {stats.unique_values} | 
                                                Most Common: {stats.most_common_value} ({stats.most_common_count} times)
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.top_issues && result.data.top_issues.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-red-600 mb-1">Top Quality Issues ({result.data.top_issues.length}):</div>
                                        <div className="space-y-1">
                                          {result.data.top_issues.slice(0, 3).map((issue: any, idx: number) => (
                                            <div key={idx} className={`p-1 rounded text-xs ${issue.severity === 'critical' ? 'bg-red-100' : issue.severity === 'high' ? 'bg-orange-100' : 'bg-yellow-100'}`}>
                                              <strong>{issue.column}:</strong> {issue.description} ({issue.affected_rows} rows, {issue.affected_pct.toFixed(1)}%)
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.correlation_preview && Object.keys(result.data.correlation_preview).length > 0 && (
                                      <div>
                                        <div className="font-semibold text-blue-600 mb-1">Top Correlations:</div>
                                        <div className="space-y-1">
                                          {Object.entries(result.data.correlation_preview).slice(0, 3).map(([pair, corr]: [string, any]) => (
                                            <div key={pair} className="bg-blue-100 p-1 rounded text-xs">
                                              <strong>{pair}:</strong> {typeof corr === 'number' ? corr.toFixed(3) : JSON.stringify(corr)}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 5: Missing Data Analysis */}
                              {phaseId === 'phase5' && (
                                <div>
                                  <div className="font-semibold mb-2 text-red-600">❌ Missing Data Analysis:</div>
                                  <div className="space-y-2">
                                    <div className="bg-red-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    
                                    {result.data.record_completeness !== undefined && (
                                      <div className="bg-blue-50 p-2 rounded text-xs">
                                        <div><strong>Record Completeness:</strong> {(result.data.record_completeness * 100).toFixed(1)}%</div>
                                      </div>
                                    )}
                                    
                                    {result.data.decisions && result.data.decisions.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-red-600 mb-1">Imputation Decisions ({result.data.decisions.length}):</div>
                                        <div className="space-y-1">
                                          {result.data.decisions.slice(0, 4).map((decision: any, idx: number) => (
                                            <div key={idx} className="bg-red-100 p-1 rounded text-xs">
                                              <strong>{decision.column}:</strong> {decision.method}
                                              <div className="ml-2 text-xs">
                                                {decision.missing_before} → {decision.missing_after} missing | {decision.reason}
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.validation && Object.keys(result.data.validation).length > 0 && (
                                      <div>
                                        <div className="font-semibold text-blue-600 mb-1">Validation Results:</div>
                                        <div className="space-y-1">
                                          {Object.entries(result.data.validation).slice(0, 3).map(([col, metrics]: [string, any]) => (
                                            <div key={col} className="bg-blue-100 p-1 rounded text-xs">
                                              <strong>{col}:</strong>
                                              <div className="ml-2 text-xs">
                                                PSI: {metrics.psi?.toFixed(3)} | KS: {metrics.ks_statistic?.toFixed(3)} | 
                                                <span className={`ml-1 ${metrics.passed ? 'text-green-600' : 'text-red-600'}`}>
                                                  {metrics.passed ? '✅ Passed' : '❌ Failed'}
                                                </span>
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.warnings && result.data.warnings.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-yellow-600 mb-1">Warnings ({result.data.warnings.length}):</div>
                                        {result.data.warnings.slice(0, 2).map((warning: string, idx: number) => (
                                          <div key={idx} className="bg-yellow-100 p-1 rounded text-xs">
                                            ⚠️ {warning}
                                          </div>
                                        ))}
                                      </div>
                                    )}
                                    
                                    {result.data.status && (
                                      <div className={`p-2 rounded text-xs ${result.data.status === 'PASS' ? 'bg-green-50' : result.data.status === 'WARN' ? 'bg-yellow-50' : 'bg-red-50'}`}>
                                        <strong>Final Status:</strong> <span className={`font-bold ${result.data.status === 'PASS' ? 'text-green-600' : result.data.status === 'WARN' ? 'text-yellow-600' : 'text-red-600'}`}>{result.data.status}</span>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 6: Standardization */}
                              {phaseId === 'phase6' && (
                                <div>
                                  <div className="font-semibold mb-2 text-indigo-600">🔧 Standardization Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-indigo-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    {result.data.standardization_info && (
                                      <div className="space-y-1 text-xs">
                                        <div><strong>Cleaned columns:</strong> {result.data.standardization_info.cleaned_columns}</div>
                                        <div><strong>Standardized formats:</strong> {result.data.standardization_info.standardized_formats}</div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 7: Feature Engineering */}
                              {phaseId === 'phase7' && (
                                <div>
                                  <div className="font-semibold mb-2 text-teal-600">⚙️ Feature Engineering Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-teal-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    {result.data.features_info && (
                                      <div className="space-y-1 text-xs">
                                        <div><strong>New features created:</strong> {result.data.features_info.new_features_count}</div>
                                        <div><strong>Feature types:</strong> {result.data.features_info.feature_types?.join(', ')}</div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 7.5: Encoding & Scaling */}
                              {phaseId === 'phase7_5' && (
                                <div>
                                  <div className="font-semibold mb-2 text-cyan-600">🔢 Encoding & Scaling Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-cyan-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    
                                    {result.data.encoding_configs && result.data.encoding_configs.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-cyan-600 mb-1">Encoding Configurations ({result.data.encoding_configs.length}):</div>
                                        <div className="space-y-1">
                                          {result.data.encoding_configs.slice(0, 4).map((config: any, idx: number) => (
                                            <div key={idx} className="bg-cyan-100 p-1 rounded text-xs">
                                              <strong>{config.column}:</strong> {config.method}
                                              <div className="ml-2 text-xs">
                                                Cardinality: {config.cardinality} | {config.reason}
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.scaling_config && (
                                      <div>
                                        <div className="font-semibold text-cyan-600 mb-1">Scaling Configuration:</div>
                                        <div className="bg-cyan-100 p-2 rounded text-xs">
                                          <div><strong>Method:</strong> {result.data.scaling_config.method}</div>
                                          <div><strong>Columns:</strong> {result.data.scaling_config.columns?.length || 0} columns</div>
                                          <div><strong>Reason:</strong> {result.data.scaling_config.reason}</div>
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.artifacts_saved && result.data.artifacts_saved.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-green-600 mb-1">Artifacts Saved ({result.data.artifacts_saved.length}):</div>
                                        <div className="space-y-1">
                                          {result.data.artifacts_saved.map((artifact: string, idx: number) => (
                                            <div key={idx} className="bg-green-100 p-1 rounded text-xs">
                                              💾 {artifact}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.encoded_columns && result.data.encoded_columns.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-cyan-600 mb-1">Encoded Columns ({result.data.encoded_columns.length}):</div>
                                        <div className="grid grid-cols-2 gap-1">
                                          {result.data.encoded_columns.slice(0, 6).map((col: string, idx: number) => (
                                            <div key={idx} className="bg-cyan-100 p-1 rounded text-xs">
                                              🔢 {col}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.scaled_columns && result.data.scaled_columns.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-cyan-600 mb-1">Scaled Columns ({result.data.scaled_columns.length}):</div>
                                        <div className="grid grid-cols-2 gap-1">
                                          {result.data.scaled_columns.slice(0, 6).map((col: string, idx: number) => (
                                            <div key={idx} className="bg-cyan-100 p-1 rounded text-xs">
                                              📏 {col}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 8: Data Merging */}
                              {phaseId === 'phase8' && (
                                <div>
                                  <div className="font-semibold mb-2 text-pink-600">🔗 Data Merging Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-pink-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    {result.data.merge_info && (
                                      <div className="space-y-1 text-xs">
                                        <div><strong>Merged datasets:</strong> {result.data.merge_info.merged_datasets}</div>
                                        <div><strong>Final rows:</strong> {result.data.merge_info.final_rows?.toLocaleString()}</div>
                                        <div><strong>Duplicates found:</strong> {result.data.merge_info.duplicates_found}</div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 9: Correlation Analysis */}
                              {phaseId === 'phase9' && (
                                <div>
                                  <div className="font-semibold mb-2 text-green-600">🔗 Correlation Analysis Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-green-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    
                                    {result.data.total_tests && (
                                      <div className="bg-gray-100 p-2 rounded text-xs">
                                        <div><strong>Total Tests:</strong> {result.data.total_tests}</div>
                                        <div><strong>FDR Applied:</strong> {result.data.fdr_applied ? 'Yes' : 'No'}</div>
                                      </div>
                                    )}
                                    
                                    {result.data.numeric_correlations && result.data.numeric_correlations.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-green-600 mb-1">Numeric Correlations ({result.data.numeric_correlations.length}):</div>
                                        <div className="space-y-1">
                                          {result.data.numeric_correlations.slice(0, 4).map((corr: any, idx: number) => (
                                            <div key={idx} className="bg-green-100 p-1 rounded text-xs">
                                              <strong>{corr.feature1} &harr; {corr.feature2}:</strong>
                                              <div className="ml-2 text-xs">
                                                {corr.method}: {typeof corr.correlation === 'number' ? corr.correlation.toFixed(3) : JSON.stringify(corr.correlation)} | 
                                                p-value: {typeof corr.p_value === 'number' ? corr.p_value.toFixed(3) : JSON.stringify(corr.p_value)} | 
                                                n: {corr.n}
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.categorical_associations && result.data.categorical_associations.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-blue-600 mb-1">Categorical Associations ({result.data.categorical_associations.length}):</div>
                                        <div className="space-y-1">
                                          {result.data.categorical_associations.slice(0, 3).map((assoc: any, idx: number) => (
                                            <div key={idx} className="bg-blue-100 p-1 rounded text-xs">
                                              <strong>{assoc.feature1} &harr; {assoc.feature2}:</strong>
                                              <div className="ml-2 text-xs">
                                                {assoc.method}: {typeof assoc.correlation === 'number' ? assoc.correlation.toFixed(3) : JSON.stringify(assoc.correlation)} | 
                                                p-value: {typeof assoc.p_value === 'number' ? assoc.p_value.toFixed(3) : JSON.stringify(assoc.p_value)} | 
                                                n: {assoc.n}
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.strong_correlations && result.data.strong_correlations.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-red-600 mb-1">Strong Correlations (|r| &gt; 0.7):</div>
                                        <div className="space-y-1">
                                          {result.data.strong_correlations.slice(0, 3).map((corr: any, idx: number) => (
                                            <div key={idx} className="bg-red-100 p-1 rounded text-xs">
                                              <strong>{corr.feature1} &harr; {corr.feature2}:</strong> {typeof corr.correlation === 'number' ? corr.correlation.toFixed(3) : JSON.stringify(corr.correlation)}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.moderate_correlations && result.data.moderate_correlations.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-yellow-600 mb-1">Moderate Correlations (0.3 &lt; |r| &le; 0.7):</div>
                                        <div className="space-y-1">
                                          {result.data.moderate_correlations.slice(0, 3).map((corr: any, idx: number) => (
                                            <div key={idx} className="bg-yellow-100 p-1 rounded text-xs">
                                              <strong>{corr.feature1} &harr; {corr.feature2}:</strong> {typeof corr.correlation === 'number' ? corr.correlation.toFixed(3) : JSON.stringify(corr.correlation)}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 9.5: Business Validation */}
                              {phaseId === 'phase9_5' && (
                                <div>
                                  <div className="font-semibold mb-2 text-yellow-600">✅ Business Validation Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-yellow-50 p-2 rounded">
                                      <strong>Status:</strong> <span className={`font-bold ${result.data.status === 'PASS' ? 'text-green-600' : result.data.status === 'WARN' ? 'text-yellow-600' : 'text-red-600'}`}>{result.data.status}</span>
                                    </div>
                                    
                                    {result.data.conflicts_detected && result.data.conflicts_detected.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-red-600 mb-1">Business Conflicts Detected ({result.data.conflicts_detected.length}):</div>
                                        <div className="space-y-1">
                                          {result.data.conflicts_detected.slice(0, 3).map((conflict: any, idx: number) => (
                                            <div key={idx} className={`p-1 rounded text-xs ${conflict.conflict_severity === 'high' ? 'bg-red-100' : conflict.conflict_severity === 'medium' ? 'bg-yellow-100' : 'bg-blue-100'}`}>
                                              <strong>{conflict.feature1} ↔ {conflict.feature2}:</strong>
                                              <div className="ml-2 text-xs">
                                                Observed: {conflict.observed_correlation.toFixed(3)} | Expected: {conflict.expected_relationship} | 
                                                Severity: {conflict.conflict_severity} | Resolution: {conflict.resolution}
                                              </div>
                                              {conflict.llm_hypothesis && (
                                                <div className="ml-2 text-xs mt-1 bg-gray-50 p-1 rounded">
                                                  <strong>LLM Hypothesis:</strong> {conflict.llm_hypothesis}
                                                </div>
                                              )}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                    
                                    {result.data.llm_hypotheses_generated !== undefined && (
                                      <div className="bg-blue-50 p-2 rounded text-xs">
                                        <div><strong>LLM Hypotheses Generated:</strong> {result.data.llm_hypotheses_generated}</div>
                                      </div>
                                    )}
                                    
                                    {result.data.conflicts_detected && result.data.conflicts_detected.length === 0 && (
                                      <div className="bg-green-50 p-2 rounded text-xs">
                                        <div><strong>✅ No Business Conflicts Detected</strong></div>
                                        <div>All correlations align with domain expectations</div>
                                      </div>
                                    )}
                                    
                                    {result.data.resolution_summary && (
                                      <div className="bg-gray-100 p-2 rounded text-xs">
                                        <div><strong>Resolution Summary:</strong></div>
                                        <div>Accepted: {result.data.resolution_summary.accepted || 0} | Vetoed: {result.data.resolution_summary.vetoed || 0} | Pending: {result.data.resolution_summary.pending || 0}</div>
                                      </div>
                                    )}
                                    
                                    {result.data.domain_insights && result.data.domain_insights.length > 0 && (
                                      <div>
                                        <div className="font-semibold text-purple-600 mb-1">Domain Insights:</div>
                                        <div className="space-y-1">
                                          {result.data.domain_insights.slice(0, 2).map((insight: string, idx: number) => (
                                            <div key={idx} className="bg-purple-100 p-1 rounded text-xs">
                                              💡 {insight}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 10: Packaging */}
                              {phaseId === 'phase10' && (
                                <div>
                                  <div className="font-semibold mb-2 text-gray-600">📦 Data Packaging Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-gray-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    {result.data?.packaging_info && (
                                      <div className="space-y-1 text-xs">
                                        <div><strong>Files created:</strong> {result.data.packaging_info.files_created}</div>
                                        <div><strong>Total size:</strong> {result.data.packaging_info.total_size_mb?.toFixed(2)} MB</div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 11: Advanced Analysis */}
                              {phaseId === 'phase11' && (
                                <div>
                                  <div className="font-semibold mb-2 text-purple-600">🔬 Advanced Analysis Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-purple-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    {result.data.advanced_analysis && (
                                      <div className="space-y-1 text-xs">
                                        <div><strong>Analysis type:</strong> {result.data.advanced_analysis.analysis_type}</div>
                                        <div><strong>Insights found:</strong> {result.data.advanced_analysis.insights_count}</div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Phase 12: Monitoring */}
                              {phaseId === 'phase12' && (
                                <div>
                                  <div className="font-semibold mb-2 text-blue-600">📊 Monitoring Setup Results:</div>
                                  <div className="space-y-2">
                                    <div className="bg-blue-50 p-2 rounded">
                                      <strong>Status:</strong> <span className="font-bold text-green-600">Success</span>
                                    </div>
                                    {result.data.monitoring_info && (
                                      <div className="space-y-1 text-xs">
                                        <div><strong>Monitoring enabled:</strong> {result.data.monitoring_info.monitoring_enabled ? 'Yes' : 'No'}</div>
                                        <div><strong>Alerts configured:</strong> {result.data.monitoring_info.alerts_configured}</div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              )}
                              
                              {/* Fallback for unknown phases */}
                              {!['phase0', 'phase1', 'phase2', 'phase3', 'phase4', 'phase5', 'phase6', 'phase7', 'phase7_5', 'phase8', 'phase9', 'phase9_5', 'phase10', 'phase10_5', 'phase11', 'phase11_5', 'phase12', 'phase13', 'phase14', 'phase14.5'].includes(phaseId) && (
                                <div>
                                  <div className="font-semibold mb-2">📄 Raw Results:</div>
                                  <pre className="whitespace-pre-wrap text-xs bg-gray-100 p-2 rounded">
                                    {JSON.stringify(result.data || {}, null, 2)}
                                  </pre>
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                        {result.error && (
                          <div className="bg-red-50 p-3 rounded text-red-700 text-sm">
                            Error: {typeof result.error === 'string' ? result.error : JSON.stringify(result.error)}
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
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
                  {blockedPhases > 0 && (
                    <p>⏸️ <strong>Awaiting Target:</strong> {blockedPhases} phase{blockedPhases === 1 ? '' : 's'} paused until the target column is confirmed.</p>
                  )}
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
                          <div className="text-xs text-red-600 mt-1">{typeof result.error === 'string' ? result.error : JSON.stringify(result.error)}</div>
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
                  <p>✅ <strong>Real Analysis:</strong> {Object.values(phaseResults).filter(r => r?.status === 'success').length} phases with actual data processing</p>
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
      </div>
    </div>
  )
}


