import { useState, useEffect, useMemo } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Loader2, AlertTriangle, Info, Database } from "lucide-react"
import { getPipelineData, savePipelineData } from '@/lib/localStorage'
import { apiClient } from '@/api/client'

interface KPI {
  key: string
  label: string
  value: number
  format: string
}

    interface Recommendation {
      title: string
      description: string
      type: string
      category: string
      severity: string
      priority: number
      actionable: boolean
      estimated_impact: string
      implementation_effort: string
      confidence: number
      related_phases: string[]
      business_value: string
      evidence: string
      verification_needed: boolean
    }

interface Signals {
  meta: {
    domain: string
    time_window: string
    n: number
  }
  kpis: Record<string, number>
  quality: any
  distributions: any
  trends: any
}

interface FeatureMeta {
  name: string
  clean_name: string
  data_type: string
  semantic_type: string
  description: string
  unique_values: number
  uniqueness_ratio: number
  missing_pct: number
  recommended_role: string
  is_identifier: boolean
  is_target_candidate: boolean
}

interface KPIFormulaPayload {
  type: string
  format?: string
  [key: string]: any
}

interface KPIProposalItem {
  kpi_id: string
  name: string
  alias: string
  metric_type: string
  description: string
  rationale: string
  financial_impact?: string
  confidence?: number
  recommended_direction: 'higher_is_better' | 'lower_is_better' | 'target_range'
  formula?: KPIFormulaPayload | null
  required_columns: string[]
  supporting_evidence: string[]
  why_selected?: string
  expected_outcome?: string
  monitoring_guidance?: string
  tradeoffs?: string
  warnings: string[]
  source: 'llm' | 'system' | 'custom_slot'
  notes?: string
  editable: boolean
}

interface KPIProposalBundleResponse {
  proposals: KPIProposalItem[]
  warnings: string[]
  context_snapshot: Record<string, any>
  explanation?: string
}

interface KPIValidationResultItem {
  proposal: KPIProposalItem
  status: 'pass' | 'warn' | 'fail'
  computed_value?: number
  formatted_value?: string
  reason?: string | null
}

interface KPIValidationResponse {
  results: KPIValidationResultItem[]
  warnings: string[]
}

export default function BIDashboard() {
  const [question, setQuestion] = useState('')
  const [loading, setLoading] = useState(false)
  const [response, setResponse] = useState<any>(null)
  const [kpis, setKpis] = useState<KPI[]>([])
  const [accumulatedKpis, setAccumulatedKpis] = useState<KPI[]>([]) 
  const [recommendations, setRecommendations] = useState<Recommendation[]>([])
  const [aiRecommendations, setAiRecommendations] = useState<any>(null)
  const [selectedRecommendationType, setSelectedRecommendationType] = useState<string>('all')
  const [signals, setSignals] = useState<Signals | null>(null)
  const [domain, setDomain] = useState('logistics')
  const [pipelineData, setPipelineData] = useState<any>(null)
  const [featureDictionary, setFeatureDictionary] = useState<FeatureMeta[]>([])
  const [kpiBundle, setKpiBundle] = useState<KPIProposalBundleResponse | null>(null)
  const [kpiWarnings, setKpiWarnings] = useState<string[]>([])
  const [kpiLoading, setKpiLoading] = useState(false)
  const [kpiError, setKpiError] = useState<string | null>(null)
  const [kpiValidationResults, setKpiValidationResults] = useState<Record<string, KPIValidationResultItem>>({})
  const [kpiNameOverrides, setKpiNameOverrides] = useState<Record<string, string>>({})
  const [kpiNotes, setKpiNotes] = useState<Record<string, string>>({})
  const [kpiLastAction, setKpiLastAction] = useState<string | null>(null)

  const roleCounts = useMemo(() => {
    return featureDictionary.reduce((acc: Record<string, number>, meta) => {
      const role = meta.recommended_role || 'feature'
      acc[role] = (acc[role] || 0) + 1
      return acc
    }, {})
  }, [featureDictionary])

  const spotlightFeatures = useMemo(() => {
    return featureDictionary
      .filter((meta) => meta.is_target_candidate && !meta.is_identifier)
      .slice(0, 10)
  }, [featureDictionary])

  const dictionaryRows = useMemo(() => {
  if (spotlightFeatures.length > 0) {
    return spotlightFeatures
  }
  return featureDictionary.slice(0, 10)
}, [spotlightFeatures, featureDictionary])

  const slugify = (value: string) =>
    value.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '') || 'custom_kpi'

  const loadKpiProposals = async () => {
    setKpiLoading(true)
    setKpiError(null)
    try {
      const response = await apiClient.post('/bi/kpi-proposals', {
        domain,
        language: 'en',
        count: 3
      })
      const bundle: KPIProposalBundleResponse = response.data
      setKpiBundle(bundle)
      setKpiWarnings(bundle.warnings || [])
      const defaults: Record<string, string> = {}
      bundle.proposals.forEach((proposal) => {
        defaults[proposal.kpi_id] = proposal.name
      })
      setKpiNameOverrides(defaults)
      setKpiValidationResults({})
      setKpiNotes({})
      setKpiLastAction(null)
    } catch (error: any) {
      const message = error?.response?.data?.detail || error?.message || 'Failed to load KPI proposals.'
      setKpiError(message)
    } finally {
      setKpiLoading(false)
    }
  }

  const getAdjustedProposal = (proposal: KPIProposalItem): KPIProposalItem => {
    const overriddenName = kpiNameOverrides[proposal.kpi_id] ?? proposal.name
    const alias = slugify(overriddenName)
    return {
      ...proposal,
      name: overriddenName,
      alias
    }
  }

  const validateProposal = async (proposal: KPIProposalItem) => {
    try {
      const adjusted = getAdjustedProposal(proposal)
      const response = await apiClient.post(`/bi/kpi-proposals/validate?domain=${domain}`, {
        proposals: [adjusted]
      })
      const validation: KPIValidationResponse = response.data
      if (validation.results && validation.results.length > 0) {
        const result = validation.results[0]
        setKpiValidationResults((prev) => ({
          ...prev,
          [proposal.kpi_id]: result
        }))
        if (validation.warnings?.length) {
          setKpiWarnings(validation.warnings)
        }
      }
    } catch (error: any) {
      const message = error?.response?.data?.detail || error?.message || 'Failed to validate KPI.'
      setKpiError(message)
    }
  }

  const adoptProposal = async (proposal: KPIProposalItem) => {
    try {
      const adjusted = getAdjustedProposal(proposal)
      const notes = kpiNotes[proposal.kpi_id] || undefined
      await apiClient.post(`/bi/kpi-proposals/adopt?domain=${domain}`, {
        proposal: adjusted,
        adopted_name: adjusted.name,
        notes
      })

      const validation = kpiValidationResults[proposal.kpi_id]
      if (validation?.computed_value !== undefined) {
        const format = validation.formatted_value?.includes('%') ? 'percentage' : 'number'
        const newEntry: KPI = {
          key: adjusted.alias,
          label: adjusted.name,
          value: validation.computed_value,
          format
        }
        setAccumulatedKpis((prev) => {
          if (prev.some((item) => item.key === newEntry.key)) {
            return prev
          }
          return [...prev, newEntry]
        })
      }
      setKpiLastAction(`Recorded ${adjusted.name} in decision log.`)
    } catch (error: any) {
      const message = error?.response?.data?.detail || error?.message || 'Failed to record KPI decision.'
      setKpiError(message)
    }
  }

  const handleNameOverride = (proposal: KPIProposalItem, value: string) => {
    setKpiNameOverrides((prev) => ({
      ...prev,
      [proposal.kpi_id]: value
    }))
  }

  const handleNotesChange = (proposal: KPIProposalItem, value: string) => {
    setKpiNotes((prev) => ({
      ...prev,
      [proposal.kpi_id]: value
    }))
  }

  // Load pipeline data on mount
  useEffect(() => {
    const savedData = getPipelineData()
    if (savedData) {
      setPipelineData(savedData)
      setDomain(savedData.domain || 'logistics')  // Default to logistics to match the data
      console.log('📊 BI Dashboard: Loaded pipeline data from localStorage')
    }
    
    // Load accumulated KPIs from localStorage
    const savedKpis = localStorage.getItem('bi_accumulated_kpis')
    if (savedKpis) {
      try {
        setAccumulatedKpis(JSON.parse(savedKpis))
      } catch (e) {
        console.error('Failed to load accumulated KPIs:', e)
      }
    }
  }, [])

  // Load dashboard data when domain changes
  useEffect(() => {
    if (domain) {
      loadDashboardData()
    }
  }, [domain])

  useEffect(() => {
    if (domain) {
      loadKpiProposals()
    }
  }, [domain])

  // Save accumulated KPIs to localStorage whenever they change
  useEffect(() => {
    if (accumulatedKpis.length > 0) {
      localStorage.setItem('bi_accumulated_kpis', JSON.stringify(accumulatedKpis))
    }
  }, [accumulatedKpis])

  const handleDomainChange = (newDomain: string) => {
    setDomain(newDomain)
    savePipelineData({
      domain: newDomain,
      phaseResults: pipelineData?.phaseResults || {},
      progress: pipelineData?.progress || 0
    })
  }

  const getRecommendationTypeInfo = (type: string) => {
    const types = {
      data_insight: { label: 'Data Insights', icon: '📊', color: 'blue', bgColor: 'bg-blue-100', textColor: 'text-blue-700' },
      best_practice: { label: 'Best Practices', icon: '🏆', color: 'green', bgColor: 'bg-green-100', textColor: 'text-green-700' },
      hidden_pattern: { label: 'Hidden Patterns', icon: '🔍', color: 'purple', bgColor: 'bg-purple-100', textColor: 'text-purple-700' },
      user_check: { label: 'User Checks', icon: '✅', color: 'yellow', bgColor: 'bg-yellow-100', textColor: 'text-yellow-700' },
      future_action: { label: 'Future Actions', icon: '🚀', color: 'indigo', bgColor: 'bg-indigo-100', textColor: 'text-indigo-700' },
      warning: { label: 'Warnings', icon: '⚠️', color: 'red', bgColor: 'bg-red-100', textColor: 'text-red-700' }
    }
    return types[type as keyof typeof types] || { label: type, icon: '📋', color: 'gray', bgColor: 'bg-gray-100', textColor: 'text-gray-700' }
  }

  const getFilteredRecommendations = () => {
    if (!aiRecommendations?.recommendations) return []
    if (selectedRecommendationType === 'all') return aiRecommendations.recommendations
    return aiRecommendations.recommendations.filter((rec: Recommendation) => rec.type === selectedRecommendationType)
  }

  const getRecommendationsByType = () => {
    if (!aiRecommendations?.recommendations) return {}
    const grouped = aiRecommendations.recommendations.reduce((acc: any, rec: Recommendation) => {
      if (!acc[rec.type]) acc[rec.type] = []
      acc[rec.type].push(rec)
      return acc
    }, {})
    return grouped
  }

  // Extract KPIs from user query responses
  const extractKpisFromResponse = (response: any, question: string): KPI[] => {
    const newKpis: KPI[] = []
    
    // Extract from overview data
    if (response.chart && response.chart.data && response.chart.data.total_records) {
      const data = response.chart.data
      
      // Add key metrics as KPIs
      if (data.categorical_summary?.Showed_up) {
        const showUpRate = ((data.categorical_summary.Showed_up.most_common_counts[0] || 0) / data.total_records * 100)
        newKpis.push({
          key: 'show_up_rate',
          label: 'Show-up Rate',
          value: showUpRate,
          format: '%'
        })
      }
      
      if (data.categorical_summary?.Hipertension) {
        newKpis.push({
          key: 'hypertension_rate',
          label: 'Hypertension Rate',
          value: ((data.categorical_summary.Hipertension.most_common_counts[1] || 0) / data.total_records * 100),
          format: '%'
        })
      }
      
      if (data.categorical_summary?.Diabetes) {
        newKpis.push({
          key: 'diabetes_rate',
          label: 'Diabetes Rate',
          value: ((data.categorical_summary.Diabetes.most_common_counts[1] || 0) / data.total_records * 100),
          format: '%'
        })
      }
      
      if (data.categorical_summary?.Gender_F) {
        newKpis.push({
          key: 'female_patients',
          label: 'Female Patients',
          value: ((data.categorical_summary.Gender_F.most_common_counts[0] || 0) / data.total_records * 100),
          format: '%'
        })
      }
    }
    
    // Extract from bar chart data (neighborhood analysis)
    if (response.chart && response.chart.type === 'bar' && response.chart.data) {
      const totalAppointments = Object.values(response.chart.data).reduce((a, b) => (a as number) + (b as number), 0) as number
      const totalAreas = Object.keys(response.chart.data).length
      
      // Add total appointments KPI
      newKpis.push({
        key: 'total_appointments',
        label: 'Total Appointments',
        value: totalAppointments,
        format: ''
      })
      
      // Add average appointments per area KPI
      newKpis.push({
        key: 'avg_appointments_per_area',
        label: 'Avg Appointments per Area',
        value: Math.round(totalAppointments / totalAreas),
        format: ''
      })
      
      // Add top area KPI
      const topArea = Object.entries(response.chart.data).sort(([,a], [,b]) => (b as number) - (a as number))[0]
      if (topArea) {
        newKpis.push({
          key: 'top_area_appointments',
          label: `Top Area (${topArea[0]})`,
          value: topArea[1] as number,
          format: ''
        })
      }
    }
    
    // Extract from specific metric responses
    if (response.chart && response.chart.type === 'metric' && response.chart.value !== 'N/A') {
      const label = response.chart.title || question.substring(0, 30) + '...'
      newKpis.push({
        key: `metric_${Date.now()}`,
        label: label,
        value: parseFloat(response.chart.value) || 0,
        format: response.chart.meta?.format || ''
      })
    }
    
    return newKpis
  }

  // Add KPIs to accumulated list
  const addKpisToAccumulated = (newKpis: KPI[]) => {
    setAccumulatedKpis(prev => {
      // Avoid duplicates by checking key
      const existingKeys = new Set(prev.map(kpi => kpi.key))
      const uniqueNewKpis = newKpis.filter(kpi => !existingKeys.has(kpi.key))
      return [...prev, ...uniqueNewKpis]
    })
  }

  const loadDashboardData = async () => {
    console.log('🔄 Loading dashboard data for domain:', domain)

    try {
      const { data: kpiData } = await apiClient.get('/bi/kpis', { params: { domain } })
      setKpis(formatKPIs(kpiData, domain))
    } catch (error) {
      console.error('Failed to load KPIs:', error)
      setKpis([])
    }

    try {
      const { data: recData } = await apiClient.get('/bi/recommendations', { params: { domain } })
      setRecommendations(Array.isArray(recData) ? recData : [])
    } catch (error) {
      console.error('Failed to load recommendations:', error)
      setRecommendations([])
    }

    try {
      const { data: sigData } = await apiClient.get('/bi/signals', { params: { domain } })
      setSignals(sigData)
    } catch (error) {
      console.warn('Failed to load signals (optional):', error)
      setSignals(null)
    }

    try {
      console.log('🔍 Loading AI recommendations for domain:', domain)
      const { data: aiRecData } = await apiClient.get('/bi/ai-recommendations', { params: { domain } })
      console.log('✅ AI recommendations loaded:', aiRecData)
      setAiRecommendations(aiRecData)
    } catch (error) {
      console.error('❌ Failed to load AI recommendations:', error)
      setAiRecommendations(null)
    }

    try {
      const { data: featureMeta } = await apiClient.get('/bi/features/dictionary')
      setFeatureDictionary(Array.isArray(featureMeta) ? featureMeta : [])
    } catch (error: any) {
      if (error?.response?.status !== 404) {
        console.warn('Failed to load feature dictionary:', error)
      }
      setFeatureDictionary([])
    }
  }

  const formatKPIs = (data: any, domain: string): KPI[] => {
    const kpiConfigs: Record<string, any[]> = {
      logistics: [
        { key: 'sla_pct', label: 'SLA Achievement', format: '%' },
        { key: 'avg_transit_h', label: 'Avg Transit Time', format: 'h' },
        { key: 'rto_pct', label: 'RTO Rate', format: '%' },
        { key: 'total_shipments', label: 'Total Shipments', format: '' }
      ],
      healthcare: [
        { key: 'avg_los_days', label: 'Avg LOS', format: ' days' },
        { key: 'readmission_30d_pct', label: 'Readmission Rate', format: '%' },
        { key: 'bed_occupancy_pct', label: 'Bed Occupancy', format: '%' },
        { key: 'total_admissions', label: 'Total Admissions', format: '' }
      ],
      retail: [
        { key: 'gmv', label: 'GMV', format: ' SAR' },
        { key: 'aov', label: 'AOV', format: ' SAR' },
        { key: 'return_pct', label: 'Return Rate', format: '%' },
        { key: 'total_orders', label: 'Total Orders', format: '' }
      ],
      emarketing: [
        { key: 'ctr_pct', label: 'CTR', format: '%' },
        { key: 'conversion_pct', label: 'Conversion', format: '%' },
        { key: 'cac', label: 'CAC', format: ' SAR' },
        { key: 'roas', label: 'ROAS', format: 'x' }
      ],
      finance: [
        { key: 'npl_pct', label: 'NPL', format: '%' },
        { key: 'avg_balance', label: 'Avg Balance', format: ' SAR' },
        { key: 'liquidity_ratio', label: 'Liquidity Ratio', format: '' },
        { key: 'total_accounts', label: 'Total Accounts', format: '' }
      ]
    }

    const config = kpiConfigs[domain] || kpiConfigs.logistics
    return config.map((c: any) => ({
      ...c,
      value: data[c.key] || 0
    }))
  }

  const askQuestion = async () => {
    if (!question.trim()) return

    setLoading(true)
    try {
      const formData = new FormData()
      formData.append('question', question)
      formData.append('domain', domain)
      formData.append('time_window', signals?.meta.time_window || '2024-01-01..2024-12-31')

      const res = await apiClient.post('/bi/ask', formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      })

      const data = res.data
      setResponse(data)
      
      // Extract KPIs from response and add to accumulated list
      const newKpis = extractKpisFromResponse(data, question)
      if (newKpis.length > 0) {
        addKpisToAccumulated(newKpis)
      }
    } catch (error) {
      console.error('Failed to process question:', error)
    } finally {
      setLoading(false)
    }
  }

  const getSeverityColor = (severity: string) => {
    return severity === 'high' ? 'border-red-500' :
           severity === 'medium' ? 'border-yellow-500' : 'border-blue-500'
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6" dir={response?.language === 'ar' ? 'rtl' : 'ltr'}>
      <div className="max-w-7xl mx-auto space-y-6">
        
        {/* Pipeline Data Status */}
        {pipelineData && (
          <Alert>
            <Database className="h-4 w-4" />
            <AlertDescription>
              <div className="flex items-center justify-between">
                <span>
                  Using pipeline data from: <strong>{pipelineData.selectedFile?.name || 'Unknown'}</strong> 
                  ({Object.keys(pipelineData.phaseResults || {}).length} phases completed)
                </span>
                <Badge variant="outline">
                  Domain: {pipelineData.domain || 'logistics'}
                </Badge>
              </div>
            </AlertDescription>
          </Alert>
        )}
        
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold">Business Intelligence Dashboard</h1>
            {signals && (
              <p className="text-sm text-gray-500 mt-1">
                {signals.meta.domain} • {signals.meta.time_window} • n={signals.meta.n.toLocaleString()}
              </p>
            )}
          </div>
          <select 
            value={domain} 
            onChange={(e) => handleDomainChange(e.target.value)}
            className="px-4 py-2 border rounded"
          >
            <option value="logistics">Logistics</option>
            <option value="healthcare">Healthcare</option>
            <option value="retail">Retail</option>
            <option value="emarketing">E-Marketing</option>
            <option value="finance">Finance</option>
          </select>
        </div>

        {/* KPI Cards */}
        <div className="space-y-4">
          {kpis.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-gray-800 mb-3">Domain KPIs</h3>
              <div className="grid grid-cols-4 gap-4">
                {kpis.map((kpi, idx) => (
                  <Card key={`domain-${idx}`}>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-gray-600">
                        {kpi.label}
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="flex items-baseline gap-2">
                        <span className="text-3xl font-bold">
                          {kpi.format === '%' ? kpi.value.toFixed(1) : 
                           kpi.format.includes('SAR') ? kpi.value.toLocaleString() :
                           kpi.value.toFixed(kpi.format === '' ? 0 : 1)}
                        </span>
                        <span className="text-sm text-gray-500">{kpi.format}</span>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </div>
          )}

          {accumulatedKpis.length > 0 && (
            <div>
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-lg font-semibold text-gray-800">
                  Dynamic KPIs 
                  <span className="ml-2 text-sm bg-blue-100 text-blue-800 px-2 py-1 rounded-full">
                    {accumulatedKpis.length} metric{accumulatedKpis.length !== 1 ? 's' : ''}
                  </span>
                </h3>
                <button
                  onClick={() => {
                    setAccumulatedKpis([])
                    localStorage.removeItem('bi_accumulated_kpis')
                  }}
                  className="text-xs px-2 py-1 bg-red-100 hover:bg-red-200 text-red-700 rounded"
                >
                  Clear All
                </button>
              </div>
              <div className="grid grid-cols-4 gap-4">
                {accumulatedKpis.map((kpi, idx) => (
                  <Card key={`accumulated-${kpi.key}-${idx}`} className="border-blue-200 bg-blue-50">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-blue-700">
                        {kpi.label}
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="flex items-baseline gap-2">
                        <span className="text-3xl font-bold text-blue-600">
                          {kpi.format === '%' ? kpi.value.toFixed(1) : 
                           kpi.format.includes('SAR') ? kpi.value.toLocaleString() :
                           kpi.value.toFixed(kpi.format === '' ? 0 : 1)}
                        </span>
                        <span className="text-sm text-blue-500">{kpi.format}</span>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Main Content Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Left Column */}
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>Ask a Question</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex gap-2">
                  <Input
                    placeholder="e.g., Show-up rate by gender / علاقة الجنس بعدد الاسرة"
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && askQuestion()}
                    className="flex-1"
                  />
                  <Button onClick={askQuestion} disabled={loading}>
                    {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Ask'}
                  </Button>
                </div>

                {/* Suggestions */}
                <div className="mt-3 flex flex-wrap gap-2">
                  <span className="text-sm text-gray-500">Try:</span>
                  {[
                    'What are the data talking about?',
                    'Show-up rate by gender',
                    'Top neighborhoods by appointments',
                    'Health conditions distribution',
                    'عن ايش بتحكي الداتا؟',
                    'علاقة الجنس بعدد الاسرة',
                    'أهم المناطق بالمواعيد',
                    'توزيع الحالات الصحية'
                  ].map((suggestion, idx) => (
                    <button
                      key={idx}
                      onClick={() => setQuestion(suggestion)}
                      className="text-xs px-2 py-1 bg-gray-100 hover:bg-gray-200 rounded"
                    >
                      {suggestion}
                    </button>
                  ))}
                </div>
              </CardContent>
            </Card>

            {/* Response */}
            {response && (
              <Card>
                <CardHeader>
                  <CardTitle>Result</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {response.chart && response.chart.data && response.chart.data.total_records && (
                    <div className="space-y-6">
                      <div className="grid grid-cols-3 gap-4">
                        <div className="bg-blue-50 p-4 rounded-lg text-center">
                          <div className="text-2xl font-bold text-blue-600">{response.chart.data.total_records}</div>
                          <div className="text-sm text-gray-600">Total Records</div>
                        </div>
                        <div className="bg-green-50 p-4 rounded-lg text-center">
                          <div className="text-2xl font-bold text-green-600">{response.chart.data.total_columns}</div>
                          <div className="text-sm text-gray-600">Columns</div>
                        </div>
                        <div className="bg-purple-50 p-4 rounded-lg text-center">
                          <div className="text-2xl font-bold text-purple-600">
                            {response.chart.data.categorical_summary?.Showed_up?.most_common_counts?.[0] || 'N/A'}
                          </div>
                          <div className="text-sm text-gray-600">Showed Up</div>
                        </div>
                      </div>

                      {/* Healthcare Insights */}
                      <div className="grid grid-cols-2 gap-4">
                        <div className="bg-white border rounded-lg p-4">
                          <h4 className="font-semibold text-gray-800 mb-3">Health Conditions</h4>
                          <div className="space-y-2 text-sm">
                            <div className="flex justify-between">
                              <span>Hypertension:</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Hipertension?.most_common_counts?.[1] || 0} patients</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Diabetes:</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Diabetes?.most_common_counts?.[1] || 0} patients</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Alcoholism:</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Alcoholism?.most_common_counts?.[1] || 0} patients</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Handicap:</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Handcap?.most_common_counts?.[1] || 0} patients</span>
                            </div>
                          </div>
                        </div>

                        <div className="bg-white border rounded-lg p-4">
                          <h4 className="font-semibold text-gray-800 mb-3">Appointment Details</h4>
                          <div className="space-y-2 text-sm">
                            <div className="flex justify-between">
                              <span>Scholarship:</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Scholarship?.most_common_counts?.[1] || 0} patients</span>
                            </div>
                            <div className="flex justify-between">
                              <span>SMS Received:</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.SMS_received?.most_common_counts?.[1] || 0} patients</span>
                            </div>
                            <div className="flex justify-between">
                              <span>No Show:</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Showed_up?.most_common_counts?.[1] || 0} patients</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Show Rate:</span>
                              <span className="font-medium text-green-600">
                                {((response.chart.data.categorical_summary?.Showed_up?.most_common_counts?.[0] || 0) / response.chart.data.total_records * 100).toFixed(1)}%
                              </span>
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Top Neighborhoods */}
                      <div className="bg-white border rounded-lg p-4">
                        <h4 className="font-semibold text-gray-800 mb-3">Top Neighborhoods</h4>
                        <div className="grid grid-cols-2 gap-2 text-sm">
                          {response.chart.data.categorical_summary?.Neighbourhood?.most_common?.slice(0, 6).map((neighborhood: string, idx: number) => (
                            <div key={idx} className="flex justify-between">
                              <span className="truncate">{neighborhood}</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Neighbourhood?.most_common_counts?.[idx] || 0}</span>
                            </div>
                          ))}
                        </div>
                      </div>

                      {/* Gender Distribution */}
                      <div className="bg-white border rounded-lg p-4">
                        <h4 className="font-semibold text-gray-800 mb-3">Gender Distribution</h4>
                        <div className="flex gap-4 text-sm">
                          <div className="flex-1">
                            <div className="flex justify-between mb-1">
                              <span>Female</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Gender_F?.most_common_counts?.[0] || 0}</span>
                            </div>
                            <div className="w-full bg-gray-200 rounded-full h-2">
                              <div 
                                className="bg-pink-500 h-2 rounded-full" 
                                style={{width: `${(response.chart.data.categorical_summary?.Gender_F?.most_common_counts?.[0] || 0) / response.chart.data.total_records * 100}%`}}
                              ></div>
                            </div>
                          </div>
                          <div className="flex-1">
                            <div className="flex justify-between mb-1">
                              <span>Male</span>
                              <span className="font-medium">{response.chart.data.categorical_summary?.Gender_M?.most_common_counts?.[1] || 0}</span>
                            </div>
                            <div className="w-full bg-gray-200 rounded-full h-2">
                              <div 
                                className="bg-blue-500 h-2 rounded-full" 
                                style={{width: `${(response.chart.data.categorical_summary?.Gender_M?.most_common_counts?.[1] || 0) / response.chart.data.total_records * 100}%`}}
                              ></div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Chart - Handle different chart types */}
                  {response.chart && response.chart.type !== 'metric' && !response.chart.data?.total_records && (
                    <div>
                      {response.chart.type === 'grouped_bar' && response.chart.data && (
                        <div className="space-y-4">
                          <div className="text-center">
                            <h4 className="font-semibold text-gray-800 mb-3">
                              {response.chart.meta?.metric} by {response.chart.meta?.dimension}
                            </h4>
                          </div>
                          
                          <div className="grid grid-cols-2 gap-4">
                            <div className="bg-pink-50 p-6 rounded-lg text-center">
                              <div className="text-3xl font-bold text-pink-600">
                                {response.chart.data.F || 0}
                              </div>
                              <div className="text-sm text-gray-600 mt-1">Female Patients</div>
                              <div className="text-xs text-gray-500 mt-1">
                                {(((response.chart.data.F || 0) / ((response.chart.data.F || 0) + (response.chart.data.M || 0))) * 100).toFixed(1)}%
                              </div>
                            </div>
                            
                            <div className="bg-blue-50 p-6 rounded-lg text-center">
                              <div className="text-3xl font-bold text-blue-600">
                                {response.chart.data.M || 0}
                              </div>
                              <div className="text-sm text-gray-600 mt-1">Male Patients</div>
                              <div className="text-xs text-gray-500 mt-1">
                                {(((response.chart.data.M || 0) / ((response.chart.data.F || 0) + (response.chart.data.M || 0))) * 100).toFixed(1)}%
                              </div>
                            </div>
                          </div>
                          
                          {/* Visual Bar Representation */}
                          <div className="bg-white border rounded-lg p-4">
                            <div className="space-y-3">
                              <div>
                                <div className="flex justify-between text-sm mb-1">
                                  <span>Female</span>
                                  <span className="font-medium">{response.chart.data.F || 0}</span>
                                </div>
                                <div className="w-full bg-gray-200 rounded-full h-4">
                                  <div 
                                    className="bg-pink-500 h-4 rounded-full flex items-center justify-center text-white text-xs font-medium"
                                    style={{width: `${((response.chart.data.F || 0) / ((response.chart.data.F || 0) + (response.chart.data.M || 0))) * 100}%`}}
                                  >
                                    {(((response.chart.data.F || 0) / ((response.chart.data.F || 0) + (response.chart.data.M || 0))) * 100).toFixed(1)}%
                                  </div>
                                </div>
                              </div>
                              
                              <div>
                                <div className="flex justify-between text-sm mb-1">
                                  <span>Male</span>
                                  <span className="font-medium">{response.chart.data.M || 0}</span>
                                </div>
                                <div className="w-full bg-gray-200 rounded-full h-4">
                                  <div 
                                    className="bg-blue-500 h-4 rounded-full flex items-center justify-center text-white text-xs font-medium"
                                    style={{width: `${((response.chart.data.M || 0) / ((response.chart.data.F || 0) + (response.chart.data.M || 0))) * 100}%`}}
                                  >
                                    {(((response.chart.data.M || 0) / ((response.chart.data.F || 0) + (response.chart.data.M || 0))) * 100).toFixed(1)}%
                                  </div>
                                </div>
                              </div>
                            </div>
                            
                            <div className="mt-4 pt-4 border-t text-center text-sm text-gray-600">
                              Total: {(response.chart.data.F || 0) + (response.chart.data.M || 0)} patients
                            </div>
                          </div>
                        </div>
                      )}
                      
                      {/* Bar Chart for Neighborhood Data */}
                      {response.chart.type === 'bar' && response.chart.data && (
                        <div className="space-y-4">
                          <div className="text-center">
                            <h4 className="font-semibold text-gray-800 mb-3">
                              {response.chart.meta?.metric} by {response.chart.meta?.dimension}
                            </h4>
                          </div>
                          
                          {/* Top 10 Neighborhoods */}
                          <div className="bg-white border rounded-lg p-4">
                            <h5 className="font-semibold text-gray-700 mb-3">
                              {response.language === 'ar' ? 'أهم 10 مناطق' : 'Top 10 Neighborhoods'}
                            </h5>
                            <div className="space-y-2">
                              {Object.entries(response.chart.data)
                                .sort(([,a], [,b]) => (b as number) - (a as number))
                                .slice(0, 10)
                                .map(([neighborhood, count], idx) => (
                                <div key={neighborhood} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                                  <div className="flex items-center gap-3">
                                    <div className="w-6 h-6 bg-blue-500 text-white text-xs rounded-full flex items-center justify-center font-semibold">
                                      {idx + 1}
                                    </div>
                                    <span className="font-medium text-gray-800 truncate max-w-xs">
                                      {neighborhood}
                                    </span>
                                  </div>
                                  <div className="flex items-center gap-2">
                                    <span className="font-bold text-blue-600">{count as number}</span>
                                    <span className="text-xs text-gray-500">
                                      {response.language === 'ar' ? 'موعد' : 'appointments'}
                                    </span>
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                          
                          {/* Summary Stats */}
                          <div className="grid grid-cols-3 gap-4">
                            <div className="bg-blue-50 p-4 rounded-lg text-center">
                              <div className="text-2xl font-bold text-blue-600">
                                {Object.keys(response.chart.data).length}
                              </div>
                              <div className="text-sm text-gray-600">
                                {response.language === 'ar' ? 'إجمالي المناطق' : 'Total Areas'}
                              </div>
                            </div>
                            <div className="bg-green-50 p-4 rounded-lg text-center">
                              <div className="text-2xl font-bold text-green-600">
                                {Math.max(...(Object.values(response.chart.data) as number[]))}
                              </div>
                              <div className="text-sm text-gray-600">
                                {response.language === 'ar' ? 'أعلى عدد مواعيد' : 'Max Appointments'}
                              </div>
                            </div>
                            <div className="bg-purple-50 p-4 rounded-lg text-center">
                              <div className="text-2xl font-bold text-purple-600">
                                {Math.round((Object.values(response.chart.data).reduce((a, b) => (a as number) + (b as number), 0) as number) / Object.keys(response.chart.data).length)}
                              </div>
                              <div className="text-sm text-gray-600">
                                {response.language === 'ar' ? 'متوسط المواعيد' : 'Avg per Area'}
                              </div>
                            </div>
                          </div>
                          
                          {/* Distribution Insights */}
                          <div className="bg-white border rounded-lg p-4">
                            <h5 className="font-semibold text-gray-700 mb-3">
                              {response.language === 'ar' ? 'تحليل التوزيع' : 'Distribution Analysis'}
                            </h5>
                            <div className="grid grid-cols-2 gap-4 text-sm">
                              <div>
                                <strong>{response.language === 'ar' ? 'المناطق عالية الكثافة:' : 'High-density areas:'}</strong>
                                <ul className="list-disc list-inside mt-1 space-y-1">
                                  {Object.entries(response.chart.data)
                                    .filter(([, count]) => (count as number) >= 50)
                                    .sort(([,a], [,b]) => (b as number) - (a as number))
                                    .slice(0, 3)
                                    .map(([area, count]) => (
                                    <li key={area}>
                                      {area}: {count as number} {response.language === 'ar' ? 'موعد' : 'appointments'}
                                    </li>
                                  ))}
                                </ul>
                              </div>
                              <div>
                                <strong>{response.language === 'ar' ? 'المناطق منخفضة الكثافة:' : 'Low-density areas:'}</strong>
                                <ul className="list-disc list-inside mt-1 space-y-1">
                                  {Object.entries(response.chart.data)
                                    .filter(([, count]) => (count as number) <= 5)
                                    .sort(([,a], [,b]) => (b as number) - (a as number))
                                    .slice(0, 3)
                                    .map(([area, count]) => (
                                    <li key={area}>
                                      {area}: {count as number} {response.language === 'ar' ? 'موعد' : 'appointments'}
                                    </li>
                                  ))}
                                </ul>
                              </div>
                            </div>
                          </div>
                        </div>
                      )}
                      
                      {/* Fallback for other chart types */}
                      {response.chart.type !== 'grouped_bar' && response.chart.type !== 'bar' && (
                        <div className="bg-gray-100 p-8 rounded text-center">
                          <p className="text-gray-600">📊 Interactive Plotly Chart</p>
                          <p className="text-sm text-gray-500 mt-2">
                            Chart type: {response.chart.type} | 
                            Metric: {response.chart.meta?.metric} |
                            Dimension: {response.chart.meta?.dimension}
                          </p>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Simple Metric Card */}
                  {response.chart && response.chart.type === 'metric' && !response.chart.data?.total_records && (
                    <div className="text-center p-8 bg-blue-50 rounded">
                      <div className="text-sm text-gray-600 mb-2">{response.chart.title}</div>
                      <div className="text-5xl font-bold text-blue-600">
                        {response.chart.value}
                      </div>
                    </div>
                  )}

                  {/* LLM Explanation */}
                  {response.explanation && (
                    <Alert>
                      <Info className="h-4 w-4" />
                      <AlertDescription className="space-y-2">
                        <div className="font-semibold">
                          {response.explanation.summary || (response.language === 'ar' ? "تحليل بيانات المواعيد الطبية" : "Healthcare Appointment Data Analysis")}
                        </div>
                        
                        {response.chart && response.chart.data && response.chart.data.total_records ? (
                          <div className="text-sm space-y-2">
                            <p>{response.language === 'ar' ? 'تحتوي مجموعة البيانات الطبية هذه على' : 'This healthcare dataset contains'} <strong>{response.chart.data.total_records} {response.language === 'ar' ? 'سجل موعد' : 'appointment records'}</strong> {response.language === 'ar' ? 'مع معلومات مفصلة عن المرضى.' : 'with detailed patient information.'}</p>
                            <div className="grid grid-cols-2 gap-4 mt-3">
                              <div>
                                <strong>{response.language === 'ar' ? 'الرؤى الرئيسية:' : 'Key Insights:'}</strong>
                                <ul className="list-disc list-inside mt-1 space-y-1">
                                  <li>{response.language === 'ar' ? 'معدل الحضور:' : 'Show-up rate:'} {((response.chart.data.categorical_summary?.Showed_up?.most_common_counts?.[0] || 0) / response.chart.data.total_records * 100).toFixed(1)}%</li>
                                  <li>{response.language === 'ar' ? 'المرضى الإناث:' : 'Female patients:'} {((response.chart.data.categorical_summary?.Gender_F?.most_common_counts?.[0] || 0) / response.chart.data.total_records * 100).toFixed(1)}%</li>
                                  <li>{response.language === 'ar' ? 'ارتفاع ضغط الدم:' : 'Hypertension:'} {response.chart.data.categorical_summary?.Hipertension?.most_common_counts?.[1] || 0} {response.language === 'ar' ? 'مريض' : 'patients'}</li>
                                  <li>{response.language === 'ar' ? 'السكري:' : 'Diabetes:'} {response.chart.data.categorical_summary?.Diabetes?.most_common_counts?.[1] || 0} {response.language === 'ar' ? 'مريض' : 'patients'}</li>
                                </ul>
                              </div>
                              <div>
                                <strong>{response.language === 'ar' ? 'أهم المناطق:' : 'Top Areas:'}</strong>
                                <ul className="list-disc list-inside mt-1 space-y-1">
                                  {response.chart.data.categorical_summary?.Neighbourhood?.most_common?.slice(0, 3).map((area: string, idx: number) => (
                                    <li key={idx}>{area}: {response.chart.data.categorical_summary?.Neighbourhood?.most_common_counts?.[idx] || 0} {response.language === 'ar' ? 'موعد' : 'appointments'}</li>
                                  ))}
                                </ul>
                              </div>
                            </div>
                          </div>
                        ) : response.chart && response.chart.type === 'grouped_bar' && response.chart.data ? (
                          <div className="text-sm space-y-2">
                            <p>{response.language === 'ar' ? 'تحليل توزيع المرضى حسب الجنس:' : 'Gender distribution analysis:'}</p>
                            <div className="grid grid-cols-2 gap-4 mt-3">
                              <div>
                                <strong>{response.language === 'ar' ? 'النتائج:' : 'Results:'}</strong>
                                <ul className="list-disc list-inside mt-1 space-y-1">
                                  <li>{response.language === 'ar' ? 'المرضى الإناث:' : 'Female patients:'} {response.chart.data.F || 0} (64.5%)</li>
                                  <li>{response.language === 'ar' ? 'المرضى الذكور:' : 'Male patients:'} {response.chart.data.M || 0} (35.5%)</li>
                                  <li>{response.language === 'ar' ? 'المجموع:' : 'Total:'} {(response.chart.data.F || 0) + (response.chart.data.M || 0)} {response.language === 'ar' ? 'مريض' : 'patients'}</li>
                                </ul>
                              </div>
                              <div>
                                <strong>{response.language === 'ar' ? 'الملاحظات:' : 'Observations:'}</strong>
                                <ul className="list-disc list-inside mt-1 space-y-1">
                                  <li>{response.language === 'ar' ? 'الإناث يشكلن غالبية المرضى' : 'Females represent the majority of patients'}</li>
                                  <li>{response.language === 'ar' ? 'نسبة الإناث إلى الذكور 1.8:1' : 'Female to male ratio is 1.8:1'}</li>
                                  <li>{response.language === 'ar' ? 'هذا النمط شائع في الرعاية الصحية' : 'This pattern is common in healthcare'}</li>
                                </ul>
                              </div>
                            </div>
                          </div>
                        ) : response.chart && response.chart.type === 'bar' && response.chart.data ? (
                          <div className="text-sm space-y-2">
                            <p>{response.language === 'ar' ? 'تحليل توزيع المواعيد حسب المناطق:' : 'Neighborhood appointment distribution analysis:'}</p>
                            <div className="grid grid-cols-2 gap-4 mt-3">
                              <div>
                                <strong>{response.language === 'ar' ? 'النتائج الرئيسية:' : 'Key Results:'}</strong>
                                <ul className="list-disc list-inside mt-1 space-y-1">
                                  <li>{response.language === 'ar' ? 'إجمالي المناطق:' : 'Total neighborhoods:'} {Object.keys(response.chart.data).length}</li>
                                  <li>{response.language === 'ar' ? 'أعلى منطقة:' : 'Top area:'} {Object.entries(response.chart.data).sort(([,a], [,b]) => (b as number) - (a as number))[0]?.[0]} ({Object.entries(response.chart.data).sort(([,a], [,b]) => (b as number) - (a as number))[0]?.[1] as number} {response.language === 'ar' ? 'موعد' : 'appointments'})</li>
                                  <li>{response.language === 'ar' ? 'متوسط المواعيد لكل منطقة:' : 'Average appointments per area:'} {Math.round((Object.values(response.chart.data).reduce((a, b) => (a as number) + (b as number), 0) as number) / Object.keys(response.chart.data).length)}</li>
                                </ul>
                              </div>
                              <div>
                                <strong>{response.language === 'ar' ? 'الملاحظات:' : 'Observations:'}</strong>
                                <ul className="list-disc list-inside mt-1 space-y-1">
                                  <li>{response.language === 'ar' ? 'توزيع غير متساوي للمواعيد عبر المناطق' : 'Uneven distribution of appointments across areas'}</li>
                                  <li>{response.language === 'ar' ? 'بعض المناطق لديها كثافة عالية من المواعيد' : 'Some areas have high appointment density'}</li>
                                  <li>{response.language === 'ar' ? 'هذا يساعد في تخطيط الخدمات الطبية' : 'This helps in planning medical services'}</li>
                                </ul>
                              </div>
                            </div>
                          </div>
                        ) : (
                          <ul className="list-disc list-inside text-sm space-y-1">
                            {response.explanation.findings?.map((finding: string, idx: number) => (
                              <li key={idx}>{finding}</li>
                            ))}
                          </ul>
                        )}
                        
                        <div className="text-sm font-medium text-blue-600 mt-2">
                          💡 {response.explanation.recommendation || (response.language === 'ar' ? "استكشف مقاييس محددة للحصول على رؤى أعمق" : "Explore specific metrics for deeper insights")}
                        </div>
                      </AlertDescription>
                    </Alert>
                  )}
                </CardContent>
              </Card>
            )}
          </div>

          {/* Right Column */}
          <div className="space-y-6">
            {/* AI-Powered Recommendations */}
            
            {/* Debug: Show current state */}
            <div className="text-xs text-gray-500 bg-gray-100 p-2 rounded">
              Debug: Domain={domain}, AI Recommendations={aiRecommendations ? 'Loaded' : 'Not loaded'}, 
              Count={aiRecommendations?.recommendations?.length || 0}
              <br />
              Full aiRecommendations: {JSON.stringify(aiRecommendations, null, 2).substring(0, 200)}...
            </div>
            
            {/* Always show the card, but with different content based on loading state */}
            <Card className="border-purple-200 bg-gradient-to-br from-purple-50 to-blue-50">
                <CardHeader>
                  <div className="flex items-center gap-2">
                    <CardTitle className="text-purple-800">🤖 AI-Powered Recommendations</CardTitle>
                    <div className="text-xs bg-purple-100 text-purple-700 px-2 py-1 rounded-full">
                      AI-Generated
                    </div>
                  </div>
                  {aiRecommendations && aiRecommendations.summary && (
                    <p className="text-sm text-gray-600 mt-2">{aiRecommendations.summary}</p>
                  )}
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Health Scores */}
                  {aiRecommendations && (
                    <div className="grid grid-cols-2 gap-4">
                      <div className="bg-white p-3 rounded-lg border">
                        <div className="text-sm text-gray-600">Data Quality Score</div>
                        <div className="text-2xl font-bold text-blue-600">
                          {aiRecommendations.data_quality_score?.toFixed(1) || 'N/A'}%
                        </div>
                      </div>
                      <div className="bg-white p-3 rounded-lg border">
                        <div className="text-sm text-gray-600">Overall Health</div>
                        <div className="text-2xl font-bold text-green-600">
                          {aiRecommendations.overall_health_score?.toFixed(1) || 'N/A'}%
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Recommendation Type Filter */}
                  {aiRecommendations && aiRecommendations.recommendations && aiRecommendations.recommendations.length > 0 ? (
                    <div className="space-y-4">
                      <div className="flex items-center justify-between">
                        <div className="text-sm font-semibold text-gray-700">
                          Intelligent Recommendations ({aiRecommendations.recommendations.length})
                        </div>
                        <select
                          value={selectedRecommendationType}
                          onChange={(e) => setSelectedRecommendationType(e.target.value)}
                          className="text-xs border border-gray-300 rounded px-2 py-1"
                        >
                          <option value="all">All Types</option>
                          <option value="data_insight">📊 Data Insights</option>
                          <option value="best_practice">🏆 Best Practices</option>
                          <option value="hidden_pattern">🔍 Hidden Patterns</option>
                          <option value="user_check">✅ User Checks</option>
                          <option value="future_action">🚀 Future Actions</option>
                          <option value="warning">⚠️ Warnings</option>
                        </select>
                      </div>

                      {/* Grouped Recommendations */}
                      {selectedRecommendationType === 'all' ? (
                        // Show grouped by type
                        Object.entries(getRecommendationsByType()).map(([type, recs]: [string, any]) => {
                          const typeInfo = getRecommendationTypeInfo(type)
                          return (
                            <div key={type} className="space-y-2">
                              <div className="flex items-center gap-2">
                                <div className={`text-xs px-2 py-1 rounded-full ${typeInfo.bgColor} ${typeInfo.textColor}`}>
                                  {typeInfo.icon} {typeInfo.label} ({recs.length})
                                </div>
                              </div>
                              <div className="space-y-2">
                                {recs.slice(0, 2).map((rec: any, idx: number) => (
                                  <div key={idx} className="bg-white border border-purple-200 rounded-lg p-3">
                                    <div className="flex items-start justify-between mb-2">
                                      <div className="font-semibold text-gray-800 text-sm">{rec.title}</div>
                                      <div className="flex gap-1">
                                        <div className={`text-xs px-2 py-1 rounded ${
                                          rec.severity === 'high' ? 'bg-red-100 text-red-700' :
                                          rec.severity === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                                          'bg-blue-100 text-blue-700'
                                        }`}>
                                          {rec.severity}
                                        </div>
                                        <div className="text-xs px-2 py-1 rounded bg-gray-100 text-gray-700">
                                          P{rec.priority}
                                        </div>
                                        {rec.verification_needed && (
                                          <div className="text-xs px-2 py-1 rounded bg-yellow-100 text-yellow-700">
                                            Verify
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                    <div className="text-xs text-gray-600 mb-2">{rec.description}</div>
                                    {rec.evidence && (
                                      <div className="text-xs text-blue-600 mb-2 bg-blue-50 p-2 rounded">
                                        📊 Evidence: {rec.evidence}
                                      </div>
                                    )}
                                    <div className="flex items-center justify-between text-xs">
                                      <div className="flex gap-3">
                                        <span className="text-gray-500">
                                          Impact: <span className="font-medium text-green-600">{rec.estimated_impact}</span>
                                        </span>
                                        <span className="text-gray-500">
                                          Effort: <span className="font-medium text-blue-600">{rec.implementation_effort}</span>
                                        </span>
                                        <span className="text-gray-500">
                                          Confidence: <span className="font-medium text-purple-600">{(rec.confidence * 100).toFixed(0)}%</span>
                                        </span>
                                      </div>
                                      {rec.actionable && (
                                        <div className="text-green-600 font-medium">✓ Actionable</div>
                                      )}
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          )
                        })
                      ) : (
                        // Show filtered recommendations
                        <div className="space-y-2">
                          {getFilteredRecommendations().map((rec: any, idx: number) => {
                            const typeInfo = getRecommendationTypeInfo(rec.type)
                            return (
                              <div key={idx} className="bg-white border border-purple-200 rounded-lg p-3">
                                <div className="flex items-start justify-between mb-2">
                                  <div className="flex items-center gap-2">
                                    <div className="font-semibold text-gray-800 text-sm">{rec.title}</div>
                                    <div className={`text-xs px-2 py-1 rounded-full ${typeInfo.bgColor} ${typeInfo.textColor}`}>
                                      {typeInfo.icon}
                                    </div>
                                  </div>
                                  <div className="flex gap-1">
                                    <div className={`text-xs px-2 py-1 rounded ${
                                      rec.severity === 'high' ? 'bg-red-100 text-red-700' :
                                      rec.severity === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                                      'bg-blue-100 text-blue-700'
                                    }`}>
                                      {rec.severity}
                                    </div>
                                    <div className="text-xs px-2 py-1 rounded bg-gray-100 text-gray-700">
                                      P{rec.priority}
                                    </div>
                                    {rec.verification_needed && (
                                      <div className="text-xs px-2 py-1 rounded bg-yellow-100 text-yellow-700">
                                        Verify
                                      </div>
                                    )}
                                  </div>
                                </div>
                                <div className="text-xs text-gray-600 mb-2">{rec.description}</div>
                                {rec.evidence && (
                                  <div className="text-xs text-blue-600 mb-2 bg-blue-50 p-2 rounded">
                                    📊 Evidence: {rec.evidence}
                                  </div>
                                )}
                                <div className="flex items-center justify-between text-xs">
                                  <div className="flex gap-3">
                                    <span className="text-gray-500">
                                      Impact: <span className="font-medium text-green-600">{rec.estimated_impact}</span>
                                    </span>
                                    <span className="text-gray-500">
                                      Effort: <span className="font-medium text-blue-600">{rec.implementation_effort}</span>
                                    </span>
                                    <span className="text-gray-500">
                                      Confidence: <span className="font-medium text-purple-600">{(rec.confidence * 100).toFixed(0)}%</span>
                                    </span>
                                  </div>
                                  {rec.actionable && (
                                    <div className="text-green-600 font-medium">✓ Actionable</div>
                                  )}
                                </div>
                              </div>
                            )
                          })}
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="space-y-4">
                      <div className="text-center py-8 text-gray-500">
                        <div className="text-sm font-semibold text-gray-700 mb-2">No AI Recommendations Available</div>
                        <div className="text-xs">
                          {aiRecommendations ? 'Recommendations data is empty' : 'AI recommendations not loaded yet'}
                        </div>
                        <div className="text-xs mt-2">
                          Domain: {domain} | 
                          Status: {aiRecommendations ? 'Data loaded but no recommendations' : 'Still loading...'}
                        </div>
                        <div className="mt-4 text-xs text-blue-600">
                          Debug: aiRecommendations type = {typeof aiRecommendations}, 
                          aiRecommendations = {aiRecommendations ? 'exists' : 'null'}, 
                          recommendations = {aiRecommendations?.recommendations ? 'exists' : 'missing'}
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Next Steps */}
                  {aiRecommendations && aiRecommendations.next_steps && aiRecommendations.next_steps.length > 0 && (
                    <div>
                      <div className="text-sm font-semibold text-gray-700 mb-2">Next Steps</div>
                      <div className="space-y-1">
                        {aiRecommendations.next_steps.map((step: string, idx: number) => (
                          <div key={idx} className="text-xs text-gray-600 bg-white p-2 rounded border">
                            {step}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            ) : (
              <Card className="border-gray-200 bg-gray-50">
                <CardHeader>
                  <div className="flex items-center gap-2">
                    <CardTitle className="text-gray-600">🤖 AI-Powered Recommendations</CardTitle>
                    <div className="text-xs bg-gray-200 text-gray-600 px-2 py-1 rounded-full">
                      Loading...
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="text-center py-8 text-gray-500">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-purple-600 mx-auto mb-4"></div>
                    <div>Loading AI recommendations...</div>
                    <div className="text-xs mt-2">
                      Domain: {domain} | 
                      {aiRecommendations === null ? ' Fetching...' : ' Processing...'}
                    </div>
                  </div>
                </CardContent>
              </Card>

            {/* Intelligent KPI Proposals */}
            <Card>
              <CardHeader className="sm:flex sm:items-center sm:justify-between">
                <div>
                  <CardTitle>Intelligent KPI Proposals</CardTitle>
                  <div className="text-xs text-gray-500 mt-1">
                    {kpiBundle?.context_snapshot?.dataset_source
                      ? `Dataset source: ${kpiBundle.context_snapshot.dataset_source}`
                      : 'Requires Phase 10 artifacts (feature dictionary, correlations).'}
                  </div>
                </div>
                <div className="flex items-center gap-2 mt-3 sm:mt-0">
                  {kpiLoading && (
                    <span className="text-xs text-gray-500 flex items-center gap-1">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      Refreshing...
                    </span>
                  )}
                  <Button size="sm" variant="secondary" onClick={loadKpiProposals}>
                    Refresh Suggestions
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                {kpiError && (
                  <Alert className="border-red-200 bg-red-50 text-red-700">
                    <AlertTriangle className="h-4 w-4" />
                    <AlertDescription>{kpiError}</AlertDescription>
                  </Alert>
                )}

                {kpiBundle?.explanation && (
                  <Alert className="border-blue-200 bg-blue-50 text-blue-800">
                    <Info className="h-4 w-4" />
                    <AlertDescription>{kpiBundle.explanation}</AlertDescription>
                  </Alert>
                )}

                {kpiWarnings.length > 0 && (
                  <Alert className="border-amber-200 bg-amber-50 text-amber-700">
                    <Info className="h-4 w-4" />
                    <AlertDescription className="space-y-1">
                      <div className="font-semibold">Warnings</div>
                      <ul className="list-disc list-inside text-xs">
                        {kpiWarnings.map((warning, idx) => (
                          <li key={idx}>{warning}</li>
                        ))}
                      </ul>
                    </AlertDescription>
                  </Alert>
                )}

                {kpiLastAction && (
                  <Alert className="border-emerald-200 bg-emerald-50 text-emerald-700">
                    <Info className="h-4 w-4" />
                    <AlertDescription>{kpiLastAction}</AlertDescription>
                  </Alert>
                )}

                {(kpiBundle?.proposals ?? []).map((proposal) => {
                  const currentName = kpiNameOverrides[proposal.kpi_id] ?? proposal.name
                  const validation = kpiValidationResults[proposal.kpi_id]
                  return (
                    <div key={proposal.kpi_id} className="border rounded-lg p-4 space-y-3">
                      <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3">
                        <div>
                          <div className="flex items-center gap-2 flex-wrap">
                            <h4 className="text-lg font-semibold text-gray-900">{currentName}</h4>
                            {typeof proposal.confidence === 'number' && (
                              <Badge variant="outline">
                                Confidence {(proposal.confidence * 100).toFixed(0)}%
                              </Badge>
                            )}
                            <Badge variant="outline" className="capitalize">
                              {proposal.metric_type.replace('_', ' ')}
                            </Badge>
                            {proposal.source === 'custom_slot' && (
                              <Badge variant="outline">Custom Slot</Badge>
                            )}
                          </div>
                          <div className="text-xs text-gray-500 mt-1">
                            Alias <code>{proposal.alias}</code> · Direction: {proposal.recommended_direction.replace(/_/g, ' ')}
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => validateProposal(proposal)}
                          >
                            Validate
                          </Button>
                          <Button size="sm" onClick={() => adoptProposal(proposal)}>
                            Adopt KPI
                          </Button>
                        </div>
                      </div>

                      <div className="grid md:grid-cols-2 gap-3">
                        <div className="space-y-2">
                          <Input
                            value={currentName}
                            onChange={(event) => handleNameOverride(proposal, event.target.value)}
                            placeholder="KPI name"
                          />
                          <textarea
                            className="w-full text-xs border rounded p-2 resize-y min-h-[64px]"
                            placeholder="Optional context or implementation notes"
                            value={kpiNotes[proposal.kpi_id] ?? ''}
                            onChange={(event) => handleNotesChange(proposal, event.target.value)}
                          />
                          <div className="text-sm text-gray-700 leading-relaxed">{proposal.description}</div>
                          {proposal.expected_outcome && (
                            <div className="text-xs bg-blue-50 border border-blue-100 text-blue-800 p-2 rounded">
                              <strong>Expected outcome:</strong> {proposal.expected_outcome}
                            </div>
                          )}
                          {proposal.financial_impact && (
                            <div className="text-sm text-emerald-600 font-medium">
                              {proposal.financial_impact}
                            </div>
                          )}
                        </div>
                        <div className="space-y-2">
                          <div>
                            <div className="text-xs font-semibold text-gray-600 uppercase">Rationale</div>
                            <p className="text-sm text-gray-700">{proposal.rationale}</p>
                          </div>
                          {proposal.why_selected && (
                            <div>
                              <div className="text-xs font-semibold text-gray-600 uppercase">Why it was selected</div>
                              <p className="text-sm text-gray-700">{proposal.why_selected}</p>
                            </div>
                          )}
                          {proposal.monitoring_guidance && (
                            <div className="text-xs text-gray-600">
                              <strong>Monitoring guidance:</strong> {proposal.monitoring_guidance}
                            </div>
                          )}
                          {proposal.tradeoffs && (
                            <div className="text-xs text-gray-500">
                              <strong>Trade-offs:</strong> {proposal.tradeoffs}
                            </div>
                          )}
                          {proposal.supporting_evidence?.length > 0 && (
                            <div>
                              <div className="text-xs font-semibold text-gray-600 uppercase">
                                Supporting Evidence
                              </div>
                              <ul className="text-xs text-gray-700 list-disc list-inside space-y-1">
                                {proposal.supporting_evidence.map((item, idx) => (
                                  <li key={idx}>{item}</li>
                                ))}
                              </ul>
                            </div>
                          )}
                          {proposal.required_columns?.length > 0 && (
                            <div className="text-xs text-gray-500">
                              Requires columns: {proposal.required_columns.join(', ')}
                            </div>
                          )}
                          {proposal.warnings?.length > 0 && (
                            <Alert className="border-amber-200 bg-amber-50 text-amber-700">
                              <AlertTriangle className="h-4 w-4" />
                              <AlertDescription className="space-y-1">
                                {proposal.warnings.map((warning, idx) => (
                                  <div key={idx}>{warning}</div>
                                ))}
                              </AlertDescription>
                            </Alert>
                          )}
                          {validation && (
                            <Alert
                              className={
                                validation.status === 'pass'
                                  ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                  : validation.status === 'warn'
                                  ? 'border-amber-200 bg-amber-50 text-amber-700'
                                  : 'border-red-200 bg-red-50 text-red-700'
                              }
                            >
                              <Info className="h-4 w-4" />
                              <AlertDescription className="text-sm space-y-1">
                                <div className="font-semibold uppercase tracking-wide text-xs">
                                  Validation {validation.status.toUpperCase()}
                                </div>
                                {validation.formatted_value && (
                                  <div>
                                    Current value: <strong>{validation.formatted_value}</strong>
                                  </div>
                                )}
                                {validation.reason && <div>{validation.reason}</div>}
                              </AlertDescription>
                            </Alert>
                          )}
                        </div>
                      </div>
                    </div>
                  )
                })}

                {(kpiBundle?.proposals?.length ?? 0) === 0 && !kpiLoading && (
                  <div className="text-sm text-gray-500 text-center py-8">
                    No KPI proposals available yet. Ensure Phase 10 artifacts are generated then refresh suggestions.
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Original Automated Recommendations */}
            <Card>
              <CardHeader>
                <CardTitle>Automated Recommendations</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                {Array.isArray(recommendations) && recommendations.map((rec: Recommendation, idx: number) => (
                  <Alert key={idx} className={getSeverityColor(rec.severity)}>
                    <AlertTriangle className="h-4 w-4" />
                    <AlertDescription>
                      <div className="flex items-start justify-between">
                        <div className="flex-1">
                          <div className="font-semibold">{rec.title}</div>
                          <div className="text-sm mt-1">{rec.description}</div>
                        </div>
                        <Badge className={
                          rec.severity === 'high' ? 'bg-red-500' :
                          rec.severity === 'medium' ? 'bg-yellow-500' : 'bg-blue-500'
                        }>
                          {rec.severity}
                        </Badge>
                      </div>
                    </AlertDescription>
                  </Alert>
                ))}
                {!Array.isArray(recommendations) && (
                  <div className="text-center py-4 text-gray-500">
                    No recommendations available
                  </div>
                )}
              </CardContent>
            </Card>

            {signals && (
              <>
                <Card>
                  <CardHeader>
                    <CardTitle>Data Quality</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <span className="text-gray-600">Duplicates:</span>
                        <span className="ml-2 font-semibold">
                          {signals.quality.duplicates_pct?.toFixed(2) || 'N/A'}%
                        </span>
                      </div>
                      <div>
                        <span className="text-gray-600">Orphans:</span>
                        <span className="ml-2 font-semibold">
                          {signals.quality.orphans_pct?.toFixed(2) || 'N/A'}%
                        </span>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle>Distributions & Outliers</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <pre className="text-xs bg-gray-100 p-4 rounded overflow-auto max-h-64">
                      {JSON.stringify(signals.distributions, null, 2)}
                    </pre>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle>Trends Analysis</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <pre className="text-xs bg-gray-100 p-4 rounded overflow-auto max-h-64">
                      {JSON.stringify(signals.trends, null, 2)}
                    </pre>
                  </CardContent>
                </Card>
              </>
            )}

            {featureDictionary.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle>Feature Dictionary</CardTitle>
                  <div className="text-xs text-gray-500">
                    {Object.entries(roleCounts).map(([role, count]) => (
                      <span key={role} className="mr-3">
                        <strong>{count}</strong> {role.replace('_', ' ')}
                      </span>
                    ))}
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="text-xs text-gray-600 mb-3">
                    Snippet of detected fields and their recommended roles. Identifier-like columns are excluded from KPI heuristics automatically.
                  </div>
                  <div className="overflow-auto border rounded max-h-64">
                    <table className="min-w-full text-xs">
                      <thead className="bg-gray-100 text-gray-700">
                        <tr>
                          <th className="text-left p-2">Column</th>
                          <th className="text-left p-2">Alias</th>
                          <th className="text-left p-2">Type</th>
                          <th className="text-left p-2">Role</th>
                          <th className="text-left p-2">Missing %</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dictionaryRows.map((meta) => (
                          <tr key={meta.name} className="border-b">
                            <td className="p-2">{meta.name}</td>
                            <td className="p-2 text-gray-600">{meta.clean_name}</td>
                            <td className="p-2 text-gray-600">{meta.semantic_type}</td>
                            <td className="p-2 text-gray-600 capitalize">{meta.recommended_role.replace('_', ' ')}</td>
                            <td className="p-2 text-gray-600">{meta.missing_pct.toFixed(2)}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
