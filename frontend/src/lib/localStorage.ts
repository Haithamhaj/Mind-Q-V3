/**
 * localStorage utilities for managing pipeline data
 */

export interface PipelineData {
  selectedFile: {
    name: string
    size: number
    type: string
    lastModified: number
  }
  domain: string
  targetColumn: string
  phaseResults: Record<string, any>
  progress: number
  timestamp: number
}

const STORAGE_KEYS = {
  PIPELINE_DATA: 'mindq_pipeline_data',
  CURRENT_SESSION: 'mindq_current_session'
} as const

/**
 * Save pipeline data to localStorage
 */
export const savePipelineData = (data: Partial<PipelineData>): void => {
  try {
    const existingData = getPipelineData()
    const updatedData = {
      ...existingData,
      ...data,
      timestamp: Date.now()
    }
    
    localStorage.setItem(STORAGE_KEYS.PIPELINE_DATA, JSON.stringify(updatedData))
    console.log('✅ Pipeline data saved to localStorage')
  } catch (error) {
    console.error('❌ Failed to save pipeline data:', error)
  }
}

/**
 * Get pipeline data from localStorage
 */
export const getPipelineData = (): PipelineData | null => {
  try {
    const data = localStorage.getItem(STORAGE_KEYS.PIPELINE_DATA)
    if (!data) return null
    
    const parsed = JSON.parse(data)
    
    // Check if data is older than 24 hours
    const isExpired = Date.now() - parsed.timestamp > 24 * 60 * 60 * 1000
    if (isExpired) {
      clearPipelineData()
      return null
    }
    
    return parsed
  } catch (error) {
    console.error('❌ Failed to get pipeline data:', error)
    return null
  }
}

/**
 * Clear pipeline data from localStorage
 */
export const clearPipelineData = (): void => {
  try {
    localStorage.removeItem(STORAGE_KEYS.PIPELINE_DATA)
    localStorage.removeItem(STORAGE_KEYS.CURRENT_SESSION)
    console.log('✅ Pipeline data cleared from localStorage')
  } catch (error) {
    console.error('❌ Failed to clear pipeline data:', error)
  }
}

/**
 * Check if pipeline data exists and is valid
 */
export const hasValidPipelineData = (): boolean => {
  const data = getPipelineData()
  return data !== null && data.phaseResults && Object.keys(data.phaseResults).length > 0
}

/**
 * Get file info from localStorage (without the actual file)
 */
export const getStoredFileInfo = () => {
  const data = getPipelineData()
  return data?.selectedFile || null
}

/**
 * Save current session info
 */
export const saveCurrentSession = (sessionData: any): void => {
  try {
    localStorage.setItem(STORAGE_KEYS.CURRENT_SESSION, JSON.stringify({
      ...sessionData,
      timestamp: Date.now()
    }))
  } catch (error) {
    console.error('❌ Failed to save session data:', error)
  }
}

/**
 * Get current session info
 */
export const getCurrentSession = (): any => {
  try {
    const data = localStorage.getItem(STORAGE_KEYS.CURRENT_SESSION)
    return data ? JSON.parse(data) : null
  } catch (error) {
    console.error('❌ Failed to get session data:', error)
    return null
  }
}
