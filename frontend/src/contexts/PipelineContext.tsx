import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { 
  getPipelineData, 
  savePipelineData, 
  clearPipelineData, 
  hasValidPipelineData,
  type PipelineData 
} from '@/lib/localStorage'

interface PipelineContextType {
  pipelineData: PipelineData | null
  hasData: boolean
  saveData: (data: Partial<PipelineData>) => void
  clearData: () => void
  refreshData: () => void
}

const PipelineContext = createContext<PipelineContextType | undefined>(undefined)

export const usePipelineData = () => {
  const context = useContext(PipelineContext)
  if (context === undefined) {
    throw new Error('usePipelineData must be used within a PipelineProvider')
  }
  return context
}

interface PipelineProviderProps {
  children: ReactNode
}

export const PipelineProvider: React.FC<PipelineProviderProps> = ({ children }) => {
  const [pipelineData, setPipelineData] = useState<PipelineData | null>(null)

  const refreshData = () => {
    const data = getPipelineData()
    setPipelineData(data)
  }

  const saveData = (data: Partial<PipelineData>) => {
    savePipelineData(data)
    refreshData()
  }

  const clearData = () => {
    clearPipelineData()
    setPipelineData(null)
  }

  useEffect(() => {
    refreshData()
  }, [])

  const value: PipelineContextType = {
    pipelineData,
    hasData: hasValidPipelineData(),
    saveData,
    clearData,
    refreshData
  }

  return (
    <PipelineContext.Provider value={value}>
      {children}
    </PipelineContext.Provider>
  )
}
