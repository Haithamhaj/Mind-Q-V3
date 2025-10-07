import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Home from './pages/Home'
import QualityControl from './pages/QualityControl'
import MissingDataAnalysis from './pages/MissingDataAnalysis'
import FullEDAPipeline from './pages/FullEDAPipeline'
import BIDashboard from './pages/BIDashboard'
import LLMAnalysis from './pages/LLMAnalysis'
import { PipelineProvider } from './contexts/PipelineContext'
import AgentViewer from './agent/pages/AgentViewer'

function App() {
  return (
    <PipelineProvider>
      <BrowserRouter
        future={{
          v7_startTransition: true,
          v7_relativeSplatPath: true
        }}
      >
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/quality-control" element={<QualityControl />} />
          <Route path="/missing-data-analysis" element={<MissingDataAnalysis />} />
          <Route path="/full-pipeline" element={<FullEDAPipeline />} />
          <Route path="/bi-dashboard" element={<BIDashboard />} />
          <Route path="/llm-analysis" element={<LLMAnalysis />} />
          <Route path="/agent-viewer" element={<AgentViewer />} />
        </Routes>
      </BrowserRouter>
    </PipelineProvider>
  )
}

export default App
