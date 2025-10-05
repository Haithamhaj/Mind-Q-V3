import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Home from './pages/Home'
import QualityControl from './pages/QualityControl'
import MissingDataAnalysis from './pages/MissingDataAnalysis'
import FullEDAPipeline from './pages/FullEDAPipeline'
import BIDashboard from './pages/BIDashboard'
import { PipelineProvider } from './contexts/PipelineContext'

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
        </Routes>
      </BrowserRouter>
    </PipelineProvider>
  )
}

export default App
