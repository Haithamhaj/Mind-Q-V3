import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Home from './pages/Home'
import QualityControl from './pages/QualityControl'
import MissingDataAnalysis from './pages/MissingDataAnalysis'
import FullEDAPipeline from './pages/FullEDAPipeline'

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/quality-control" element={<QualityControl />} />
        <Route path="/missing-data-analysis" element={<MissingDataAnalysis />} />
        <Route path="/full-pipeline" element={<FullEDAPipeline />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
