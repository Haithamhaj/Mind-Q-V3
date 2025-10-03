import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Home from './pages/Home'
import QualityControl from './pages/QualityControl'

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/quality-control" element={<QualityControl />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
