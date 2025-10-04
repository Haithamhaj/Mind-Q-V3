import { Link } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { ArrowRight } from 'lucide-react'

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center">
      <div className="text-center max-w-2xl mx-auto px-6">
        <h1 className="text-5xl font-bold text-gray-900 mb-4">
          Mind-Q-V3 Platform
        </h1>
        <p className="text-xl text-gray-600 mb-8">
          Intelligent Data Preparation for Machine Learning
        </p>
        
        <div className="bg-white rounded-lg shadow-lg p-8 mb-8">
          <p className="text-green-600 font-semibold mb-4">✓ System Ready</p>
          <p className="text-sm text-gray-500 mb-6">Version 1.2.2</p>
          
          <div className="space-y-4">
            <Link to="/full-pipeline">
              <Button className="w-full" size="lg">
                🚀 Mind-Q-V3 Real Analysis Pipeline
                <ArrowRight className="ml-2 h-4 w-4" />
              </Button>
            </Link>
            
            <Link to="/quality-control">
              <Button variant="outline" className="w-full">
                Quick Start: Quality Control Only
                <ArrowRight className="ml-2 h-4 w-4" />
              </Button>
            </Link>
            
            <div className="text-sm text-gray-500">
              <p><strong>Mind-Q-V3 Pipeline</strong>: Real backend analysis with your data</p>
              <p><strong>Quick Start</strong>: Quality control + missing data analysis only</p>
            </div>
          </div>
        </div>
        
        <div className="text-sm text-gray-400">
          <p>Supported formats: CSV, Excel (XLSX, XLS)</p>
        </div>
      </div>
    </div>
  )
}
