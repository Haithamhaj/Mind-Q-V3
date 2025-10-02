export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center">
      <div className="text-center">
        <h1 className="text-5xl font-bold text-gray-900 mb-4">
          Master EDA Platform
        </h1>
        <p className="text-xl text-gray-600 mb-8">
          Intelligent Data Preparation for Machine Learning
        </p>
        <div className="bg-white rounded-lg shadow-lg p-8 inline-block">
          <p className="text-green-600 font-semibold">✓ System Ready</p>
          <p className="text-sm text-gray-500 mt-2">Version 1.2.2</p>
        </div>
      </div>
    </div>
  )
}
