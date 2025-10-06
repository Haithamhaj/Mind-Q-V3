import React, { useState, useRef, useEffect } from 'react'
import { Send, Bot, User } from 'lucide-react'

interface Message {
  role: 'user' | 'assistant'
  content: string
  timestamp: string
}

export default function ChatInterface() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [language, setLanguage] = useState<'en' | 'ar'>('en')
  const messagesEndRef = useRef<HTMLDivElement>(null)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }
  useEffect(() => { scrollToBottom() }, [messages])

  const sendMessage = async () => {
    if (!input.trim() || loading) return
    const userMessage: Message = { role: 'user', content: input, timestamp: new Date().toISOString() }
    setMessages(prev => [...prev, userMessage])
    setInput('')
    setLoading(true)
    try {
      const base = (import.meta as any).env?.VITE_API_URL || 'http://localhost:8000'
      const response = await fetch(`${base}/api/v1/llm-analysis/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: userMessage.content, language })
      })
      if (!response.ok) throw new Error('Chat request failed')
      const data = await response.json()
      setMessages(prev => [...prev, data])
    } catch (error) {
      setMessages(prev => [...prev, { role: 'assistant', content: 'Sorry, I encountered an error. Please try again.', timestamp: new Date().toISOString() }])
    } finally {
      setLoading(false)
    }
  }

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  return (
    <div className="bg-white rounded-lg shadow-lg flex flex-col h-[700px]">
      <div className="p-4 border-b flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Bot className="w-6 h-6 text-purple-600" />
          <h2 className="text-lg font-semibold text-gray-900">Ask AI About Your Models</h2>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={() => setLanguage('en')} className={`px-3 py-1 rounded ${language === 'en' ? 'bg-purple-600 text-white' : 'bg-gray-200 text-gray-700'}`}>EN</button>
          <button onClick={() => setLanguage('ar')} className={`px-3 py-1 rounded ${language === 'ar' ? 'bg-purple-600 text-white' : 'bg-gray-200 text-gray-700'}`}>AR</button>
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.length === 0 ? (
          <div className="text-center py-12">
            <Bot className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <p className="text-gray-500">Ask me anything about your model results, features, or recommendations!</p>
            <div className="mt-6 space-y-2 text-sm text-gray-600">
              <p>Example questions:</p>
              <ul className="space-y-1">
                <li>- Why is transit_time the most important feature?</li>
                <li>- Which model should I use for production?</li>
                <li>- How can I improve recall?</li>
                <li>- Explain the confusion matrix for Random Forest</li>
              </ul>
            </div>
          </div>
        ) : (
          messages.map((msg, idx) => (
            <div key={idx} className={`flex items-start gap-3 ${msg.role === 'user' ? 'justify-end' : ''}`}>
              {msg.role === 'assistant' && (
                <div className="w-8 h-8 rounded-full bg-purple-100 flex items-center justify-center flex-shrink-0">
                  <Bot className="w-5 h-5 text-purple-600" />
                </div>
              )}
              <div className={`max-w-[70%] rounded-lg p-4 ${msg.role === 'user' ? 'bg-purple-600 text-white' : 'bg-gray-100 text-gray-900'}`}>
                <p className="whitespace-pre-wrap">{msg.content}</p>
                <p className={`text-xs mt-2 ${msg.role === 'user' ? 'text-purple-200' : 'text-gray-500'}`}>{new Date(msg.timestamp).toLocaleTimeString()}</p>
              </div>
              {msg.role === 'user' && (
                <div className="w-8 h-8 rounded-full bg-gray-200 flex items-center justify-center flex-shrink-0">
                  <User className="w-5 h-5 text-gray-600" />
                </div>
              )}
            </div>
          ))
        )}
        {loading && (
          <div className="flex items-start gap-3">
            <div className="w-8 h-8 rounded-full bg-purple-100 flex items-center justify-center flex-shrink-0">
              <Bot className="w-5 h-5 text-purple-600" />
            </div>
            <div className="bg-gray-100 rounded-lg p-4">
              <div className="flex gap-2">
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"></div>
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.1s' }}></div>
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      <div className="p-4 border-t">
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder={language === 'ar' ? 'اسأل سؤالك هنا...' : 'Ask your question here...'}
            className="flex-1 px-4 py-3 border rounded-lg focus:outline-none focus:ring-2 focus:ring-purple-600"
            disabled={loading}
          />
          <button
            onClick={sendMessage}
            disabled={loading || !input.trim()}
            className="px-6 py-3 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:bg-gray-400 font-semibold flex items-center gap-2"
          >
            <Send className="w-5 h-5" />
          </button>
        </div>
      </div>
    </div>
  )
}




