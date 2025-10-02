import axios from 'axios'

const API_BASE_URL = (import.meta as any).env?.VITE_API_URL || 'http://localhost:8000'

export const apiClient = axios.create({
  baseURL: `${API_BASE_URL}/api/v1`,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Health check
export const checkHealth = async () => {
  const response = await axios.get(`${API_BASE_URL}/health`)
  return response.data
}
