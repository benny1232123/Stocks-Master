import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './styles.css'

// Render（或本地 dev）走真实后端 /api，无需静态拦截层。
ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
