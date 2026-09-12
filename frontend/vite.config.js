import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { execSync } from 'child_process'

// 构建版本标识：git 短 SHA + 构建时间（注入前端常量，界面常驻显示；
// 用于分辨「浏览器缓存旧版」vs「新构建」——Render 构建环境里有 git，
// 异常时回退纯时间戳）
function buildVersion() {
  try {
    const sha = execSync('git rev-parse --short HEAD').toString().trim()
    const t = new Date().toISOString().replace('T', ' ').slice(5, 16)
    return { sha, time: t }
  } catch {
    const t = new Date().toISOString().replace('T', ' ').slice(5, 16)
    return { sha: 'unknown', time: t }
  }
}

const VER = buildVersion()

export default defineConfig({
  base: './',
  define: {
    __BUILD_SHA__: JSON.stringify(VER.sha),
    __BUILD_TIME__: JSON.stringify(VER.time),
  },
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8000',
      '/health': 'http://localhost:8000',
      '/admin': 'http://localhost:8000',
    },
  },
})