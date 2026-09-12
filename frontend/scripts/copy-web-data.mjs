// 构建后把数据快照拷进 dist（Cloudflare Pages 等纯静态部署的数据源）。
// 由 package.json 的 build 链调用（cwd = frontend/）。
import { cpSync, existsSync, readdirSync, mkdirSync } from 'fs'
import { resolve } from 'path'

const src = resolve(process.cwd(), '../stock_data/web_data')
const dest = resolve(process.cwd(), 'dist/web_data')

if (!existsSync(src)) {
  console.warn('[copy-web-data] 源不存在，跳过:', src)
  process.exit(0)
}
mkdirSync(dest, { recursive: true })
cpSync(src, dest, { recursive: true })
const files = readdirSync(dest)
console.log(`[copy-web-data] ${files.length} 个快照 → dist/web_data/ (${files.join(', ')})`)
