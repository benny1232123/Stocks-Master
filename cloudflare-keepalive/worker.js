// Stocks-Master keep-alive + 定时触发器 worker.
//
// 职责一（keep-alive）：Render Free 15 分钟无入站流量即休眠。本 Worker 每 5 分钟
//   Cron 触发一次 GET 一次 Render 源站，让免费实例永久在线，$0 成本。
//
// 职责二（准时触发）：GitHub Actions 免费共享 Runner 的 schedule 是"尽力而为"，
//   北京晚间高峰期实测排队 4~11 小时（甚至整槽丢弃：*/20 槽位每天应有 12 个 run，
//   实际每天只记录到 1 个）。导致"每日选股 / 每日持仓分析推送"经常深夜才发出。
//   这里在各自窗口内用 GitHub REST API 主动 workflow_dispatch —— dispatch 触发的
//   run 不进 schedule 队列，秒级起跑。GitHub 侧原有 schedule 保留作兜底
//   （延迟到达的 run 会被 workflow 内已有的防重逻辑空转掉）。
//
// 部署（免费层 5 个 Cron Trigger 用 1 个）：
//   npx wrangler login
//   npx wrangler secret put GH_DISPATCH_TOKEN   # 需 Actions: write 的 PAT
//   npx wrangler deploy
//
// GH_DISPATCH_TOKEN 缺失时职责二整体 fail-soft 跳过，不影响 keep-alive。

// Ping RENDER 源站（不是 CF 加速域名），确保请求直达源站、不被边缘缓存命中。
const KEEPALIVE_TARGET = "https://stocks-master.onrender.com/";
const API = "https://api.github.com";

// ── 触发目标（窗口/名称走 wrangler.toml [vars]，默认值仅作兜底）──────────
// done() 是各目标自己的"今天已完成"判定，与对应 workflow 内的防重口径保持一致：
//   - 持仓推送：当天 artifact holdings-analysis-YYYYMMDD 已存在
//   - 每日选股：远端已有 stock_data/Daily-Action-List-YYYYMMDD.csv 提交
const TARGETS = [
  {
    key: "holdings",
    label: "每日持仓分析推送",
    workflowVar: "GITHUB_WORKFLOW",
    fallbackWorkflow: "daily-holdings.yml",
    startVar: "DISPATCH_HOLDINGS_WINDOW_START",
    fallbackStart: "1830",
    endVar: "DISPATCH_HOLDINGS_WINDOW_END",
    fallbackEnd: "1900",
    done: (env, c, ymd) => artifactExists(env, c, `holdings-analysis-${ymd}`),
  },
  {
    key: "pick",
    label: "每日选股",
    workflowVar: "DAILY_PICK_WORKFLOW",
    fallbackWorkflow: "daily-pick.yml",
    startVar: "DISPATCH_PICK_WINDOW_START",
    fallbackStart: "1630",
    endVar: "DISPATCH_PICK_WINDOW_END",
    fallbackEnd: "1700",
    done: (env, c, ymd) =>
      commitExists(env, c, `stock_data/Daily-Action-List-${ymd}.csv`),
  },
];

function cfg(env) {
  return {
    owner: env.GITHUB_OWNER || "benny1232123",
    repo: env.GITHUB_REPO || "Stocks-Master",
    ref: env.GITHUB_REF || "master",
    guardMin: Number(env.DISPATCH_RECENT_RUN_GUARD_MIN || 30),
  };
}

function ghHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "User-Agent": "stocks-master-keepalive",
  };
}

/** 北京时间（UTC+8）→ { ymd: "20260908", hhmm: "1830" } */
function beijingNow(now = new Date()) {
  const bj = new Date(now.getTime() + 8 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, "0");
  return {
    ymd: `${bj.getUTCFullYear()}${p(bj.getUTCMonth() + 1)}${p(bj.getUTCDate())}`,
    hhmm: `${p(bj.getUTCHours())}${p(bj.getUTCMinutes())}`,
  };
}

async function artifactExists(env, c, name) {
  const url = `${API}/repos/${c.owner}/${c.repo}/actions/artifacts?per_page=1&name=${name}`;
  const res = await fetch(url, { headers: ghHeaders(env.GH_DISPATCH_TOKEN) });
  if (!res.ok) {
    console.warn(`artifact 查询失败 HTTP ${res.status}，保守视为未完成`);
    return false;
  }
  const data = await res.json();
  return (data.total_count || 0) > 0;
}

async function commitExists(env, c, path) {
  const url =
    `${API}/repos/${c.owner}/${c.repo}/commits` +
    `?path=${encodeURIComponent(path)}&per_page=1`;
  const res = await fetch(url, { headers: ghHeaders(env.GH_DISPATCH_TOKEN) });
  if (!res.ok) {
    console.warn(`commits 查询失败 HTTP ${res.status}，保守视为未完成`);
    return false;
  }
  const data = await res.json();
  return Array.isArray(data) && data.length > 0;
}

/** 最近 guardMin 分钟内是否已有该 workflow 的 run（防 5 分钟轮询重复 dispatch）。 */
async function recentlyDispatched(env, c, workflow, now) {
  const url = `${API}/repos/${c.owner}/${c.repo}/actions/workflows/${workflow}/runs?per_page=5`;
  const res = await fetch(url, { headers: ghHeaders(env.GH_DISPATCH_TOKEN) });
  if (!res.ok) {
    console.warn(`runs 查询失败 HTTP ${res.status}，保守跳过触发 ${workflow}`);
    return true;
  }
  const data = await res.json();
  const cutoff = now.getTime() - c.guardMin * 60 * 1000;
  return (data.workflow_runs || []).some(
    (r) => new Date(r.created_at).getTime() >= cutoff
  );
}

async function dispatch(env, target, now = new Date()) {
  const c = cfg(env);
  const workflow = env[target.workflowVar] || target.fallbackWorkflow;
  const start = env[target.startVar] || target.fallbackStart;
  const end = env[target.endVar] || target.fallbackEnd;

  if (!env.GH_DISPATCH_TOKEN) {
    return { key: target.key, skipped: "no_token" };
  }
  const { ymd, hhmm } = beijingNow(now);
  if (hhmm < start || hhmm >= end) {
    return {
      key: target.key,
      skipped: "out_of_window",
      window: `${start}-${end}`,
      beijing: `${ymd} ${hhmm}`,
    };
  }
  if (await target.done(env, c, ymd)) {
    console.log(`北京 ${hhmm}：${target.label} 今日已完成，跳过`);
    return { key: target.key, skipped: "already_done", ymd };
  }
  if (await recentlyDispatched(env, c, workflow, now)) {
    console.log(`北京 ${hhmm}：${target.label} ${c.guardMin} 分钟内已有 run，跳过`);
    return { key: target.key, skipped: "recent_run", ymd };
  }

  const url = `${API}/repos/${c.owner}/${c.repo}/actions/workflows/${workflow}/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: { ...ghHeaders(env.GH_DISPATCH_TOKEN), "Content-Type": "application/json" },
    body: JSON.stringify({ ref: c.ref, inputs: {} }),
  });
  const ok = res.status === 204;
  console.log(
    `dispatch ${workflow}@${c.ref} -> HTTP ${res.status}` +
      (ok
        ? `（${target.label}，北京 ${hhmm}，信号日 ${ymd}）`
        : `：${(await res.text()).slice(0, 200)}`)
  );
  return {
    key: target.key,
    dispatched: ok,
    status: res.status,
    workflow,
    ymd,
    beijing: hhmm,
  };
}

async function dispatchAll(env, now = new Date()) {
  const out = [];
  for (const t of TARGETS) {
    try {
      out.push(await dispatch(env, t, now));
    } catch (err) {
      // 单个目标失败不影响其他目标、更不影响 keep-alive（已先行执行）
      console.error(`dispatch ${t.key} failed:`, err.message);
      out.push({ key: t.key, error: err.message });
    }
  }
  return out;
}

async function keepAlive() {
  try {
    const res = await fetch(KEEPALIVE_TARGET, { method: "GET", redirect: "follow" });
    console.log(`keep-alive -> ${KEEPALIVE_TARGET} HTTP ${res.status}`);
  } catch (err) {
    console.error("keep-alive failed:", err.message);
  }
}

export default {
  async scheduled(_event, env) {
    await keepAlive();
    await dispatchAll(env);
  },

  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    // 手动验证入口（幂等、只读）：立即跑一次触发逻辑并返回判定结果
    if (path === "/dispatch") {
      return Response.json({ beijing: beijingNow(), results: await dispatchAll(env) });
    }
    if (path.startsWith("/dispatch/")) {
      const key = path.slice("/dispatch/".length);
      const t = TARGETS.find((x) => x.key === key);
      if (!t) {
        return Response.json({ error: `unknown target: ${key}` }, { status: 404 });
      }
      return Response.json({ beijing: beijingNow(), results: [await dispatch(env, t)] });
    }
    if (path === "/status") {
      const c = cfg(env);
      const { ymd, hhmm } = beijingNow();
      return Response.json({
        beijing: { ymd, hhmm },
        repo: `${c.owner}/${c.repo}`,
        ref: c.ref,
        guardMin: c.guardMin,
        token: env.GH_DISPATCH_TOKEN ? "set" : "missing",
        targets: TARGETS.map((t) => ({
          key: t.key,
          label: t.label,
          workflow: env[t.workflowVar] || t.fallbackWorkflow,
          window: [env[t.startVar] || t.fallbackStart, env[t.endVar] || t.fallbackEnd],
        })),
      });
    }
    return new Response(
      "Stocks-Master keep-alive worker. Driven by Cron Trigger, not HTTP.",
      { status: 200 }
    );
  },
};
