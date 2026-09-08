// Stocks-Master keep-alive + 定时触发器 worker.
//
// 职责一（keep-alive）：Render Free 15 分钟无入站流量即休眠。本 Worker 每 5 分钟
//   Cron 触发一次 GET 一次 Render 源站，让免费实例永久在线，$0 成本。
//
// 职责二（准时推送）：GitHub Actions 免费共享 Runner 的 schedule 是"尽力而为"，
//   北京晚间高峰期实测排队 4~11 小时（甚至整槽丢弃），导致"每日持仓分析推送"
//   经常 22:00 以后才发出。这里在窗口内用 GitHub REST API 主动
//   workflow_dispatch —— dispatch 触发的 run 不进 schedule 队列，秒级起跑。
//   GitHub 侧原有的 schedule 保留作为兜底（延迟 run 会被 workflow 内的
//   artifact 防重逻辑空转掉）。
//
// 部署（免费层 5 个 Cron Trigger 用 1 个）：
//   npx wrangler login
//   npx wrangler secret put GH_DISPATCH_TOKEN   # 需 Actions: write 的 PAT
//   npx wrangler deploy
//
// GH_DISPATCH_TOKEN 缺失时职责二整体 fail-soft 跳过，不影响 keep-alive。

// Ping RENDER 源站（不是 CF 加速域名），确保请求直达源站、不被边缘缓存命中。
const TARGET = "https://stocks-master.onrender.com/";

// ── 配置（全部走 wrangler.toml [vars]，无硬编码魔数）────────────────────
const DEFAULT_CONFIG = {
  OWNER: "benny1232123",
  REPO: "Stocks-Master",
  WORKFLOW: "daily-holdings.yml",
  REF: "master",
  WINDOW_START: "1830", // 北京时间 HHMM，含
  WINDOW_END: "1900", // 北京时间 HHMM，不含
  RECENT_RUN_GUARD_MIN: 30, // 该分钟内已有本 workflow 的 run → 视为已触发，跳过
};

function cfg(env) {
  return {
    owner: env.GITHUB_OWNER || DEFAULT_CONFIG.OWNER,
    repo: env.GITHUB_REPO || DEFAULT_CONFIG.REPO,
    workflow: env.GITHUB_WORKFLOW || DEFAULT_CONFIG.WORKFLOW,
    ref: env.GITHUB_REF || DEFAULT_CONFIG.REF,
    start: env.DISPATCH_HOLDINGS_WINDOW_START || DEFAULT_CONFIG.WINDOW_START,
    end: env.DISPATCH_HOLDINGS_WINDOW_END || DEFAULT_CONFIG.WINDOW_END,
    guardMin: Number(env.DISPATCH_RECENT_RUN_GUARD_MIN || DEFAULT_CONFIG.RECENT_RUN_GUARD_MIN),
  };
}

const API = "https://api.github.com";

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

/** 当天（北京日期）的持仓推送产物是否已存在 = 已推送过，勿重复触发。 */
async function alreadyPushed(env, c, ymd) {
  const url =
    `${API}/repos/${c.owner}/${c.repo}/actions/artifacts` +
    `?per_page=1&name=holdings-analysis-${ymd}`;
  const res = await fetch(url, { headers: ghHeaders(env.GH_DISPATCH_TOKEN) });
  if (!res.ok) {
    console.warn(`artifact 查询失败 HTTP ${res.status}，保守视为未推送`);
    return false;
  }
  const data = await res.json();
  return (data.total_count || 0) > 0;
}

/** 最近 guardMin 分钟内是否已有本 workflow 的 run（防 5 分钟轮询重复 dispatch）。 */
async function recentlyDispatched(env, c, now) {
  const url = `${API}/repos/${c.owner}/${c.repo}/actions/workflows/${c.workflow}/runs?per_page=5`;
  const res = await fetch(url, { headers: ghHeaders(env.GH_DISPATCH_TOKEN) });
  if (!res.ok) {
    console.warn(`runs 查询失败 HTTP ${res.status}，保守跳过本次触发`);
    return true;
  }
  const data = await res.json();
  const cutoff = now.getTime() - c.guardMin * 60 * 1000;
  return (data.workflow_runs || []).some(
    (r) => new Date(r.created_at).getTime() >= cutoff
  );
}

async function dispatchHoldings(env, now = new Date()) {
  const c = cfg(env);
  if (!env.GH_DISPATCH_TOKEN) {
    console.log("未配置 GH_DISPATCH_TOKEN，跳过定时 dispatch（keep-alive 不受影响）");
    return { skipped: "no_token" };
  }
  const { ymd, hhmm } = beijingNow(now);
  if (hhmm < c.start || hhmm >= c.end) {
    return { skipped: "out_of_window", beijing: `${ymd} ${hhmm}` };
  }
  if (await alreadyPushed(env, c, ymd)) {
    console.log(`北京 ${hhmm}：holdings-analysis-${ymd} 已存在，跳过`);
    return { skipped: "already_pushed", ymd };
  }
  if (await recentlyDispatched(env, c, now)) {
    console.log(`北京 ${hhmm}：${c.guardMin} 分钟内已有 run，跳过`);
    return { skipped: "recent_run", ymd };
  }

  const url = `${API}/repos/${c.owner}/${c.repo}/actions/workflows/${c.workflow}/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: { ...ghHeaders(env.GH_DISPATCH_TOKEN), "Content-Type": "application/json" },
    body: JSON.stringify({ ref: c.ref, inputs: {} }),
  });
  const ok = res.status === 204;
  console.log(
    `dispatch ${c.workflow}@${c.ref} -> HTTP ${res.status}` +
      (ok ? `（北京 ${hhmm}，信号日 ${ymd}）` : `：${(await res.text()).slice(0, 200)}`)
  );
  return { dispatched: ok, status: res.status, ymd, beijing: hhmm };
}

async function keepAlive() {
  try {
    const res = await fetch(TARGET, { method: "GET", redirect: "follow" });
    console.log(`keep-alive -> ${TARGET} HTTP ${res.status}`);
  } catch (err) {
    console.error("keep-alive failed:", err.message);
  }
}

export default {
  async scheduled(_event, env) {
    await keepAlive();
    try {
      await dispatchHoldings(env);
    } catch (err) {
      // 定时触发失败绝不能拖垮 keep-alive（上面已先行执行）
      console.error("dispatch holdings failed:", err.message);
    }
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    // 手动验证入口：GET /dispatch-holdings 立即跑一次触发逻辑并返回结果
    if (url.pathname === "/dispatch-holdings") {
      const out = await dispatchHoldings(env);
      return Response.json(out, { status: 200 });
    }
    if (url.pathname === "/status") {
      const { ymd, hhmm } = beijingNow();
      return Response.json(
        { beijing: { ymd, hhmm }, config: { ...cfg(env) } },
        { status: 200 }
      );
    }
    return new Response(
      "Stocks-Master keep-alive worker. Driven by Cron Trigger, not HTTP.",
      { status: 200 }
    );
  },
};
