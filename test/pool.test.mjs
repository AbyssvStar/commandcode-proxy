// 密钥池：请求用哪把 key、额度耗尽怎么换、全池耗尽怎么回、threadId 如何按 key 隔离。
// 全部走 loopback mock 上游，不需要真 key。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import crypto from 'node:crypto';
import { startMockUpstream, startProxy } from './helpers.mjs';

const KEY_A = 'user_aaaa1111';
const KEY_B = 'user_bbbb2222';

function poolConfig(overrides = {}) {
  return {
    port: 0,
    host: '127.0.0.1',
    apiBase: 'https://api.commandcode.ai',
    projectSlug: 'cc-proxy',
    keyPool: {
      mode: 'pool',
      file: 'keys.json',
      stateFile: 'keys.state.json',
      switchAtRatio: 0.95,
      probeIntervalMs: 3600000,
      maxAttempts: 3,
      adminToken: '',
      ...overrides,
    },
  };
}

function quotaBody(weeklyUsed, { weeklyCap = 6, creditsLeft = 5 } = {}) {
  return {
    credits: { monthlyCredits: creditsLeft, purchasedCredits: 0, freeCredits: 0, belowThreshold: false },
    windowLimits: {
      limited: true,
      exceeded: null,
      fiveHour: { used: 0.1, cap: 3, exceeded: false, resetAt: Date.now() + 3600_000 },
      weekly: { used: weeklyUsed, cap: weeklyCap, exceeded: false, resetAt: Date.now() + 86400_000 },
    },
  };
}

const QUOTA_400 = {
  status: 400,
  body: { success: false, error: { code: 'USAGE_EXCEEDED', message: 'weekly usage limit reached' } },
};

async function startPool({ keyPool = {}, credits = {}, generate = {}, onRequest } = {}) {
  const cwd = mkdtempSync(join(tmpdir(), 'ccp-pool-'));
  writeFileSync(join(cwd, 'config.json'), JSON.stringify(poolConfig(keyPool)));
  writeFileSync(join(cwd, 'keys.json'), JSON.stringify([
    { key: KEY_A, label: 'A' },
    { key: KEY_B, label: 'B' },
  ]));

  const mock = await startMockUpstream({
    onRequest: async (req, res, rec) => {
      const key = String(req.headers.authorization || '').replace(/^Bearer /, '');
      if (req.url === '/alpha/billing/credits') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(credits[key] || quotaBody(0.1)));
        return;
      }
      if (req.url === '/alpha/generate' && generate[key]) {
        res.writeHead(generate[key].status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(generate[key].body));
        return;
      }
      if (onRequest) await onRequest(req, res, rec);
    },
  });

  const proxy = await startProxy({ upstreamPort: mock.port, cwd });
  return {
    mock, proxy, cwd,
    generateAuth: () => mock.seen
      .filter(s => s.url === '/alpha/generate')
      .map(s => String(s.headers.authorization || '').replace(/^Bearer /, '')),
    generateBodies: () => mock.seen
      .filter(s => s.url === '/alpha/generate')
      .map(s => JSON.parse(s.raw)),
    async close() {
      await proxy.kill();
      await mock.close();
      try { rmSync(cwd, { recursive: true, force: true }); } catch {}
    },
  };
}

async function readState(cwd, timeoutMs = 3000) {
  const path = join(cwd, 'keys.state.json');
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try { return JSON.parse(readFileSync(path, 'utf-8')); } catch {}
    await sleep(100);
  }
  return null;
}

const chatBody = () => ({
  model: 'deepseek/deepseek-v4-flash',
  messages: [{ role: 'user', content: 'hi' }],
});

test('池模式：没有 Authorization 头也能用，请求带的是池里的 key', async () => {
  const t = await startPool();
  try {
    const res = await t.proxy.post('/v1/chat/completions', chatBody());
    assert.equal(res.status, 200);
    assert.deepEqual(t.generateAuth(), [KEY_A]);
  } finally {
    await t.close();
  }
});

test('额度耗尽（400）→ 同一个客户端请求内换下一把 key，客户端仍拿到 200', async () => {
  const t = await startPool({ generate: { [KEY_A]: QUOTA_400 } });
  try {
    const res = await t.proxy.post('/v1/chat/completions', chatBody());
    assert.equal(res.status, 200);
    // A 先失败，B 重试成功：对客户端只发生一次请求
    assert.deepEqual(t.generateAuth(), [KEY_A, KEY_B]);

    const state = await readState(t.cwd);
    const a = Object.values(state.keys).find(k => k.label === 'A');
    assert.equal(a.cooldownReason, 'weekly_limit');
    assert.ok(a.cooldownUntil > Date.now(), 'A 应冷却到周窗口 reset');
  } finally {
    await t.close();
  }
});

test('全池额度耗尽 → 429 insufficient_quota（不是上游的 400），retry_after 有上界', async () => {
  const t = await startPool({ generate: { [KEY_A]: QUOTA_400, [KEY_B]: QUOTA_400 } });
  try {
    const res = await t.proxy.post('/v1/chat/completions', chatBody());
    assert.equal(res.status, 429);
    const body = await res.json();
    assert.equal(body.error.type, 'insufficient_quota');
    assert.ok(body.retry_after >= 30 && body.retry_after <= 300, `retry_after 应在 30~300，实际 ${body.retry_after}`);
    assert.ok(res.headers.get('retry-after'), '应带 Retry-After 头');
  } finally {
    await t.close();
  }
});

test('401 → 换下一把 key 重试一次；同一把 key 连续两次 401 才禁用', async () => {
  const t = await startPool({
    generate: { [KEY_A]: { status: 401, body: { error: { message: 'invalid api key' } } } },
  });
  try {
    const res = await t.proxy.post('/v1/chat/completions', chatBody());
    assert.equal(res.status, 200);
    assert.deepEqual(t.generateAuth(), [KEY_A, KEY_B]);

    const state = await readState(t.cwd);
    const a = Object.values(state.keys).find(k => k.label === 'A');
    assert.equal(a.cooldownReason, 'auth_failed');
  } finally {
    await t.close();
  }
});

test('探测发现周额度达 95% → 冷却该 key，后续请求直接走另一把', async () => {
  // probeIntervalMs 同时决定首探延迟的上界（500ms），否则测试要等 5s
  const t = await startPool({
    keyPool: { probeIntervalMs: 500 },
    credits: { [KEY_A]: quotaBody(5.9) },
  });
  try {
    // 等首探 + 状态落盘
    let state = null;
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      state = await readState(t.cwd, 200);
      const a = state && Object.values(state.keys).find(k => k.label === 'A');
      if (a && a.cooldownReason === 'weekly_limit') break;
      await sleep(200);
    }
    const a = Object.values(state.keys).find(k => k.label === 'A');
    assert.equal(a.cooldownReason, 'weekly_limit');

    const res = await t.proxy.post('/v1/chat/completions', chatBody());
    assert.equal(res.status, 200);
    assert.deepEqual(t.generateAuth(), [KEY_B], 'A 已被探测冷却，不应再被使用');
  } finally {
    await t.close();
  }
});

test('同一会话落到不同 key 上用不同 threadId；同一 key 上保持稳定', async () => {
  const t = await startPool({ generate: { [KEY_A]: QUOTA_400 } });
  try {
    const session = 'sess-shared-1';
    const body = { ...chatBody(), prompt_cache_key: session };
    const res1 = await t.proxy.post('/v1/chat/completions', body);
    assert.equal(res1.status, 200);
    const [b1, b2] = t.generateBodies();

    const salted = (key) => {
      const h = crypto.createHash('sha256').update(`${key}\0${session}`).digest('hex');
      return `${h.slice(0, 8)}-${h.slice(8, 12)}-5${h.slice(13, 16)}-a${h.slice(17, 20)}-${h.slice(20, 32)}`;
    };
    // 语义：thread 身份按 key 隔离，但派生是确定性的（可复算）
    assert.equal(b1.threadId, salted(KEY_A));
    assert.equal(b2.threadId, salted(KEY_B));
    assert.notEqual(b1.threadId, b2.threadId);

    // 同一会话再来一次：现在只有 B 可用，threadId 必须与上一次 B 的相同（缓存可复用）
    const res2 = await t.proxy.post('/v1/chat/completions', body);
    assert.equal(res2.status, 200);
    const bodies = t.generateBodies();
    assert.equal(bodies[bodies.length - 1].threadId, salted(KEY_B));
  } finally {
    await t.close();
  }
});
