#!/usr/bin/env node
/**
 * Stream Processor Integration Tests
 *
 * Tests server-side stream processing pipelines: create/pause/resume/delete
 * processors, filter expressions, aggregation (COUNT/SUM/LAST), groupBy, and
 * multi-processor fan-out / pipeline topologies.
 *
 * Requirements:
 *   - simpleMQ running at BASE_URL (default: http://localhost:8080)
 *   - Node 18+ (native fetch + node:test runner)
 *
 * Usage:
 *   node test_processors.js
 *   BASE_URL=http://localhost:8080 node test_processors.js
 */

const { describe, it, before, after, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const BASE = process.env.BASE_URL || 'http://localhost:8080';
const API  = `${BASE}/api`;

// ─── Helpers ─────────────────────────────────────────────────────────────────

async function req(path, method = 'GET', body = null) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body !== null) opts.body = JSON.stringify(body);
  const res = await fetch(`${API}${path}`, opts);
  if (res.status === 204 || res.status === 201) {
    const text = await res.text();
    return text ? JSON.parse(text) : null;
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${method} ${path} → ${res.status}: ${text}`);
  }
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * Polls `fn()` every `interval` ms until it returns true or `timeout` ms elapses.
 * Throws an AssertionError with `message` on timeout.
 */
async function waitUntil(fn, message, timeout = 5000, interval = 200) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await sleep(interval);
  }
  assert.fail(`Timeout (${timeout}ms): ${message}`);
}

/**
 * Polls a STANDARD queue, accumulating messages one-by-one until `expected`
 * are received or `timeout` ms elapses.  Unlike countMessages(), this never
 * "drains and resets" — each message is counted exactly once regardless of
 * inter-arrival gaps.
 */
async function awaitCount(queueName, expected, timeout = 7000) {
  let count = 0;
  const deadline = Date.now() + timeout;
  while (count < expected) {
    if (Date.now() >= deadline) {
      assert.fail(`Timeout (${timeout}ms): expected ${expected} messages in ${queueName}, got ${count}`);
    }
    const res = await fetch(`${API}/poll/${encodeURIComponent(queueName)}`);
    if (res.status === 204) {
      await sleep(200);
      continue;
    }
    if (res.ok) {
      const msg = await res.json();
      if (msg && msg.id) {
        await req(`/poll/ack/${encodeURIComponent(msg.id)}`, 'POST').catch(() => {});
        count++;
      }
    }
  }
  return count;
}

/** Drains all messages from a STANDARD queue (polls until empty, no ack needed). */
async function drainQueue(queueName, maxMessages = 200) {
  for (let i = 0; i < maxMessages; i++) {
    const res = await fetch(`${API}/poll/${encodeURIComponent(queueName)}`);
    if (res.status === 204) return i;
    if (res.ok) {
      const msg = await res.json();
      if (msg && msg.id) {
        await req(`/poll/ack/${encodeURIComponent(msg.id)}`, 'POST').catch(() => {});
      }
    }
  }
  return maxMessages;
}

/** Counts messages currently in a STANDARD queue by draining and counting. */
async function countMessages(queueName, limit = 200) {
  let count = 0;
  const ids = [];
  for (let i = 0; i < limit; i++) {
    const res = await fetch(`${API}/poll/${encodeURIComponent(queueName)}`);
    if (res.status === 204) break;
    if (res.ok) {
      const msg = await res.json();
      if (msg && msg.id) { ids.push(msg.id); count++; }
    }
  }
  // Re-ack all so they are consumed (alternatively nack+requeue — we just ack here)
  for (const id of ids) {
    await req(`/poll/ack/${encodeURIComponent(id)}`, 'POST').catch(() => {});
  }
  return count;
}

/** Publishes a batch of messages to an exchange. */
async function publishBatch(exchange, messages) {
  for (const { routingKey = '', payload } of messages) {
    await req(`/publish/${encodeURIComponent(exchange)}`, 'POST', { routingKey, payload });
  }
}

/** Creates a processor, returns its id. */
async function createProcessor(opts) {
  const p = await req('/processors', 'POST', {
    name: opts.name,
    sourceQueue: opts.sourceQueue,
    filterExpression: opts.filterExpression ?? null,
    targetExchange: opts.targetExchange,
    targetRoutingKey: opts.targetRoutingKey ?? null,
    aggregationType: opts.aggregationType ?? null,
    aggregationField: opts.aggregationField ?? null,
    aggregationGroupBy: opts.aggregationGroupBy ?? null,
  });
  assert.ok(p && p.id, 'createProcessor should return an object with id');
  return p.id;
}

/** Deletes a processor by id. */
async function deleteProcessor(id) {
  await req(`/processors/${encodeURIComponent(id)}`, 'DELETE');
}

/** Gets aggregation state for a processor. Returns an array of state records. */
async function getProcessorState(id) {
  return await req(`/processors/${encodeURIComponent(id)}/state`);
}

/** Parses a state record's stateValue JSON. */
function parseState(state) {
  return JSON.parse(state.stateValue);
}

// ─── Unique name helper ───────────────────────────────────────────────────────

let _seq = 0;
function uid(prefix = 'p') { return `${prefix}-${Date.now()}-${++_seq}`; }

// ─── Server readiness ─────────────────────────────────────────────────────────

/**
 * Waits until the server is up AND a Raft leader has been elected.
 * Just because the HTTP endpoint responds doesn't mean write operations work —
 * the Raft cluster needs a quorum and a leader before any replicated command
 * can succeed.
 */
async function waitForServer(retries = 30, delay = 2000) {
  process.stdout.write(`  Waiting for server at ${BASE}`);
  for (let i = 0; i < retries; i++) {
    try {
      const r = await fetch(`${API}/management/summary`);
      if (r.ok) {
        const body = await r.json();
        if (body.leaderId) {
          console.log(` ✓  (leader: ${body.leaderId})\n`);
          return;
        }
      }
    } catch {}
    process.stdout.write('.');
    await sleep(delay);
  }
  throw new Error(`Cluster not ready after ${retries} attempts`);
}

/** Makes a request, ignoring 409 Conflict (idempotent setup). */
async function setupReq(path, method = 'POST', body = null) {
  try {
    return await req(path, method, body);
  } catch (err) {
    // Idempotent: ignore "already exists" conflicts
    if (err.message.includes('409') || err.message.includes('already')) return null;
    throw err;
  }
}

// ─── Cluster readiness only ───────────────────────────────────────────────────

// Each test gets its own isolated STREAM source queue (via makeSrc) so processors
// never scan messages from other tests, avoiding both contamination and the
// per-message Raft-offset overhead from accumulated history.

before(async () => {
  await waitForServer();

  // Delete any processors left over from a previous run so they don't flood
  // the Raft client with offset-update commands during this run.
  try {
    const stale = await req('/processors');
    for (const p of stale) {
      await req(`/processors/${p.id}`, 'DELETE').catch(() => {});
    }
    if (stale.length) console.log(`  Cleaned up ${stale.length} stale processors\n`);
  } catch (_) {}
});

// ─── Utility: create a private target exchange+queue+binding for a test ───────

async function makeTarget(suffix) {
  const ex = `test-target-ex-${suffix}`;
  const q  = `test-target-q-${suffix}`;
  const rk = 'out';
  await setupReq('/management/exchanges', 'POST', { name: ex, type: 'DIRECT', durable: true });
  await setupReq('/management/queues',   'POST', { name: q, queueGroup: 'test', durable: true, autoDelete: false });
  await setupReq('/management/bindings', 'POST', { exchangeName: ex, queueName: q, routingKey: rk });
  return { ex, q, rk };
}

async function tearTarget(ex, q) {
  await req(`/management/exchanges/${encodeURIComponent(ex)}`, 'DELETE').catch(() => {});
  await req(`/management/queues/${encodeURIComponent(q)}`, 'DELETE').catch(() => {});
}

/**
 * Creates a fresh, isolated STREAM source queue + FANOUT exchange + binding.
 * Each test gets its own source so processors never scan messages from other tests.
 */
async function makeSrc(suffix) {
  const q  = `test-src-q-${suffix}`;
  const ex = `test-src-ex-${suffix}`;
  await setupReq('/management/queues',   'POST', { name: q, queueGroup: 'test', durable: true, autoDelete: false, queueType: 'STREAM' });
  await setupReq('/management/exchanges','POST', { name: ex, type: 'FANOUT', durable: true });
  await setupReq('/management/bindings', 'POST', { exchangeName: ex, queueName: q });
  return { q, ex };
}

async function tearSrc(ex, q) {
  await req(`/management/exchanges/${encodeURIComponent(ex)}`, 'DELETE').catch(() => {});
  await req(`/management/queues/${encodeURIComponent(q)}`, 'DELETE').catch(() => {});
}

// =============================================================================
// TEST SUITE
// =============================================================================

describe('Stream Processors', () => {

  // ── 1. CRUD ────────────────────────────────────────────────────────────────

  describe('CRUD', () => {
    it('creates a processor and lists it', async () => {
      const tag = uid('crud');
      const { ex, q } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: 'out',
      });

      const list = await req('/processors');
      assert.ok(Array.isArray(list), 'list should be an array');
      const found = list.find(p => p.id === pid);
      assert.ok(found, 'created processor should appear in list');
      assert.equal(found.status, 'RUNNING');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('fetches a single processor by id', async () => {
      const tag = uid('get');
      const { ex, q } = await makeTarget(tag);
      const src = await makeSrc(tag);
      const name = `proc-${tag}`;
      const pid = await createProcessor({ name, sourceQueue: src.q, targetExchange: ex });

      const p = await req(`/processors/${pid}`);
      assert.equal(p.id, pid);
      assert.equal(p.name, name);
      assert.equal(p.sourceQueue, src.q);

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('deletes a processor', async () => {
      const tag = uid('del');
      const { ex, q } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`, sourceQueue: src.q, targetExchange: ex,
      });
      await deleteProcessor(pid);

      const list = await req('/processors');
      assert.ok(!list.find(p => p.id === pid), 'deleted processor should not be in list');
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });
  });

  // ── 2. Pause / Resume ────────────────────────────────────────────────────

  describe('Pause / Resume', () => {
    it('pauses a processor — no new messages forwarded after pause', async () => {
      const tag = uid('pause');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
      });

      // Let the processor start up, then pause it
      await sleep(1500);
      await req(`/processors/${pid}/pause`, 'POST');

      const p = await req(`/processors/${pid}`);
      assert.equal(p.status, 'PAUSED');

      // Record how many messages are already in target
      const countBefore = await countMessages(q);

      // Publish messages while paused
      await publishBatch(src.ex, [
        { payload: JSON.stringify({ type: 'test', seq: 'paused-1' }) },
        { payload: JSON.stringify({ type: 'test', seq: 'paused-2' }) },
        { payload: JSON.stringify({ type: 'test', seq: 'paused-3' }) },
      ]);

      // Wait long enough that the processor *would* have run if active
      await sleep(2500);

      const countAfter = await countMessages(q);
      assert.equal(countAfter, 0, 'No messages should be forwarded while paused');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('resumes a processor from the correct offset', async () => {
      const tag = uid('resume');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
      });

      // Pause before publishing
      await sleep(1200);
      await req(`/processors/${pid}/pause`, 'POST');
      await drainQueue(q);

      // Publish 4 messages while paused
      await publishBatch(src.ex, [
        { payload: JSON.stringify({ type: 'resume-test', n: 1 }) },
        { payload: JSON.stringify({ type: 'resume-test', n: 2 }) },
        { payload: JSON.stringify({ type: 'resume-test', n: 3 }) },
        { payload: JSON.stringify({ type: 'resume-test', n: 4 }) },
      ]);

      // Resume
      await req(`/processors/${pid}/resume`, 'POST');
      const p2 = await req(`/processors/${pid}`);
      assert.equal(p2.status, 'RUNNING');

      // Wait for all 4 to be forwarded (processor runs every 1s, batch = 100)
      await awaitCount(q, 4, 8000);

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });
  });

  // ── 3. Filter expressions ──────────────────────────────────────────────────

  describe('Filter expressions', () => {
    it('pass-all (no filter) — forwards every message', async () => {
      const tag = uid('passall');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        filterExpression: null,
      });

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ kind: 'A' }) },
        { payload: JSON.stringify({ kind: 'B' }) },
        { payload: JSON.stringify({ kind: 'C' }) },
      ]);

      await awaitCount(q, 3, 7000);

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('payload.type == "order" — only matching messages forwarded', async () => {
      const tag = uid('filter-eq');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        filterExpression: 'payload.type == "order"',
      });

      // Drain any lingering messages
      await drainQueue(q);

      // Publish 5 orders and 5 shipments
      const msgs = [
        ...Array.from({ length: 5 }, (_, i) =>
          ({ payload: JSON.stringify({ type: 'order', id: `ord-${i}`, amount: 100 + i * 10 }) })),
        ...Array.from({ length: 5 }, (_, i) =>
          ({ payload: JSON.stringify({ type: 'shipment', id: `shp-${i}` }) })),
      ];
      await publishBatch(src.ex, msgs);

      // Wait for exactly 5 orders to appear (processor scans all 10, forwards 5)
      await awaitCount(q, 5, 8000);

      // Confirm no extra messages slipped through
      await sleep(1500);
      const extra = await countMessages(q);
      assert.equal(extra, 0, 'No extra messages beyond the 5 orders');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('payload.type != "order" — filters out orders', async () => {
      const tag = uid('filter-ne');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        filterExpression: 'payload.type != "order"',
      });

      await drainQueue(q);

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ type: 'order', id: 'o1' }) },
        { payload: JSON.stringify({ type: 'order', id: 'o2' }) },
        { payload: JSON.stringify({ type: 'shipment', id: 's1' }) },
        { payload: JSON.stringify({ type: 'return', id: 'r1' }) },
      ]);

      await awaitCount(q, 2, 7000);

      await sleep(1500);
      const extra = await countMessages(q);
      assert.equal(extra, 0, 'Only shipment and return forwarded (not orders)');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('payload.amount > 500 — numeric comparison', async () => {
      const tag = uid('filter-gt');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        filterExpression: 'payload.amount > 500',
      });

      await drainQueue(q);

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ type: 'order', amount: 100 }) },
        { payload: JSON.stringify({ type: 'order', amount: 499 }) },
        { payload: JSON.stringify({ type: 'order', amount: 501 }) },
        { payload: JSON.stringify({ type: 'order', amount: 1000 }) },
        { payload: JSON.stringify({ type: 'order', amount: 500 }) },  // not > 500, excluded
      ]);

      await awaitCount(q, 2, 7000);

      await sleep(1500);
      const leftover = await countMessages(q);
      assert.equal(leftover, 0, 'Only amounts 501 and 1000 should be forwarded');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('payload.amount >= 500 — includes boundary value', async () => {
      const tag = uid('filter-gte');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        filterExpression: 'payload.amount >= 500',
      });

      await drainQueue(q);

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ amount: 499 }) },
        { payload: JSON.stringify({ amount: 500 }) },
        { payload: JSON.stringify({ amount: 501 }) },
      ]);

      await awaitCount(q, 2, 7000);

      await sleep(1500);
      const leftover = await countMessages(q);
      assert.equal(leftover, 0, 'Exactly 500 and 501 forwarded');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('payload.tag contains "vip" — string containment', async () => {
      const tag = uid('filter-contains');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        filterExpression: 'payload.tag contains "vip"',
      });

      await drainQueue(q);

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ tag: 'vip-customer' }) },
        { payload: JSON.stringify({ tag: 'regular' }) },
        { payload: JSON.stringify({ tag: 'supervip' }) },
        { payload: JSON.stringify({ tag: 'normal' }) },
      ]);

      await awaitCount(q, 2, 7000);

      await sleep(1500);
      const leftover = await countMessages(q);
      assert.equal(leftover, 0, 'Only vip-customer and supervip forwarded');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('routingKey == "priority" — direct field filter', async () => {
      const tag = uid('filter-rk');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        filterExpression: 'routingKey == "priority"',
      });

      await drainQueue(q);

      await publishBatch(src.ex, [
        { routingKey: 'priority', payload: JSON.stringify({ id: 1 }) },
        { routingKey: 'normal',   payload: JSON.stringify({ id: 2 }) },
        { routingKey: 'priority', payload: JSON.stringify({ id: 3 }) },
        { routingKey: 'bulk',     payload: JSON.stringify({ id: 4 }) },
      ]);

      await awaitCount(q, 2, 7000);

      await sleep(1500);
      const leftover = await countMessages(q);
      assert.equal(leftover, 0, 'Only 2 priority messages forwarded');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });
  });

  // ── 4. Aggregation ────────────────────────────────────────────────────────

  describe('Aggregation', () => {
    it('COUNT — tracks total message count', async () => {
      const tag = uid('agg-count');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        aggregationType: 'COUNT',
      });

      await drainQueue(q);

      const n = 6;
      await publishBatch(src.ex,
        Array.from({ length: n }, (_, i) => ({ payload: JSON.stringify({ seq: i }) }))
      );

      await waitUntil(async () => {
        const states = await getProcessorState(pid);
        if (!states.length) return false;
        const global = states.find(s => s.stateKey === 'global');
        return global && parseState(global).count >= n;
      }, `COUNT reaches ${n}`, 8000);

      const states = await getProcessorState(pid);
      const global = states.find(s => s.stateKey === 'global');
      assert.ok(global, 'global state record should exist');
      assert.ok(parseState(global).count >= n, `count should be >= ${n}`);

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('COUNT with groupBy — separate counts per group', async () => {
      const tag = uid('agg-count-grp');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        aggregationType: 'COUNT',
        aggregationGroupBy: 'payload.region',
      });

      await drainQueue(q);

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ region: 'eu', id: 1 }) },
        { payload: JSON.stringify({ region: 'us', id: 2 }) },
        { payload: JSON.stringify({ region: 'eu', id: 3 }) },
        { payload: JSON.stringify({ region: 'eu', id: 4 }) },
        { payload: JSON.stringify({ region: 'ap', id: 5 }) },
        { payload: JSON.stringify({ region: 'us', id: 6 }) },
      ]);

      await waitUntil(async () => {
        const states = await getProcessorState(pid);
        const byKey = Object.fromEntries(states.map(s => [s.stateKey, parseState(s)]));
        return byKey.eu?.count >= 3 && byKey.us?.count >= 2 && byKey.ap?.count >= 1;
      }, 'all groups fully processed (eu>=3, us>=2, ap>=1)', 8000);

      const states = await getProcessorState(pid);
      const byKey = Object.fromEntries(states.map(s => [s.stateKey, parseState(s)]));

      assert.ok(byKey.eu,       'eu group state should exist');
      assert.ok(byKey.us,       'us group state should exist');
      assert.ok(byKey.ap,       'ap group state should exist');
      assert.ok(byKey.eu.count >= 3, `eu count should be >= 3, got ${byKey.eu?.count}`);
      assert.ok(byKey.us.count >= 2, `us count should be >= 2, got ${byKey.us?.count}`);
      assert.ok(byKey.ap.count >= 1, `ap count should be >= 1, got ${byKey.ap?.count}`);

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('SUM — accumulates a numeric field', async () => {
      const tag = uid('agg-sum');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const amounts = [100, 250, 75, 500, 1000];
      const expectedSum = amounts.reduce((a, b) => a + b, 0); // 1925

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        aggregationType: 'SUM',
        aggregationField: 'payload.amount',
      });

      await drainQueue(q);

      await publishBatch(src.ex,
        amounts.map(amount => ({ payload: JSON.stringify({ amount }) }))
      );

      await waitUntil(async () => {
        const states = await getProcessorState(pid);
        const global = states.find(s => s.stateKey === 'global');
        return global && parseState(global).sum >= expectedSum;
      }, `SUM reaches ${expectedSum}`, 8000);

      const states = await getProcessorState(pid);
      const global = states.find(s => s.stateKey === 'global');
      assert.ok(global, 'global state should exist');
      assert.ok(
        Math.abs(parseState(global).sum - expectedSum) < 0.01,
        `sum should be ${expectedSum}, got ${parseState(global).sum}`,
      );

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('SUM with groupBy — per-group sums', async () => {
      const tag = uid('agg-sum-grp');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        aggregationType: 'SUM',
        aggregationField: 'payload.amount',
        aggregationGroupBy: 'payload.region',
      });

      await drainQueue(q);

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ region: 'eu', amount: 200 }) },
        { payload: JSON.stringify({ region: 'us', amount: 300 }) },
        { payload: JSON.stringify({ region: 'eu', amount: 150 }) },
        { payload: JSON.stringify({ region: 'us', amount: 100 }) },
      ]);
      // eu: 350, us: 400

      await waitUntil(async () => {
        const states = await getProcessorState(pid);
        const byKey = Object.fromEntries(states.map(s => [s.stateKey, parseState(s)]));
        return byKey.eu?.sum >= 350 && byKey.us?.sum >= 400;
      }, 'both groups fully processed (eu>=350, us>=400)', 8000);

      const states = await getProcessorState(pid);
      const byKey = Object.fromEntries(states.map(s => [s.stateKey, parseState(s)]));

      assert.ok(Math.abs(byKey.eu.sum - 350) < 0.01, `eu sum should be 350, got ${byKey.eu?.sum}`);
      assert.ok(Math.abs(byKey.us.sum - 400) < 0.01, `us sum should be 400, got ${byKey.us?.sum}`);

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('LAST — tracks the most recent value of a field', async () => {
      const tag = uid('agg-last');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        aggregationType: 'LAST',
        aggregationField: 'payload.status',
      });

      await drainQueue(q);

      // Publish in a known order; the last one should win
      await publishBatch(src.ex, [
        { payload: JSON.stringify({ status: 'pending' }) },
        { payload: JSON.stringify({ status: 'processing' }) },
        { payload: JSON.stringify({ status: 'shipped' }) },
        { payload: JSON.stringify({ status: 'delivered' }) },
      ]);

      await waitUntil(async () => {
        const states = await getProcessorState(pid);
        const global = states.find(s => s.stateKey === 'global');
        return global && parseState(global).last === 'delivered';
      }, 'LAST tracks "delivered"', 8000);

      const states = await getProcessorState(pid);
      const global = states.find(s => s.stateKey === 'global');
      assert.ok(global, 'global state should exist');
      assert.equal(parseState(global).last, 'delivered');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('LAST with groupBy — per-group latest value', async () => {
      const tag = uid('agg-last-grp');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        aggregationType: 'LAST',
        aggregationField: 'payload.status',
        aggregationGroupBy: 'payload.orderId',
      });

      await drainQueue(q);

      // Two independent orders progressing through states
      await publishBatch(src.ex, [
        { payload: JSON.stringify({ orderId: 'A', status: 'pending' }) },
        { payload: JSON.stringify({ orderId: 'B', status: 'pending' }) },
        { payload: JSON.stringify({ orderId: 'A', status: 'shipped' }) },
        { payload: JSON.stringify({ orderId: 'B', status: 'processing' }) },
        { payload: JSON.stringify({ orderId: 'A', status: 'delivered' }) },
      ]);

      await waitUntil(async () => {
        const states = await getProcessorState(pid);
        const a = states.find(s => s.stateKey === 'A');
        return a && parseState(a).last === 'delivered';
      }, 'order A reaches delivered', 8000);

      const states = await getProcessorState(pid);
      const byKey = Object.fromEntries(states.map(s => [s.stateKey, parseState(s)]));

      assert.equal(byKey.A.last, 'delivered', 'order A last status should be delivered');
      assert.ok(['pending','processing'].includes(byKey.B.last),
        `order B last should be processing (or pending if race), got ${byKey.B?.last}`);

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });
  });

  // ── 5. Multi-processor topologies ─────────────────────────────────────────

  describe('Multi-processor topologies', () => {
    it('fan-out — two processors reading same stream, each with different filter', async () => {
      const tag = uid('fanout');
      const src = await makeSrc(tag);
      const tagA = uid('fanout-a');
      const tagB = uid('fanout-b');
      const tA = await makeTarget(tagA);
      const tB = await makeTarget(tagB);

      const pidA = await createProcessor({
        name: `proc-${tagA}`,
        sourceQueue: src.q,
        targetExchange: tA.ex,
        targetRoutingKey: tA.rk,
        filterExpression: 'payload.channel == "web"',
      });
      const pidB = await createProcessor({
        name: `proc-${tagB}`,
        sourceQueue: src.q,
        targetExchange: tB.ex,
        targetRoutingKey: tB.rk,
        filterExpression: 'payload.channel == "mobile"',
      });

      await drainQueue(tA.q);
      await drainQueue(tB.q);

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ channel: 'web',    id: 1 }) },
        { payload: JSON.stringify({ channel: 'mobile', id: 2 }) },
        { payload: JSON.stringify({ channel: 'web',    id: 3 }) },
        { payload: JSON.stringify({ channel: 'email',  id: 4 }) },
        { payload: JSON.stringify({ channel: 'mobile', id: 5 }) },
        { payload: JSON.stringify({ channel: 'mobile', id: 6 }) },
      ]);

      // web processor should see 2, mobile should see 3
      await Promise.all([awaitCount(tA.q, 2, 8000), awaitCount(tB.q, 3, 8000)]);

      await sleep(1500);
      const leftA = await countMessages(tA.q);
      const leftB = await countMessages(tB.q);
      assert.equal(leftA, 0, 'No extra messages in web queue');
      assert.equal(leftB, 0, 'No extra messages in mobile queue');

      await deleteProcessor(pidA);
      await deleteProcessor(pidB);
      await tearTarget(tA.ex, tA.q);
      await tearTarget(tB.ex, tB.q);
      await tearSrc(src.ex, src.q);
    });

    it('pipeline — processor output into a second STREAM queue fed by another processor', async () => {
      // Stage 1: src (STREAM) → filter "order" → stage2-raw (STREAM)
      // Stage 2: stage2-raw (STREAM) → filter amount>100 → final-q
      const suffix = uid('pipeline');
      const src = await makeSrc(suffix);
      const stage2Queue = `test-stage2-${suffix}`;
      const stage2Ex    = `test-stage2-ex-${suffix}`;
      const { ex: finalEx, q: finalQ, rk: finalRk } = await makeTarget(`final-${suffix}`);

      // Create stage-2 intermediate stream queue and exchange
      await setupReq('/management/queues', 'POST', {
        name: stage2Queue, queueGroup: 'test', durable: true, autoDelete: false, queueType: 'STREAM',
      });
      await setupReq('/management/exchanges', 'POST', { name: stage2Ex, type: 'FANOUT', durable: true });
      await setupReq('/management/bindings', 'POST', { exchangeName: stage2Ex, queueName: stage2Queue });

      // Stage-1 processor: only orders
      const pid1 = await createProcessor({
        name: `proc-stage1-${suffix}`,
        sourceQueue: src.q,
        targetExchange: stage2Ex,
        filterExpression: 'payload.type == "order"',
      });

      // Stage-2 processor: only high-value orders
      const pid2 = await createProcessor({
        name: `proc-stage2-${suffix}`,
        sourceQueue: stage2Queue,
        targetExchange: finalEx,
        targetRoutingKey: finalRk,
        filterExpression: 'payload.amount > 100',
      });

      await drainQueue(finalQ);

      // Publish mixed messages: only orders with amount > 100 should reach finalQ
      await publishBatch(src.ex, [
        { payload: JSON.stringify({ type: 'order',    amount: 50  }) }, // passes stage1, blocked at stage2
        { payload: JSON.stringify({ type: 'order',    amount: 200 }) }, // passes both
        { payload: JSON.stringify({ type: 'shipment', amount: 500 }) }, // blocked at stage1
        { payload: JSON.stringify({ type: 'order',    amount: 300 }) }, // passes both
        { payload: JSON.stringify({ type: 'order',    amount: 75  }) }, // passes stage1, blocked at stage2
      ]);
      // Expected in finalQ: 2 (amounts 200 and 300)

      await awaitCount(finalQ, 2, 10000);

      await sleep(2000);
      const leftover = await countMessages(finalQ);
      assert.equal(leftover, 0, 'Only 2 high-value orders in final queue');

      await deleteProcessor(pid1);
      await deleteProcessor(pid2);
      await req(`/management/exchanges/${encodeURIComponent(stage2Ex)}`, 'DELETE').catch(() => {});
      await req(`/management/queues/${encodeURIComponent(stage2Queue)}`, 'DELETE').catch(() => {});
      await tearTarget(finalEx, finalQ);
      await tearSrc(src.ex, src.q);
    });
  });

  // ── 6. Edge cases ──────────────────────────────────────────────────────────

  describe('Edge cases', () => {
    it('processor on empty stream — no error, count stays 0', async () => {
      const tag = uid('empty-stream');
      const emptyQueue = `test-empty-${tag}`;
      const { ex, q } = await makeTarget(tag);

      await setupReq('/management/queues', 'POST', {
        name: emptyQueue, queueGroup: 'test', durable: true, autoDelete: false, queueType: 'STREAM',
      });

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: emptyQueue,
        targetExchange: ex,
        aggregationType: 'COUNT',
      });

      // Let it run a few ticks — should not crash
      await sleep(3000);

      const p = await req(`/processors/${pid}`);
      assert.equal(p.status, 'RUNNING', 'Processor should still be RUNNING');

      const states = await getProcessorState(pid);
      if (states.length > 0) {
        const global = states.find(s => s.stateKey === 'global');
        if (global) {
          assert.equal(parseState(global).count, 0, 'count should be 0 on empty stream');
        }
      }

      await deleteProcessor(pid);
      await req(`/management/queues/${encodeURIComponent(emptyQueue)}`, 'DELETE').catch(() => {});
      await tearTarget(ex, q);
    });

    it('malformed filter expression — treated as pass-all', async () => {
      const tag = uid('bad-filter');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
        filterExpression: 'this is not a valid expression!!!',
      });

      await drainQueue(q);

      await publishBatch(src.ex, [
        { payload: JSON.stringify({ x: 1 }) },
        { payload: JSON.stringify({ x: 2 }) },
      ]);

      // With invalid filter = pass-all, both messages should be forwarded
      await awaitCount(q, 2, 7000);

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('creating two processors with the same name fails', async () => {
      const tag = uid('dup-name');
      const { ex, q } = await makeTarget(tag);
      const src = await makeSrc(tag);
      const name = `proc-${uid('dup')}`;

      const pid = await createProcessor({ name, sourceQueue: src.q, targetExchange: ex });

      // Second create with same name should fail
      try {
        await req('/processors', 'POST', {
          name,
          sourceQueue: src.q,
          targetExchange: ex,
        });
        // If the server replicated it (idempotent on id but duplicate name),
        // we just verify only one record exists with this name
        const list = await req('/processors');
        const matches = list.filter(p => p.name === name);
        assert.ok(matches.length <= 1, 'Duplicate name should not create multiple processors');
      } catch (err) {
        // A 4xx error is the expected behavior for duplicate names
        assert.ok(
          err.message.includes('4') || err.message.includes('5'),
          'Expected error response for duplicate name',
        );
      }

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });

    it('processor state endpoint returns empty array for new processor', async () => {
      const tag = uid('no-state');
      const { ex, q } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({ name: `proc-${tag}`, sourceQueue: src.q, targetExchange: ex });

      // Immediately check state before any messages processed
      const states = await getProcessorState(pid);
      assert.ok(Array.isArray(states), 'state endpoint should return an array');

      await deleteProcessor(pid);
      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });
  });

  // ── 7. Processor lifecycle after delete ──────────────────────────────────

  describe('Delete stops processing', () => {
    it('deleted processor no longer forwards messages', async () => {
      const tag = uid('del-stop');
      const { ex, q, rk } = await makeTarget(tag);
      const src = await makeSrc(tag);

      const pid = await createProcessor({
        name: `proc-${tag}`,
        sourceQueue: src.q,
        targetExchange: ex,
        targetRoutingKey: rk,
      });

      // Let it run
      await sleep(1500);

      // Drain anything already forwarded
      await drainQueue(q);

      // Delete the processor
      await deleteProcessor(pid);

      // Verify it's gone
      const list = await req('/processors');
      assert.ok(!list.find(p => p.id === pid), 'Processor should be removed from list');

      // Publish new messages
      await publishBatch(src.ex, [
        { payload: JSON.stringify({ id: 'after-delete-1' }) },
        { payload: JSON.stringify({ id: 'after-delete-2' }) },
      ]);

      // Wait and confirm nothing arrives
      await sleep(3000);
      const count = await countMessages(q);
      assert.equal(count, 0, 'No messages should be forwarded after processor deletion');

      await tearTarget(ex, q);
      await tearSrc(src.ex, src.q);
    });
  });
});
