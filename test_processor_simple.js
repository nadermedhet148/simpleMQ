#!/usr/bin/env node
/**
 * Simple Stream Processor Smoke Test
 * Tests: create, list, forward messages, pause/resume, delete
 *
 * Usage:
 *   node test_processor_simple.js
 *   BASE_URL=http://localhost:8080 node test_processor_simple.js
 */

const BASE = process.env.BASE_URL || 'http://localhost:8080';
const API  = `${BASE}/api`;

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function req(path, method = 'GET', body = null) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body !== null) opts.body = JSON.stringify(body);
  const res = await fetch(`${API}${path}`, opts);
  const text = await res.text();
  if (!res.ok && res.status !== 409) {
    throw new Error(`${method} ${path} → ${res.status}: ${text}`);
  }
  return text ? JSON.parse(text) : null;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

let passed = 0, failed = 0;

async function test(name, fn) {
  try {
    await fn();
    console.log(`  ✓  ${name}`);
    passed++;
  } catch (err) {
    console.log(`  ✗  ${name}`);
    console.log(`     ${err.message}`);
    failed++;
  }
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

// ─── Poll queue until at least `n` messages arrive ───────────────────────────

async function waitForMessages(queue, n, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  const msgs = [];
  while (msgs.length < n) {
    if (Date.now() > deadline) throw new Error(`Timeout: expected ${n} msgs in ${queue}, got ${msgs.length}`);
    const res = await fetch(`${API}/poll/${encodeURIComponent(queue)}`);
    if (res.status === 204) { await sleep(300); continue; }
    if (res.ok) {
      const m = await res.json();
      if (m && m.id) {
        await req(`/poll/ack/${m.id}`, 'POST').catch(() => {});
        msgs.push(m);
      }
    }
  }
  return msgs;
}

// ─── Setup helpers ────────────────────────────────────────────────────────────

async function createStreamQueue(name) {
  await req('/management/queues', 'POST', { name, queueGroup: 'test', durable: true, autoDelete: false, queueType: 'STREAM' });
}

async function createStandardQueue(name) {
  await req('/management/queues', 'POST', { name, queueGroup: 'test', durable: true, autoDelete: false });
}

async function createExchange(name, type = 'DIRECT') {
  await req('/management/exchanges', 'POST', { name, type, durable: true });
}

async function bind(exchangeName, queueName, routingKey = '') {
  await req('/management/bindings', 'POST', { exchangeName, queueName, routingKey });
}

async function publish(exchange, routingKey, payload) {
  // payload must be a JSON string per the PublishResource API
  const payloadStr = typeof payload === 'string' ? payload : JSON.stringify(payload);
  await req(`/publish/${encodeURIComponent(exchange)}`, 'POST', { routingKey, payload: payloadStr });
}

async function del(path) {
  await req(path, 'DELETE').catch(() => {});
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  // Wait for server
  process.stdout.write(`Waiting for server at ${BASE}`);
  for (let i = 0; i < 20; i++) {
    try {
      const r = await fetch(`${API}/management/summary`);
      if (r.ok) {
        const b = await r.json();
        if (b.leaderId) { console.log(` ✓ (leader: ${b.leaderId})\n`); break; }
      }
    } catch {}
    process.stdout.write('.');
    await sleep(2000);
    if (i === 19) { console.log('\nServer not ready'); process.exit(1); }
  }

  const ts = Date.now();

  // Queue/exchange names
  const srcQ  = `t-src-${ts}`;
  const srcEx = `t-src-ex-${ts}`;
  const dstQ  = `t-dst-${ts}`;
  const dstEx = `t-dst-ex-${ts}`;

  // ── Setup ──────────────────────────────────────────────────────────────────
  await createStreamQueue(srcQ);
  await createExchange(srcEx, 'FANOUT');
  await bind(srcEx, srcQ);

  await createStandardQueue(dstQ);
  await createExchange(dstEx, 'DIRECT');
  await bind(dstEx, dstQ, 'out');

  console.log('Stream Processor Tests\n');

  let pid = null;

  // ── Test 1: Create processor ──────────────────────────────────────────────
  await test('create processor', async () => {
    const p = await req('/processors', 'POST', {
      name: `proc-${ts}`,
      sourceQueue: srcQ,
      targetExchange: dstEx,
      targetRoutingKey: 'out',
    });
    assert(p && p.id, `expected {id}, got: ${JSON.stringify(p)}`);
    assert(p.status === 'RUNNING', `expected RUNNING, got ${p.status}`);
    pid = p.id;
  });

  if (!pid) { console.log('\nProcessor creation failed — skipping remaining tests'); }

  // ── Test 2: List shows the processor ─────────────────────────────────────
  await test('processor appears in list', async () => {
    const list = await req('/processors');
    assert(Array.isArray(list), 'expected array');
    const found = list.find(p => p.id === pid);
    assert(found, `pid ${pid} not in list`);
  });

  // ── Test 3: Fetch by id ───────────────────────────────────────────────────
  await test('fetch processor by id', async () => {
    const p = await req(`/processors/${pid}`);
    assert(p.id === pid, `id mismatch`);
    assert(p.sourceQueue === srcQ, `sourceQueue mismatch`);
  });

  // ── Test 4: Message forwarding ────────────────────────────────────────────
  await test('forwards messages to target queue', async () => {
    await publish(srcEx, '', { type: 'order', amount: 100 });
    await publish(srcEx, '', { type: 'event', amount: 200 });
    const msgs = await waitForMessages(dstQ, 2, 10000);
    assert(msgs.length === 2, `expected 2, got ${msgs.length}`);
  });

  // ── Test 5: Pause stops forwarding ────────────────────────────────────────
  await test('pause stops forwarding', async () => {
    await req(`/processors/${pid}/pause`, 'POST');
    await sleep(500);
    const p = await req(`/processors/${pid}`);
    assert(p.status === 'PAUSED', `expected PAUSED, got ${p.status}`);

    // Publish while paused — should NOT appear in dst
    await publish(srcEx, '', { type: 'paused-msg' });
    await sleep(2500); // wait 2.5s — if forwarded it would arrive quickly

    const res = await fetch(`${API}/poll/${encodeURIComponent(dstQ)}`);
    assert(res.status === 204, `expected 204 (empty), got ${res.status} — message leaked through pause`);
  });

  // ── Test 6: Resume forwards new messages ──────────────────────────────────
  await test('resume forwards messages again', async () => {
    await req(`/processors/${pid}/resume`, 'POST');
    // The paused message should now be forwarded
    const msgs = await waitForMessages(dstQ, 1, 8000);
    assert(msgs.length >= 1, `expected >=1 after resume, got ${msgs.length}`);
  });

  // ── Test 7: Delete ────────────────────────────────────────────────────────
  await test('delete removes processor', async () => {
    await req(`/processors/${pid}`, 'DELETE');
    const list = await req('/processors');
    assert(!list.find(p => p.id === pid), 'processor still in list after delete');
    pid = null;
  });

  // ── Cleanup ───────────────────────────────────────────────────────────────
  if (pid) await del(`/processors/${pid}`);
  await del(`/management/exchanges/${encodeURIComponent(srcEx)}`);
  await del(`/management/queues/${encodeURIComponent(srcQ)}`);
  await del(`/management/exchanges/${encodeURIComponent(dstEx)}`);
  await del(`/management/queues/${encodeURIComponent(dstQ)}`);

  console.log(`\n${passed + failed} tests: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => { console.error(err); process.exit(1); });
