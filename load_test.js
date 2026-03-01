const BASE_URL = process.env.BASE_URL || 'http://localhost:8080/api';
const PUBLISH_DELAY = parseInt(process.env.PUBLISH_DELAY || '1000'); // ms between publishes
const POLL_DELAY   = parseInt(process.env.POLL_DELAY   || '500');    // ms between polls

// ─── topology ────────────────────────────────────────────────────────────────
const DIRECT_EXCHANGE = 'direct-exchange';
const FANOUT_EXCHANGE  = 'fanout-exchange';
const DIRECT_QUEUE    = 'q-direct';
const FANOUT_QUEUE    = 'q-fanout';
const ROUTING_KEY     = 'order';

// ─── stats ───────────────────────────────────────────────────────────────────
const stats = {
  published: { direct: 0, fanout: 0 },
  consumed:  { [DIRECT_QUEUE]: 0, [FANOUT_QUEUE]: 0 },
  acked:     { [DIRECT_QUEUE]: 0, [FANOUT_QUEUE]: 0 },
  errors:    0,
};

// ─── helpers ─────────────────────────────────────────────────────────────────
async function apiRequest(path, method = 'GET', body = null) {
  const options = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) options.body = JSON.stringify(body);
  try {
    const res = await fetch(`${BASE_URL}${path}`, options);
    if (!res.ok && res.status !== 204 && res.status !== 201) {
      const text = await res.text();
      console.error(`[ERROR] ${method} ${path} → ${res.status} ${text}`);
      stats.errors++;
      return null;
    }
    if (res.status === 204 || res.status === 201) return true;
    const text = await res.text();
    return text ? JSON.parse(text) : true;
  } catch (err) {
    if (err.code === 'ECONNREFUSED' || err.message?.includes('fetch failed')) {
      throw new Error(`Connection refused to ${BASE_URL}. Is simpleMQ running?`);
    }
    throw err;
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── setup ───────────────────────────────────────────────────────────────────
async function setup() {
  console.log('\n=== Setup ===');

  await apiRequest('/management/exchanges', 'POST', { name: DIRECT_EXCHANGE, type: 'DIRECT', durable: true });
  console.log(`  exchange: ${DIRECT_EXCHANGE} (DIRECT)`);

  await apiRequest('/management/exchanges', 'POST', { name: FANOUT_EXCHANGE, type: 'FANOUT', durable: true });
  console.log(`  exchange: ${FANOUT_EXCHANGE} (FANOUT)`);

  await apiRequest('/management/queues', 'POST', { name: DIRECT_QUEUE, durable: true, autoDelete: false });
  console.log(`  queue:    ${DIRECT_QUEUE}`);

  await apiRequest('/management/queues', 'POST', { name: FANOUT_QUEUE, durable: true, autoDelete: false });
  console.log(`  queue:    ${FANOUT_QUEUE}`);

  await apiRequest('/management/bindings', 'POST', { exchangeName: DIRECT_EXCHANGE, queueName: DIRECT_QUEUE, routingKey: ROUTING_KEY });
  console.log(`  bind:     ${DIRECT_EXCHANGE} --[${ROUTING_KEY}]--> ${DIRECT_QUEUE}`);

  await apiRequest('/management/bindings', 'POST', { exchangeName: FANOUT_EXCHANGE, queueName: FANOUT_QUEUE });
  console.log(`  bind:     ${FANOUT_EXCHANGE} --> ${FANOUT_QUEUE}`);

  console.log('=== Setup complete ===\n');
}

// ─── producer ────────────────────────────────────────────────────────────────
async function producer() {
  let seq = 0;
  while (true) {
    seq++;
    const payload = `msg-${seq} @ ${new Date().toISOString()}`;

    const d = await apiRequest(`/publish/${DIRECT_EXCHANGE}`, 'POST', { routingKey: ROUTING_KEY, payload });
    if (d) {
      stats.published.direct++;
      process.stdout.write(`[PROD] direct #${seq}\r`);
    }

    const f = await apiRequest(`/publish/${FANOUT_EXCHANGE}`, 'POST', { payload });
    if (f) {
      stats.published.fanout++;
      process.stdout.write(`[PROD] fanout #${seq}\r`);
    }

    await sleep(PUBLISH_DELAY);
  }
}

// ─── consumer ────────────────────────────────────────────────────────────────
async function consumer(queue) {
  while (true) {
    const msg = await apiRequest(`/poll/${queue}`);
    if (msg && typeof msg === 'object' && msg.id) {
      stats.consumed[queue]++;
      console.log(`[CONS:${queue}] #${stats.consumed[queue]} → "${msg.payload}"`);
      const ok = await apiRequest(`/poll/ack/${msg.id}`, 'POST');
      if (ok) stats.acked[queue]++;
    }
    await sleep(POLL_DELAY);
  }
}

// ─── stats printer ───────────────────────────────────────────────────────────
function startStatsPrinter() {
  setInterval(() => {
    console.log(
      `\n[STATS] published: direct=${stats.published.direct} fanout=${stats.published.fanout}` +
      ` | consumed: ${DIRECT_QUEUE}=${stats.consumed[DIRECT_QUEUE]} ${FANOUT_QUEUE}=${stats.consumed[FANOUT_QUEUE]}` +
      ` | errors: ${stats.errors}`
    );
  }, 5000);
}

// ─── server probe ────────────────────────────────────────────────────────────
async function waitForServer(retries = 10, delay = 2000) {
  process.stdout.write(`Connecting to ${BASE_URL} `);
  for (let i = 0; i < retries; i++) {
    try {
      await fetch(`${BASE_URL}/management/summary`);
      console.log('✓');
      return;
    } catch {
      process.stdout.write('.');
      await sleep(delay);
    }
  }
  throw new Error(`Could not connect to ${BASE_URL} after ${retries} attempts.`);
}

// ─── main ────────────────────────────────────────────────────────────────────
async function main() {
  await waitForServer();
  await setup();

  startStatsPrinter();

  console.log(`Publishing every ${PUBLISH_DELAY}ms, polling every ${POLL_DELAY}ms\n`);

  await Promise.all([
    producer(),
    consumer(DIRECT_QUEUE),
    consumer(FANOUT_QUEUE),
  ]);
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
