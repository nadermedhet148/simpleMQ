const BASE_URL = process.env.BASE_URL || 'http://localhost:8080/api';
const MODE = process.env.MODE || 'all'; // all, producer, consumer, setup
const DELAY = parseInt(process.env.DELAY || '1000');

async function apiRequest(path, method = 'GET', body = null) {
  const options = {
    method,
    headers: { 'Content-Type': 'application/json' },
  };
  if (body) {
    options.body = JSON.stringify(body);
  }
  try {
    const response = await fetch(`${BASE_URL}${path}`, options);
    if (!response.ok && response.status !== 204 && response.status !== 201) {
      const errorText = await response.text();
      console.error(`Error on ${method} ${path}: ${response.status} ${errorText}`);
      return null;
    }
    if (response.status === 204 || response.status === 201) return true;
    
    const text = await response.text();
    if (!text) return true;
    return JSON.parse(text);
  } catch (error) {
    if (error.code === 'ECONNREFUSED' || error.message.includes('fetch failed')) {
      throw new Error(`Connection refused to ${BASE_URL}. Is the simpleMQ server running? Use BASE_URL=http://localhost:8081/api if using docker-compose.`);
    }
    throw error;
  }
}

async function waitForServer(retries = 10, delay = 2000) {
  console.log(`Checking connection to ${BASE_URL}...`);
  for (let i = 0; i < retries; i++) {
    try {
      await fetch(`${BASE_URL}/management/summary`);
      console.log(`Connected to simpleMQ at ${BASE_URL}`);
      return true;
    } catch (err) {
      process.stdout.write(`.`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  console.log('\n');
  throw new Error(`Could not connect to ${BASE_URL} after ${retries} attempts. Is the simpleMQ server running? Use BASE_URL=http://localhost:8081/api if using docker-compose.`);
}

async function setup() {
  console.log('--- Setting up simpleMQ ---');

  // Create Direct Exchange
  await apiRequest('/management/exchanges', 'POST', { name: 'direct-exchange', type: 'DIRECT', durable: true });
  console.log('Created direct-exchange');

  // Create Fanout Exchange
  await apiRequest('/management/exchanges', 'POST', { name: 'fanout-exchange', type: 'FANOUT', durable: true });
  console.log('Created fanout-exchange');

  // Create Queues
  await apiRequest('/management/queues', 'POST', { name: 'q-direct-1', queueGroup: 'group1', durable: true, autoDelete: false });
  await apiRequest('/management/queues', 'POST', { name: 'q-direct-2', queueGroup: 'group1', durable: true, autoDelete: false });
  await apiRequest('/management/queues', 'POST', { name: 'q-fanout-1', queueGroup: 'group2', durable: true, autoDelete: false });
  await apiRequest('/management/queues', 'POST', { name: 'q-fanout-2', queueGroup: 'group2', durable: true, autoDelete: false });
  console.log('Created queues');

  // Bind Queues
  await apiRequest('/management/bindings', 'POST', { exchangeName: 'direct-exchange', queueName: 'q-direct-1', routingKey: 'key1' });
  await apiRequest('/management/bindings', 'POST', { exchangeName: 'direct-exchange', queueName: 'q-direct-2', routingKey: 'key2' });
  await apiRequest('/management/bindings', 'POST', { exchangeName: 'fanout-exchange', queueName: 'q-fanout-1' });
  await apiRequest('/management/bindings', 'POST', { exchangeName: 'fanout-exchange', queueName: 'q-fanout-2' });
  console.log('Bound queues');
  
  console.log('--- Setup complete ---');
}

async function produce() {
  let counter = 0;
  while (true) {
    counter++;
    const payload = `Message #${counter} sent at ${new Date().toISOString()}`;
    
    // Publish to direct exchange
    const routingKey = counter % 2 === 0 ? 'key1' : 'key2';
    await apiRequest(`/publish/direct-exchange`, 'POST', { routingKey, payload });
    console.log(`[Producer] Published to direct-exchange with key ${routingKey}`);

    // Publish to fanout exchange
    await apiRequest(`/publish/fanout-exchange`, 'POST', { payload });
    console.log(`[Producer] Published to fanout-exchange`);

    await new Promise(resolve => setTimeout(resolve, DELAY));
  }
}

async function consume(queueName) {
  while (true) {
    const msg = await apiRequest(`/poll/${queueName}`);
    if (msg && typeof msg === 'object') {
      console.log(`[Consumer ${queueName}] Received: ${msg.payload}`);
      await apiRequest(`/poll/ack/${msg.id}`, 'POST');
      console.log(`[Consumer ${queueName}] Acked: ${msg.id}`);
    } else {
      // console.log(`[Consumer ${queueName}] No messages`);
    }
    await new Promise(resolve => setTimeout(resolve, DELAY / 2));
  }
}

async function main() {
  await waitForServer();
  
  if (MODE === 'setup' || MODE === 'all') {
    await setup();
  }

  const tasks = [];
  if (MODE === 'producer' || MODE === 'all') {
    tasks.push(produce());
  }
  if (MODE === 'consumer' || MODE === 'all') {
    tasks.push(consume('q-direct-1'));
    tasks.push(consume('q-direct-2'));
    tasks.push(consume('q-fanout-1'));
    tasks.push(consume('q-fanout-2'));
  }

  if (tasks.length > 0) {
    await Promise.all(tasks);
  }
}

main().catch(err => {
  console.error('Fatal error in main:', err);
});
