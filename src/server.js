import 'dotenv/config';
import { connectBroker } from './broker.js';
import { createApp } from './app.js';
import { startWorker } from './worker.js';
import { parsePositiveInt } from './config.js';

let broker;
try {
  const port = parsePositiveInt(process.env.PORT, 3000, 1, 65535, 'PORT');
  const maxRetries = parsePositiveInt(process.env.MAX_RETRIES, 3, 0, 20, 'MAX_RETRIES');
  const retryDelay = parsePositiveInt(process.env.RETRY_DELAY_MS, 5000, 1000, 3600000, 'RETRY_DELAY_MS');
  broker = await connectBroker(process.env.RABBITMQ_URL || 'amqp://guest:guest@127.0.0.1:5672', retryDelay);
  if (process.env.RUN_CONSUMER !== 'false') await startWorker(broker, maxRetries);
  const server = createApp(broker).listen(port, () => console.log(`RabbitMQ Queue API on http://localhost:${port}`));
  let closing = false;
  const shutdown = () => {
    if (closing) return;
    closing = true;
    server.close(async () => { await broker.close(); process.exit(0); });
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
} catch (error) {
  console.error('Startup failed:', error.message);
  if (broker) await broker.close().catch(() => {});
  process.exit(1);
}
