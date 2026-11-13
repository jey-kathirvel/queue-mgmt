import amqp from 'amqplib';
import { QUEUES, EXCHANGES, ROUTING } from './config.js';

export async function connectBroker(url, retryDelayMs = 5000) {
  const connection = await amqp.connect(url);
  const channel = await connection.createConfirmChannel();
  let connected = true;
  connection.on('error', error => console.error('RabbitMQ connection error:', error.message));
  connection.on('close', () => { connected = false; console.error('RabbitMQ connection closed'); });
  channel.on('error', error => console.error('RabbitMQ channel error:', error.message));
  channel.on('close', () => { connected = false; });

  await channel.assertExchange(EXCHANGES.main, 'direct', { durable: true });
  await channel.assertExchange(EXCHANGES.retry, 'direct', { durable: true });
  await channel.assertExchange(EXCHANGES.dead, 'direct', { durable: true });
  await channel.assertQueue(QUEUES.main, { durable: true });
  await channel.assertQueue(QUEUES.retry, {
    durable: true,
    arguments: {
      'x-message-ttl': retryDelayMs,
      'x-dead-letter-exchange': EXCHANGES.main,
      'x-dead-letter-routing-key': ROUTING.main
    }
  });
  await channel.assertQueue(QUEUES.dead, { durable: true });
  await channel.bindQueue(QUEUES.main, EXCHANGES.main, ROUTING.main);
  await channel.bindQueue(QUEUES.retry, EXCHANGES.retry, ROUTING.retry);
  await channel.bindQueue(QUEUES.dead, EXCHANGES.dead, ROUTING.dead);

  function publish(exchange, routingKey, job, headers = {}) {
    if (!connected) return Promise.reject(new Error('RabbitMQ is unavailable'));
    return new Promise((resolve, reject) => {
      try {
        channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(job)), {
          contentType: 'application/json', deliveryMode: 2,
          messageId: job.id, timestamp: Math.floor(Date.now() / 1000),
          headers, mandatory: true
        }, error => error ? reject(error) : resolve());
      } catch (error) { reject(error); }
    });
  }
  return {
    publishJob: job => publish(EXCHANGES.main, ROUTING.main, job, { 'x-retry-count': 0 }),
    publishRetry: (job, attempt) => publish(EXCHANGES.retry, ROUTING.retry, job, { 'x-retry-count': attempt }),
    publishDead: (job, attempts, error) => publish(EXCHANGES.dead, ROUTING.dead,
      { ...job, failure: { attempts, reason: String(error).slice(0, 200), failedAt: new Date().toISOString() } },
      { 'x-retry-count': attempts }),
    queueStats: async () => {
      if (!connected) throw new Error('RabbitMQ is unavailable');
      const entries = await Promise.all(Object.entries(QUEUES).map(async ([name, queue]) => {
        const result = await channel.checkQueue(queue);
        return [name, { name: queue, readyMessages: result.messageCount, consumers: result.consumerCount }];
      }));
      return Object.fromEntries(entries);
    },
    consume: async handler => {
      await channel.prefetch(1);
      return channel.consume(QUEUES.main, handler, { noAck: false });
    },
    ack: msg => channel.ack(msg),
    nack: msg => channel.nack(msg, false, true),
    isConnected: () => connected,
    close: async () => { connected = false; await channel.close().catch(() => {}); await connection.close().catch(() => {}); }
  };
}
