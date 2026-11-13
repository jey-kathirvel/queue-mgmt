import express from 'express';
import { randomUUID } from 'node:crypto';

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
export function createApp(broker) {
  const app = express();
  app.use(express.json({ limit: '16kb' }));
  const wrap = fn => (req, res, next) => Promise.resolve(fn(req, res)).catch(next);

  app.get('/health', (_req, res) => {
    const connected = broker.isConnected();
    res.status(connected ? 200 : 503).json({ status: connected ? 'ok' : 'unavailable', rabbitmq: connected ? 'connected' : 'disconnected' });
  });

  app.post('/api/jobs', wrap(async (req, res) => {
    const input = req.body;
    if (!isRecord(input) || !['email', 'report'].includes(input.taskType) ||
      !isRecord(input.payload) ||
      (input.simulateFailure !== undefined && typeof input.simulateFailure !== 'boolean') ||
      Object.keys(input).some(k => !['taskType', 'payload', 'simulateFailure'].includes(k))) {
      return res.status(400).json({ error: 'Provide taskType (email or report), payload object and optional simulateFailure boolean' });
    }
    const job = { id: randomUUID(), taskType: input.taskType, payload: input.payload,
      simulateFailure: input.simulateFailure ?? false, createdAt: new Date().toISOString() };
    await broker.publishJob(job);
    res.status(202).json({ id: job.id, status: 'queued', taskType: job.taskType });
  }));

  app.get('/api/queues', wrap(async (_req, res) => {
    res.json({ queues: await broker.queueStats(), note: 'readyMessages does not include unacknowledged or completed jobs' });
  }));

  app.use((error, _req, res, _next) => {
    if (error instanceof SyntaxError && 'body' in error) return res.status(400).json({ error: 'Malformed JSON' });
    console.error('API error:', error.message);
    res.status(503).json({ error: 'RabbitMQ unavailable' });
  });
  return app;
}
