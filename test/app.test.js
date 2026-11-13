import test from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import { createApp } from '../src/app.js';

function fakeBroker() {
  const published = [];
  return {
    published,
    isConnected: () => true,
    publishJob: async job => { published.push(job); },
    queueStats: async () => ({ main: { name: 'demo.jobs.main', readyMessages: 1, consumers: 1 } })
  };
}
test('health reports connected broker', async () => {
  const res = await request(createApp(fakeBroker())).get('/health');
  assert.equal(res.status, 200);
  assert.equal(res.body.rabbitmq, 'connected');
});
test('job publish returns 202 with UUID and preserves payload', async () => {
  const broker = fakeBroker();
  const res = await request(createApp(broker)).post('/api/jobs').send({ taskType: 'email', payload: { to: 'test@example.test' } });
  assert.equal(res.status, 202);
  assert.match(res.body.id, /^[0-9a-f-]{36}$/);
  assert.equal(broker.published[0].payload.to, 'test@example.test');
});
test('validation returns 400 for missing payload, wrong type or unknown field', async () => {
  const app = createApp(fakeBroker());
  assert.equal((await request(app).post('/api/jobs').send({ taskType: 'email' })).status, 400);
  assert.equal((await request(app).post('/api/jobs').send({ taskType: 'unknown', payload: {} })).status, 400);
  assert.equal((await request(app).post('/api/jobs').send({ taskType: 'email', payload: [], unknown: true })).status, 400);
});
test('queue statistics returns ready messages and consumers', async () => {
  const res = await request(createApp(fakeBroker())).get('/api/queues');
  assert.equal(res.status, 200);
  assert.equal(res.body.queues.main.consumers, 1);
});
test('broker publishing failure returns 503', async () => {
  const broker = fakeBroker();
  broker.publishJob = async () => { throw new Error('disconnected'); };
  const res = await request(createApp(broker)).post('/api/jobs').send({ taskType: 'report', payload: {} });
  assert.equal(res.status, 503);
});
