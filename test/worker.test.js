import test from 'node:test';
import assert from 'node:assert/strict';
import { handleDelivery, processJob } from '../src/worker.js';

const silent = { info() {}, warn() {}, error() {} };
const message = (job, retry = 0) => ({
  content: Buffer.from(JSON.stringify(job)),
  properties: { headers: { 'x-retry-count': retry }, messageId: job.id }
});
function fakeBroker() {
  const calls = [];
  return {
    calls,
    ack: () => calls.push(['ack']),
    nack: () => calls.push(['nack']),
    publishRetry: async (_job, count) => { calls.push(['retry', count]); },
    publishDead: async (_job, count) => { calls.push(['dead', count]); }
  };
}
const job = { id: 'job-123', taskType: 'email', payload: { to: 'demo@example.test' }, simulateFailure: false };

test('successful job is acknowledged without retry', async () => {
  const broker = fakeBroker();
  assert.equal(await handleDelivery(message(job), broker, { log: silent }), 'completed');
  assert.deepEqual(broker.calls, [['ack']]);
});
test('failed job is retried with incremented header and then acknowledged', async () => {
  const broker = fakeBroker();
  const fail = { ...job, simulateFailure: true };
  assert.equal(await handleDelivery(message(fail, 1), broker, { maxRetries: 3, log: silent }), 'retry');
  assert.deepEqual(broker.calls, [['retry', 2], ['ack']]);
});
test('exhausted retries route to DLQ', async () => {
  const broker = fakeBroker();
  assert.equal(await handleDelivery(message({ ...job, simulateFailure: true }, 3), broker, { maxRetries: 3, log: silent }), 'dead');
  assert.deepEqual(broker.calls, [['dead', 3], ['ack']]);
});
test('invalid message goes directly to DLQ', async () => {
  const broker = fakeBroker();
  const invalid = { content: Buffer.from('not-json'), properties: { headers: {}, messageId: 'bad' } };
  assert.equal(await handleDelivery(invalid, broker, { log: silent }), 'dead');
  assert.deepEqual(broker.calls, [['dead', 0], ['ack']]);
});
test('failed retry publication requeues original without ack', async () => {
  const broker = fakeBroker();
  broker.publishRetry = async () => { throw new Error('broker error'); };
  assert.equal(await handleDelivery(message({ ...job, simulateFailure: true }), broker, { log: silent }), 'requeued');
  assert.deepEqual(broker.calls, [['nack']]);
});
test('simulated handler supports email/report, rejects unsupported jobs', async () => {
  assert.deepEqual(await processJob(job), { processed: true, taskType: 'email' });
  await assert.rejects(processJob({ ...job, taskType: 'unknown' }), /Unsupported/);
  await assert.rejects(processJob({ ...job, simulateFailure: true }), /Simulated/);
});
