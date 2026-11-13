/** Simulated handler. No real email is sent and no external report is generated. */
export async function processJob(job) {
  if (job.simulateFailure === true) throw new Error('Simulated processing failure');
  if (!['email', 'report'].includes(job.taskType)) throw new Error('Unsupported task type');
  return { processed: true, taskType: job.taskType };
}

/** One delivery; can be unit-tested with fake broker methods. */
export async function handleDelivery(message, broker, { maxRetries = 3, handler = processJob, log = console } = {}) {
  if (!message) return;
  let job;
  let retryCount = 0;
  try {
    job = JSON.parse(message.content.toString('utf8'));
    if (!job || typeof job !== 'object' || typeof job.id !== 'string') throw new Error('Invalid message envelope');
    retryCount = Number(message.properties?.headers?.['x-retry-count'] ?? 0);
    if (!Number.isSafeInteger(retryCount) || retryCount < 0) throw new Error('Invalid retry header');
    await handler(job);
    broker.ack(message);
    log.info(`Job ${job.id} processed successfully`);
    return 'completed';
  } catch (error) {
    try {
      const safeJob = job && typeof job.id === 'string' ? job : {
        id: message.properties?.messageId || 'unidentified',
        rawMessage: message.content.toString('utf8').slice(0, 1000)
      };
      if (job && retryCount < maxRetries) {
        await broker.publishRetry(safeJob, retryCount + 1);
        broker.ack(message);
        log.warn(`Job ${safeJob.id} queued for retry ${retryCount + 1}/${maxRetries}: ${error.message}`);
        return 'retry';
      }
      await broker.publishDead(safeJob, retryCount, error.message);
      broker.ack(message);
      log.error(`Job ${safeJob.id} moved to DLQ: ${error.message}`);
      return 'dead';
    } catch (publishError) {
      log.error(`Failed to republish message: ${publishError.message}`);
      broker.nack(message);
      return 'requeued';
    }
  }
}

export async function startWorker(broker, maxRetries = 3) {
  await broker.consume(message => {
    void handleDelivery(message, broker, { maxRetries }).catch(error => {
      console.error('Unexpected worker error:', error);
      if (message) broker.nack(message);
    });
  });
  console.log('RabbitMQ worker consuming main queue (manual acknowledgments enabled)');
}
