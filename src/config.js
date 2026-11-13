export const QUEUES = Object.freeze({
  main: 'demo.jobs.main', retry: 'demo.jobs.retry', dead: 'demo.jobs.dead'
});
export const EXCHANGES = Object.freeze({
  main: 'demo.jobs.exchange', retry: 'demo.retry.exchange', dead: 'demo.dead.exchange'
});
export const ROUTING = Object.freeze({ main: 'job', retry: 'retry', dead: 'dead' });

export function parsePositiveInt(raw, fallback, min, max, label) {
  const value = raw === undefined ? fallback : Number(raw);
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${label} must be an integer between ${min} and ${max}`);
  }
  return value;
}
