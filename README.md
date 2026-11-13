# RabbitMQ Queue Management API

A practical **RabbitMQ messaging and QA automation portfolio** sample built with Node.js, Express and amqplib. It demonstrates publishing, durable queues, asynchronous workers, manual acknowledgments, delayed retries, a dead-letter queue, monitoring and tests.

> Learning demo only. The worker **simulates** email/report processing and does not deliver emails or generate real reports. No authentication or public-deployment hardening. Use with local test data.

## Stack
- Node.js 20+, Express, AMQP 0-9-1 / amqplib
- RabbitMQ 3.13 Management Docker image
- Node.js built-in test runner and Supertest
- Docker Compose

## Architecture and message flow

```text
HTTP POST /api/jobs
       |
   Confirm publisher
       |
   demo.jobs.exchange -> demo.jobs.main -> Worker
                                          | success: ACK
                                          | error + retries left:
                                          |   confirm publish -> retry + ACK
                                          |              |
                                          |     demo.jobs.retry
                                          |        (5s TTL)
                                          |              |
                                          |     dead-letter back
                                          |     to main exchange
                                          |              |
                                          |         retry worker
                                          |
                                          | error, retries exhausted:
                                          |   confirm publish -> dead + ACK
                                          v
                                    demo.jobs.dead
```

Durable queues and persistent messages support broker restarts. Publisher confirms are used for publishing, including retry/dead-letter republishing. The consumer only ACKs after processing or confirmed republishing. If republishing fails, it NACKs and requeues the original. This is **at-least-once delivery**: duplicate processing is possible and real consumers must be idempotent.

## Quick start
Requires Node.js 20+, npm and Docker Compose.

```bash
git clone https://github.com/jey-kathirvel/queue-mgmt.git
cd queue-mgmt
docker compose up -d
cp .env.example .env
npm install
npm test
npm start
```

RabbitMQ management UI: http://localhost:15672 (local login: `guest` / `guest`).
API: http://localhost:3000.

The Compose configuration binds port 5672 and the management console port 15672 to **127.0.0.1**; never expose the default guest credentials on a public network.

## API endpoints

| Method | URL | Description |
|---|---|---|
| GET | `/health` | RabbitMQ connection status (200 or 503) |
| POST | `/api/jobs` | Publish a demo email/report job (202 / 400 / 503) |
| GET | `/api/queues` | Ready-message and consumer counts for all three queues (200 / 503) |

### Successful example

```bash
curl -X POST http://localhost:3000/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"taskType":"email","payload":{"to":"demo@example.test","subject":"Test notification"}}'
```

A successful response resembles:

```json
{"id":"a-generated-uuid","status":"queued","taskType":"email"}
```

**HTTP 202 means RabbitMQ confirmed the publication, not that the worker completed the task.** Messages include a UUID, creation timestamp and persistent delivery mode.

### Force retries and dead-letter routing

```bash
curl -X POST http://localhost:3000/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"taskType":"report","payload":{"reportId":42},"simulateFailure":true}'
```

The simulated worker fails and retries **three additional times** using the retry queue, with approximately five seconds between attempts. After retries are exhausted, the job is republished to `demo.jobs.dead`. Inspect messages and queues through the RabbitMQ management UI.

### Monitor queues

```bash
curl http://localhost:3000/api/queues
curl http://localhost:3000/health
```

`readyMessages` is the number waiting for delivery; it excludes unacknowledged or completed jobs and is **not** a historical job count.

## RabbitMQ resources

| Resource | Name | Description |
|---|---|---|
| Main direct exchange | `demo.jobs.exchange` | Routes new jobs and returned retries |
| Main durable queue | `demo.jobs.main` | Queue consumed by the worker |
| Retry direct exchange | `demo.retry.exchange` | Routes controlled retries |
| Retry durable queue | `demo.jobs.retry` | 5-second queue TTL then DLX to main |
| Dead direct exchange | `demo.dead.exchange` | Routes terminal failures |
| Dead durable queue | `demo.jobs.dead` | Exhausted/malformed jobs |

The retry queue has `x-message-ttl` and an `x-dead-letter-exchange` pointing back to the main exchange. The worker explicitly publishes terminal failures to the dead exchange. Retry count is carried in the custom `x-retry-count` header; it is not RabbitMQ's automatic `x-death` header.

## What the project demonstrates

1. Durable queues/exchanges, message persistence and publisher confirms.
2. JSON job envelopes and UUID message identifiers.
3. Manual ACK on success and negative ACK/requeue on republish failure.
4. Controlled retry count, delayed retry via queue-level TTL, and DLX routing.
5. Dead-letter handling after retry exhaustion and on malformed messages.
6. Consumer prefetch of one outstanding delivery for predictable processing.
7. Queue monitoring with ready-message and consumer counts.
8. Separation of HTTP transport, RabbitMQ configuration and worker decisions.

## Run automated tests

```bash
npm test
npm run check
```

Worker unit tests exercise successful ACK, retry-count increments, dead-letter routing, malformed envelopes, republish failures, and simulated processing errors. Supertest tests check health, publishing, UUID generation, validation failures, queue status and broker error responses. Tests use a **fake broker**, so they do not prove live RabbitMQ integration.

## Live RabbitMQ manual test plan

1. Start Docker, install dependencies and start the API.
2. Submit a normal job; observe the worker's success log.
3. Submit a job with `simulateFailure:true`; monitor the retry queue and worker logs.
4. After the final retry, inspect the message in `demo.jobs.dead`.
5. Set `RUN_CONSUMER=false`, restart the API, publish a job and confirm `readyMessages` increases.
6. Restore `RUN_CONSUMER=true` and verify the worker consumes the pending job.
7. Stop RabbitMQ and verify health/publish operations reflect a broker outage.
8. Submit malformed input and unsupported `taskType` values to verify HTTP 400.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | 3000 | Express HTTP port |
| `RABBITMQ_URL` | amqp://guest:guest@127.0.0.1:5672 | Local AMQP address |
| `MAX_RETRIES` | 3 | Additional attempts after original |
| `RETRY_DELAY_MS` | 5000 | Retry queue message TTL (ms) |
| `RUN_CONSUMER` | true | Set false for publisher-only demo |

**Changing `RETRY_DELAY_MS`:** RabbitMQ prevents redeclaring an existing queue with a different TTL. Delete/recreate that demo retry queue, or reset the *demo* volume with `docker compose down -v` (deletes all demo queue data).

## Limitations and security

- No actual emailing/reporting, authentication, rate limits, job-status DB or replay endpoint.
- No automatic connection recovery on unexpected broker failure; restart the service.
- At-least-once delivery can yield duplicates; handlers here are simulated, not idempotency-hardened.
- `mandatory: true` is requested for publishing, but the demo does **not** handle `basic.return` unroutable messages; implement it for production.
- No TLS, secrets management, quorum queues, monitoring/alerts, distributed tracing, or operational controls.

## Portfolio / resume line

After running and validating the project:

> Developed a RabbitMQ-based Queue Management REST API using Node.js, Express and AMQP, demonstrating durable messages, asynchronous consumers, acknowledgments, delayed retries, dead-letter queues, publisher confirms and automated API/worker tests.
