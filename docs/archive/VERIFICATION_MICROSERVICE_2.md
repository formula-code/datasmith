# Variant 2: Async API + Worker Queue

## Intent
Split verification into a lightweight API and a worker pool. The API accepts `(repo, sha)` jobs quickly, and workers execute heavy verification asynchronously.

This is suited for batch runs where many repo/sha pairs are verified concurrently.

## Proposed Repository Shape

```text
docs/
  VERIFICATION_MICROSERVICE_2.md
src/datasmith/verification/
  api/
    app.py
    routes.py
  workers/
    worker.py
    executor.py
  queue.py
  store.py
  models.py
scratch/scripts/
  submit_verification_jobs.py
  run_verification_worker.py
```

## Service Topology

```mermaid
flowchart TB
  caller[Pipeline / CLI]
  api[Verification API]
  queue[(Redis/SQS queue)]
  worker[Verification workers]
  docker[Docker build + profile/test]
  store[(Result store)]
  poll[GET /v1/jobs/{id}]

  caller -->|POST /v1/jobs| api --> queue --> worker --> docker --> store
  caller --> poll --> api --> store
```

## Verification Flow

1. Caller submits verification request and gets back `job_id`.
2. API validates input and enqueues a deduplicated job key.
3. Worker pulls job, runs build + validation, and writes full result.
4. Caller polls status/result or streams updates from the API.
5. Repeated `(repo, sha, config_hash)` requests can reuse stored outcomes.

## Why This Variant

- Decouples user-facing latency from long-running Docker work.
- Enables horizontal worker scaling for throughput spikes.
- Supports retries, dead-letter queues, and failure isolation.

## Trade-offs

- More operational components (queue, worker lifecycle, store).
- Eventually consistent API behavior (`submitted` vs `completed`).
- Needs careful idempotency rules to avoid duplicate expensive builds.
