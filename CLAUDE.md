# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

A self-hosted API uptime and latency monitor. Users register HTTP endpoints; APScheduler probes each one every 60s, results are published to Kafka, a separate consumer container detects incidents, and a second scheduled job emails periodic digests.

FastAPI · PostgreSQL · Kafka (aiokafka) · Redis · APScheduler · SQLModel/SQLAlchemy async · Docker Compose.

Full architecture and flow diagrams: [architecture.md](architecture.md). Active improvement plan: [IMPROVEMENT_PLAN.md](IMPROVEMENT_PLAN.md).

## Two processes, not one

| Process | Entry point | Responsibility |
|---|---|---|
| `monitoring_api` | [app/main.py](app/main.py) | REST API **plus both** APScheduler jobs (health checks 60s, alert digests 30 min) |
| `alert_consumer` | [app/services/healthConsumer.py](app/services/healthConsumer.py) | Kafka consumer → incident detection |

The consumer runs as `python -m app.services.healthConsumer` and does **not** import `api_client.py` or `producer.py`. Keep it that way — module-level side effects in those files would land in the consumer process.

## Layering — follow it

```
routers → services → repositories → SQLModel/SQLAlchemy → Postgres
```

- Repositories **never** open their own session. The session is constructed in the router via `Depends(get_db_session)` and threaded down. Background jobs use `async with db.pg_session_factory() as session:` (see [monitoring.py:21](app/services/monitoring.py#L21)) — that is the established pattern.
- Repository methods return `dict(row)` from `result.mappings()`, **not** ORM objects. Use subscript access (`row["field"]`), never attribute access. Several live bugs come from getting this wrong.
- Tenant scoping is enforced **in the SQL predicate** (`owner_user_id == self.user_uid`), not in Python. Preserve this on every user-facing query.

## Commands

```bash
docker compose up --build                                          # full stack
docker compose exec -T fastapi sh -c "cd /app && alembic upgrade head"   # migrations
docker compose logs -f fastapi                                     # API + scheduler
docker compose logs -f consumer                                    # incident detection

pip install pytest && pytest tests/ -v -s                          # tests (need a live stack)
```

Tests are **black-box HTTP** against `API_BASE_URL` — they import nothing from `app/`, so internal refactors are invisible to them. They also create real rows and never clean up. Point at a throwaway DB.

Schema is Alembic-managed; the app does **not** create tables at startup.

## Event topology

One topic, two partitions, two consumer groups:

```
producer (health-check loop) ──► api-monitoring-results (2 partitions, key=endpoint_id)
                                   ├─ monitoring_consumer_group  (2 consumers, 1 partition each)
                                   │    healthConsumer.py → incidents table
                                   └─ metrics-rollup-group       (1 consumer)
                                        metricsConsumer.py → endpoint_metrics_hourly
```

Both groups read every message with independent offsets — rollup lag cannot affect alerting. Keying by `endpoint_id` pins each endpoint to one partition, which is what makes two detector consumers safe on the "last 3 checks" window.

## Invariants — do not break these

- **The health-check loop writes to Postgres BEFORE publishing to Kafka.** A failed publish then delays detection by one cycle instead of producing a wrong verdict. Never reorder.
- **`get_consumer_service` and `get_last_three_records` are system-context queries** and intentionally carry no `owner_user_id` filter. They are reachable only from the consumers. Adding a tenant filter there silently returns nothing (it renders as `IS NULL` against a `NOT NULL` column) and kills the incident pipeline.
- **`enable_auto_commit=False`; commit only after successful processing.** At-least-once is safe because incident detection recomputes from the DB window and is idempotent.
- **Never set `value_deserializer` on the Kafka consumer.** Decoding must happen in our code (`KafkaConsumerClient.deserialize`); inside aiokafka's fetcher a malformed payload cannot be caught or skipped, creating an unbreakable poison loop.
- **`keepalive_expiry` on the shared httpx client must stay below the 60s check interval.** A pooled socket the server already closed raises on reuse and httpx does not retry, which would register as a false unhealthy reading.
- **Redis is optional and every call fails open.** Use `redis_call()` from `infrastructure/redis/client.py`; never let a cache failure break a request.
- **bcrypt must stay off the event loop** — wrap `verify_password` / `get_password_hash` in `run_in_threadpool`. It is ~250 ms of CPU on the loop APScheduler shares.
- **The API cannot be replicated.** Every replica runs its own scheduler → duplicate probes and duplicate log rows, which corrupts the "last 3 records" rule. Single replica only; scale the consumers instead.

## Remaining known issues

- `datetime.utcnow()` (naive) and `datetime.now(timezone.utc)` (aware) are mixed across modules; `alert_scheduler.py` normalizes tzinfo to compensate.
- `response_validation` is stored and selected but never evaluated anywhere.
- The alert digest job still polls Postgres on a 30-minute timer rather than consuming an `incidents` topic.
- `MetricsRepository.record_check` uses counter increments, which are not idempotent under at-least-once delivery. `reconcile_hour()` exists to correct drift but is not yet scheduled.

## Conventions

- Every router handler wraps its body in `try/except Exception`, sets `response.status_code` manually, and returns the `{success, message, data}` envelope via `ApiResponse[T]`. Match this shape in new endpoints.
- Logging goes through `get_logger()` from [app/utils/loggers.py](app/utils/loggers.py).
- **Never log request headers unredacted** — monitored endpoints carry user-supplied `Authorization` values, and they end up in the log file, container stdout, and CI output.
- Config is a single pydantic-settings singleton, `Config` in [app/core/config.py](app/core/config.py). Note the Kafka clients read `KAFKA_BROKER_URL`; `KAFKA_BROKER` exists but is dead.

## Do not change without strong reason

These were deliberately built and reviewed — they are correct:

- The LATERAL join in `get_services()` — the right idiom for latest-row-per-group.
- `UPDATE/DELETE ... WHERE id = ? AND owner_user_id = ? RETURNING id` — race-free authorized writes, no read-then-write TOCTOU.
- Ownership predicates on all seven user-facing service routes.
- Session injection via `Depends(get_db_session)`.
- bcrypt/passlib pinning (`passlib==1.7.4` + `bcrypt==4.0.1` — passlib breaks on bcrypt ≥ 4.1).
- Kafka keying by endpoint UUID (preserves per-endpoint ordering), `acks=all`, `enable_idempotence=True`.
- The two-process split and the 3-consecutive-failures incident rule.
