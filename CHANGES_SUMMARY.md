# Changes Summary

Everything that was fixed and added, file by file, in plain language.

**Headline:** two subsystems in this project had **never worked** — incident detection and email alerts. Both failed silently because broad `except Exception` blocks logged the error and continued, so the system reported itself healthy while doing nothing. Those are fixed, verified live, and an incident now actually gets created.

**Verified running:** incident created after 3 failed checks · 2 Kafka partitions with 2 consumer groups · metrics rollup writing · indexes in use · **21/21 existing tests still pass** · app works with Redis stopped.

---

## Part 1 — Bugs that made features silently dead

### `app/repositories/service_repository.py`

**The bug that killed incident detection.** Two queries used by the Kafka consumer filtered on `owner_user_id == self.user_uid`. But the consumer creates the repository *without* a user, so `user_uid` was `None`. SQLAlchemy turns `column == None` into `column IS NULL`, and that column can never be null — so **the query always returned nothing**. The consumer logged "No API details found" and gave up, every single time.

- **Fix:** removed the ownership filter from `get_consumer_service()` and `get_last_three_records()`, with a comment explaining they are system-context queries. A background job reacting to its own event has no logged-in user; requiring one was a category error. These two are unreachable from any HTTP route, so there is no security impact — I checked all seven service endpoints.
- Also removed a now-pointless table join that only existed to support that filter.

**Other fixes here:**
- `func` (used for `func.now()`) was imported *inside a different function*, so `update_last_checked()` crashed with `NameError` on every call. Moved the import to the top of the file.
- `update_last_checked()` now accepts an optional timestamp — the caller was passing one to a function that didn't take it.
- `get_all_services()` (the query feeding the health-check loop) **ignored the `is_active` flag**, so disabled endpoints were still being probed every minute. Now filters on it, plus a `LIMIT 1000` safety cap.

### `app/services/service.py`

**The bug that broke incident merging.** `get_last_incident()` returns a plain dictionary, but the code accessed it like an object (`last_incident.end_time`). That raises `AttributeError`, which was caught and rolled back — meaning once an endpoint had one incident, **it could never get another one**.

- **Fix:** dictionary access (`last_incident["end_time"]`). Same for `initial_error` and `id`.
- `update_last_checked()` signature now matches how it is actually called.

### `app/services/alert_scheduler.py`

**The bug that killed all email alerts.** Line 14 called `db.get_session()` — **a method that does not exist**. Only `get_db_session` and `pg_session_factory` exist. So the alert job crashed on its very first line, every 30 minutes, forever. No digest email has ever been sent.

- **Fix:** one line — use `db.pg_session_factory()`, matching the pattern already used elsewhere in the codebase.

### `app/services/healthConsumer.py`

Three problems, all fixed:

1. **Same dictionary-vs-object bug** as above (`api_details.expected_status_code` on a dict).
2. **A malformed message killed the whole consumer process.** Reading `msg["id"]` happened *outside* the try/except, so one bad message crashed the container. Now everything is inside the guard, and unknown message shapes are logged and skipped.
3. **The database session helper returned an already-closed session.** It closed the session in a `finally` block that ran *before* the value was returned. It only appeared to work because SQLAlchemy quietly reopens. Replaced with the standard `async with` pattern.

Also: `response_time_ms` can be `NULL` in the database, and comparing `None > 500` raises `TypeError`. Now treated as 0.

The consumer's main loop is now **supervised**: if Kafka fails it reconnects with increasing delays instead of ending the loop and letting the process exit with code 0 — a silent death that made `docker ps` look fine.

---

## Part 2 — Speed and scale

### `alembic/versions/a1b2c3d4e5f6_...py` (new)

**The single biggest performance fix.** The database had **no indexes** on the tables that matter. Postgres does not create them automatically for foreign keys, so every dashboard load and every consumer query scanned the entire `health_check_logs` table — which grows by ~720,000 rows per day at 500 endpoints.

Added three indexes:
- `health_check_logs (endpoint_id, checked_at)`
- `incidents (endpoint_id, start_time)`
- `monitored_endpoints (owner_user_id)`

Plus a new `endpoint_metrics_hourly` table (see Part 4).

### `app/services/monitoring.py` (health-check loop)

**It was checking endpoints one at a time.** A plain `for` loop: probe endpoint 1, wait for the response, write to the database, publish to Kafka, *then* start endpoint 2. With a 15-second worst case per endpoint, 500 endpoints could not finish inside the 60-second window. (The docstring claimed "concurrently" — it wasn't.)

- **Now runs up to 25 checks at once**, using `asyncio.gather` with a semaphore to cap concurrency.
- **One slow endpoint can no longer affect the others.** Each check is wrapped individually, plus a 20-second hard deadline.
- **Bad data no longer kills the whole cycle.** Validation used to happen in one batch outside the error handling, so a single endpoint with a malformed URL aborted every check that minute. Now it's per-endpoint.
- **Order swapped: database write now happens BEFORE the Kafka publish.** The consumer reads recent checks from the database, so publishing first meant it could read a window missing the very check it was reacting to. Writing first means a failed publish delays detection by one cycle instead of producing a *wrong answer*.
- Added a summary log line each cycle: duration, total, succeeded, failed.

### `app/infrastructure/clients/api_client.py` (the HTTP prober)

- **Was creating a brand-new HTTP client for every single check** — a full connection handshake per endpoint per minute. Now uses one shared, pooled client.
- **Connection reuse is capped at 15 seconds on purpose.** A reused connection the other server already closed fails, and the library doesn't retry — which on a monitoring tool would look like *the endpoint is down*. Recycling early avoids inventing failures.
- **Pool exhaustion is no longer reported as an endpoint failure.** `PoolTimeout` looks like a normal timeout to Python; three in a row would have created an incident for a perfectly healthy service. It's now handled separately.
- **Fixed the broken response truncation.** When a response was over 100KB the code set `"_truncated": True` and then stored **the entire untruncated body anyway**. It also converted huge payloads to text just to measure them, blocking everything else. Now checks the size first and caps at 8KB.
- Removed a duplicated block of imports.

### `app/utils/connect.py`

- **Database connection pool raised from 15 to 30.** This had to happen *before* enabling concurrency — 25 simultaneous checks against a 15-connection pool would fail with confusing per-endpoint errors.
- **Redis connections now have a 250ms timeout.** They previously had *none*, meaning a frozen (not crashed) Redis would hang every request forever. This is what makes "Redis is optional" actually true.
- Redis password is now URL-encoded; special characters would have silently broken the connection.

### `app/main.py`

- **Scheduler was skipping runs.** The library's default grace period is **1 second** — if the app was momentarily busy when the timer fired, the check was silently abandoned. Raised to 30 seconds.
- Made "only one cycle at a time" explicit, since two overlapping cycles would double-write history and corrupt the 3-failure rule.
- Starts and cleanly closes the shared HTTP client.
- Refuses to start if `SECRET_KEY` is missing or left at the placeholder.

---

## Part 3 — Kafka made reliable

### `app/infrastructure/kafka/consumer.py` (rewritten)

- **Messages are now confirmed only after successful processing.** Previously offsets were committed on a 5-second timer regardless of whether processing worked, so a crash **lost messages**. Now it's at-least-once — safe here because reprocessing recalculates the same answer.
- **Fixed an unrecoverable "poison message" trap.** JSON decoding was configured *inside the Kafka library*, where a bad message raises somewhere your code cannot catch it — so the same broken message is re-read forever. Decoding now happens in our own code, where it can be logged, skipped, and moved past.
- **No longer shuts itself down on error.** It used to close the consumer and end quietly; recovery is now the caller's job, with retry and backoff.
- Added missing security settings so a password-protected Kafka would work (the producer had them, the consumer didn't).
- Consumer group is now configurable, which is what allows a second group to exist.

### `app/infrastructure/kafka/producer.py`

- **Recovers if Kafka was down at startup.** Previously it gave up permanently and returned "failed" for the life of the process — which is exactly what happened here: **Kafka had been stopped for 2 months and every health result was being silently dropped.**
- **Reconnect attempts are serialised with a lock.** Without it, 25 concurrent checks finding a dead connection would each start their own retry storm.
- **Fixed a memory leak** — each retry created a new producer without shutting down the previous one.
- Retry delays now back off gradually instead of hammering at a fixed interval.

### `app/schemas/service.py`

**Kafka messages now carry the information needed to judge them** — expected status code, expected latency, name, and a version number. Before, the message was just "something happened, go look it up in the database". Now the consumer doesn't need that lookup, and re-reading old messages produces the same verdict even if the endpoint has since been edited.

### `docker-compose.yml`

- **Kafka topic: 1 partition → 2.** With one partition only one consumer can ever be active, no matter how many you start.
- **Kafka now has a storage volume.** Without it, every topic and all consumer progress vanished whenever the container was replaced.
- **The detector now runs 2 copies**, one per partition.
- **Added the metrics rollup service** (below).

---

## Part 4 — New feature: uptime and latency statistics

### `app/services/metricsConsumer.py` + `app/repositories/metrics_repository.py` (new)

A **second consumer group** reading the same stream independently. It answers the two questions the product couldn't: *"what's my uptime this month?"* and *"is my response time getting worse?"*

It keeps hourly totals per endpoint: checks, successes, and a latency histogram.

**Why this is a separate consumer and not a database query:** at 500 endpoints, "uptime over 30 days" would mean adding up ~21 million rows every time someone opens the dashboard. Summarising as data arrives is simply the right approach.

**Why a separate group:** it has its own position in the stream, so if it falls behind or crashes, **alerting is completely unaffected**.

Honest limitation, documented in the code: counters can double-count if a message is delivered twice. The drift on a percentage is tiny, and `reconcile_hour()` recalculates from the raw logs to correct it. *(That function exists but isn't scheduled yet.)*

---

## Part 5 — Security

### `app/services/auth.py`

**Anyone's own valid token could mint new access tokens forever.** The refresh endpoint never compared the token you sent against the one stored for you — it just read your user ID from it and then validated *the database's* copy. So **an ordinary access token, or a refresh token revoked months ago, worked**. It also ignored expiry entirely.

Three checks added: the token must match the stored one, must not be expired, and must actually be a refresh token.

Password hashing moved off the main thread (below).

### `app/core/security.py`

- **Expired tokens were being accepted** by the function every protected route depends on — expiry checking was explicitly disabled. Now enforced.
- A missing or malformed `Authorization` header returned **500 instead of 401**. Now handled properly.

### `app/routers/auth.py`

- **Password hashing was freezing the entire application.** bcrypt takes ~250ms and was running directly on the main thread — the same thread the health checks run on. About 4 login attempts per second would stall monitoring completely. Now runs on a worker thread.
- **Added login rate limiting** (30/5min per IP, 5/15min per email). There was none at all. Checked *before* the database lookup and before hashing, so blocked attempts cost nothing.
- **Logout now actually logs you out.** It only cleared the refresh token, leaving the access token valid for up to an hour. It's now revoked immediately.

### `app/middlewares/token_refresh.py`

Checks the revocation list on every authenticated request — one place, so no route needed changing. If Redis is unavailable it lets requests through rather than logging everyone out.

### `app/core/config.py`

- **Removed the hardcoded fallback secret key** (`"change-this-secret"`). Without a `.env` file the app booted and signed real tokens with a key that is public in this repository.
- **Fixed a units bug**: the refresh-token lifetime was documented in days but used as seconds, so the default meant tokens expired **after 1 second**.

---

## Part 6 — Redis (previously unused)

Redis was running, connected, and **never actually used** — `cache.py` was empty and `client.py` was entirely commented out.

### `app/infrastructure/redis/client.py` + `cache.py` (written)

Two small features, both chosen because they solve real problems here:
1. **Logout revocation list** — the token IDs were already being generated and thrown away.
2. **Login rate limiting** — there was no protection at all.

**Everything fails open.** If Redis is down, rate limiting allows and revocation checks pass. Verified by stopping the container: signup and login both kept working.

Deliberately *not* built: caching dashboard queries (the indexes made that unnecessary), and moving the recent-checks window into Redis (it would create a second source of truth for alerts).

---

## Part 7 — Smaller fixes

- **`app/routers/service.py`** — added `/api/services/readyz`, which actually checks Postgres, Redis, and Kafka. The existing `/health` returns "OK" no matter what, including while Kafka was dead for two months.
- **`app/utils/loggers.py`** — log messages containing emoji **crashed the logger on Windows**, printing a stack trace instead of the log line. Now forces UTF-8.
- **`.gitignore`** — `*.log` doesn't match rotated files like `app.log.1`, which is how a 5MB log file ended up committed. Added `*.log.*` and `logs/`.
- **`app/db/models.py`** — added the `EndpointMetricsHourly` model.

---

## Deliberately unchanged

These were reviewed and are correct — please don't "fix" them:

- The **LATERAL join** in `get_services()` — the right way to fetch the newest row per group.
- `UPDATE ... WHERE owner_user_id = ? RETURNING id` — checks ownership and updates in one step, with no race condition.
- **Ownership checks on all seven service endpoints** — no user can reach another user's data.
- The **session injection pattern** and the routers → services → repositories layering.
- The **bcrypt/passlib version pinning** (newer bcrypt breaks passlib).
- Kafka **keying by endpoint ID** — this is what guarantees each endpoint's checks stay in order and reach the same consumer.

---

## Still outstanding

1. **Email digests still poll the database** every 30 minutes instead of consuming an incidents topic. Working now, but not event-driven.
2. **`reconcile_hour()` isn't scheduled** — the drift correction for metrics exists but nothing calls it.
3. **`response_validation`** is stored on every endpoint and never actually checked anywhere.
4. **Mixed timezone handling** — some code uses naive timestamps, some uses timezone-aware.

---

## Running it

```bash
docker compose up -d --build
docker compose exec -T fastapi sh -c "cd /app && alembic upgrade head"
```

To confirm incident detection works: register an endpoint with an `expected_status_code` it will never return, wait ~3 minutes, then check the `incidents` table for a new row.
