# Internal API Monitoring System

A self-hosted API uptime and latency monitor — a personal UptimeRobot / Statuspage you run yourself.

Register your HTTP endpoints and the platform probes each one every 60 seconds, streams the results through Kafka, opens incidents when something is genuinely down, tracks hourly uptime, and emails you a periodic summary.

> **Deep dive:** [docs/architecture.md](docs/architecture.md) — full architecture, flow diagrams, API reference, and engineering notes
> **Visual:** [docs/architecture-diagram.html](docs/architecture-diagram.html) — open in a browser

---

## Features

- **Endpoint monitoring** — any HTTP method, with custom headers, body, expected status code and latency budget
- **Concurrent health checks** — up to 25 endpoints probed in parallel every 60 seconds; one slow endpoint can't stall the rest
- **Noise-resistant incidents** — raised only after **3 consecutive** failures, so a single dropped packet never pages you
- **Uptime & latency metrics** — hourly rollups with uptime % and p50/p95/p99 latency
- **Email digests** — incidents grouped per user and sent on each endpoint's own cadence
- **JWT auth** — signup, login, refresh, and a logout that genuinely revokes the token
- **Rate limiting** — login attempts capped per IP and per email

## Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI 0.111 (Python 3.12), Uvicorn |
| Database | PostgreSQL 16 · SQLModel + SQLAlchemy 2.0 async · asyncpg |
| Event bus | Apache Kafka (KRaft mode) via aiokafka |
| Cache | Redis 7 |
| Scheduling | APScheduler |
| Auth | PyJWT + passlib/bcrypt |
| Migrations | Alembic |
| Packaging | Docker + Docker Compose |

---

## Prerequisites

- Docker Engine 20.10+ with Docker Compose v2
- Or, to run locally: Python 3.12 with PostgreSQL, Kafka, and Redis reachable

---

## Setup

**1. Clone and create your `.env`** — copy the template, which documents every setting:

```bash
cp .env.example .env
```

Minimum you must fill in:

```env
PGHOST=localhost
PGPORT=5432
PGDATABASE=api_monitoring
PGUSER=postgres
PGPASSWORD=your_password

REDIS_HOST=localhost
REDIS_PORT=6379

KAFKA_BROKER_URL=localhost:9092
KAFKA_TOPIC_NAME=api-monitoring-results

# Required — the app refuses to start without a real value
SECRET_KEY=replace-with-a-long-random-string
ACCESS_TOKEN_EXPIRY=3600      # seconds
REFRESH_TOKEN_EXPIRY=86400    # seconds

BREVO_SMTP_SERVER=smtp-relay.brevo.com
BREVO_SMTP_PORT=587
BREVO_SMTP_USERNAME=your_brevo_login
BREVO_SMTP_PASSWORD=your_brevo_smtp_key
SENDER_EMAIL=alerts@yourdomain.com

API_BASE_URL=http://localhost:8000   # target for the test suite
```

**2. Start the stack — in this order:**

```bash
# a. Database only
docker compose up -d postgres

# b. Migrate BEFORE any app code starts.
#    `run` (not `exec`) because the app is deliberately still down;
#    --rm discards the throwaway container, --no-deps skips Kafka/Redis.
docker compose run --rm --no-deps fastapi sh -c "cd /app && alembic upgrade head"

# c. Now start everything
docker compose up -d --build

# d. Create the Kafka topic with 2 partitions (safe to re-run)
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic api-monitoring-results \
  --partitions 2 --replication-factor 1 --if-not-exists

# e. Verify — expect PartitionCount: 2
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic api-monitoring-results
```

If step (e) shows `PartitionCount: 1`, the topic was auto-created earlier at the old default. Widen it — partitions can be increased but never decreased:

```bash
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic api-monitoring-results --partitions 2
```

**Why the order matters:** the app does not create tables at startup, and `metrics_consumer` writes to a table this migration creates — start it first and it crash-loops until you migrate. Step (b) is safe to re-run any time; Alembic does nothing if you're already at head.

**3. Verify:**

```bash
curl http://localhost:8000/api/services/readyz     # postgres/redis/kafka → "up"
```

The API is now at `http://localhost:8000`, with Swagger docs at [/api/docs](http://localhost:8000/api/docs).

---

## What's Running

| Container | Role |
|---|---|
| `monitoring_api` | REST API + scheduled health checks and email digests |
| `consumer` ×2 | Kafka consumers → incident detection |
| `metrics_consumer` | Kafka consumer → hourly uptime and latency rollup |
| `postgres` · `kafka` · `redis` | Storage, event bus, cache |

One Kafka topic with 2 partitions feeds two independent consumer groups, so the metrics rollup can never slow down or break alerting. Details in [docs/architecture.md](docs/architecture.md#kafka-topology).

---

## Project Layout

```text
app/
├── main.py            FastAPI app, middleware, APScheduler jobs
├── core/              Config, JWT + password security
├── db/models.py       SQLModel tables
├── routers/           HTTP endpoints  (/api/auth, /api/services)
├── services/          Business logic + the probe loop and both consumers
├── repositories/      All database queries
├── infrastructure/    Kafka clients, HTTP prober, Redis helpers
├── middlewares/       JWT validation and revocation
└── utils/             DB engines, mail, logging

alembic/versions/      Database migrations
tests/                 Black-box HTTP integration suite
docs/                  Architecture guide + diagram
```

Layering is strict: `routers → services → repositories → database`.

---

## API Overview

Authenticated routes take `Authorization: Bearer <access_token>`.

| Method | Path | Description |
|---|---|---|
| POST | `/api/auth/signup` | Create an account |
| POST | `/api/auth/login` | Authenticate (rate limited) |
| POST | `/api/auth/refresh` | Exchange a refresh token |
| GET | `/api/auth/logout` | Revoke both tokens |
| GET | `/api/services/` | List your endpoints with latest health |
| POST | `/api/services/service` | Register an endpoint |
| GET | `/api/services/service_details/{id}` | Config + health + last 20 latencies |
| PUT · DELETE | `/api/services/service/{id}` | Update / remove |
| GET | `/api/services/service/{id}/logs` | Health-check history |
| GET | `/api/services/service/{id}/incident-logs` | Detected incidents |
| GET | `/api/services/health` · `/readyz` | Liveness · dependency readiness |

All responses share one envelope: `{ "success": bool, "message": str, "data": any }`.

Full reference: [docs/architecture.md](docs/architecture.md#api-reference).

---

## Running Locally (without Docker)

```bash
python -m venv venv
.\venv\Scripts\Activate.ps1     # Windows;  source venv/bin/activate on macOS/Linux
pip install -r requirements.txt
alembic upgrade head

# Terminal 1 — API + schedulers
uvicorn app.main:app --reload --port 8000

# Terminal 2 — incident detection
python -m app.services.healthConsumer

# Terminal 3 — metrics rollup
python -m app.services.metricsConsumer
```

Start the backing services with `docker compose up -d postgres redis kafka` and point `.env` at `localhost`.

---

## Tests

The suite is **integration-style** — it calls a running API over HTTP at `API_BASE_URL`, so start the stack and apply migrations first. `pytest` isn't in `requirements.txt`:

```bash
pip install pytest
pytest tests/ -v -s
```

Or run it inside the container the way CI does:

```bash
docker compose exec -T fastapi sh -c "pip install pytest && pytest tests/ -v -s"
```

Tests create real users and services and don't clean up — point them at a throwaway environment, never production.

---

## Common Commands

```bash
docker compose up -d --build             # start detached
docker compose logs -f fastapi           # API + scheduler logs
docker compose logs -f consumer          # incident detection
docker compose logs -f metrics_consumer  # hourly rollup
docker compose down                      # stop
docker compose down -v                   # stop and wipe data volumes

curl http://localhost:8000/api/services/readyz   # check Postgres, Redis, Kafka
```

Code is bind-mounted with `--reload` enabled — edits hot-reload without a rebuild.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `relation "users" does not exist` | Migrations not applied — run step 3 above |
| App won't start, complains about `SECRET_KEY` | Set a real value in `.env`; the placeholder is rejected by design |
| Health logs appear but no incidents | Needs **3 consecutive** failures — check `docker compose logs -f consumer` |
| Nothing happens for the first minute | Expected — the scheduler fires on an interval, not at startup |

More in [docs/architecture.md](docs/architecture.md#troubleshooting).
