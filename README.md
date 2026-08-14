# Internal API Monitoring System

A self-hosted API uptime and latency monitor. Register your HTTP endpoints, and the platform probes each one every 60 seconds, streams results through Kafka, detects incidents, and emails you a periodic summary.

Built with **FastAPI · PostgreSQL · Kafka · Redis · Docker**.

> Architecture, flow diagrams, API reference, and operational notes: [architecture.md](architecture.md)

---

## Prerequisites

- Docker Engine 20.10+ with Docker Compose v2
- Or, to run locally: Python 3.12 with PostgreSQL, Kafka, and Redis reachable

---

## Setup

**1. Clone and create a `.env` in the repo root:**

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

SECRET_KEY=replace-with-a-long-random-string
ACCESS_TOKEN_EXPIRY=3600
REFRESH_TOKEN_EXPIRY=1

BREVO_SMTP_SERVER=smtp-relay.brevo.com
BREVO_SMTP_PORT=587
BREVO_SMTP_USERNAME=your_brevo_login
BREVO_SMTP_PASSWORD=your_brevo_smtp_key
SENDER_EMAIL=alerts@yourdomain.com

API_BASE_URL=http://localhost:8000   # target for the test suite
```

**2. Start the stack:**

```bash
docker compose up --build
```

**3. Apply the migrations** — the app does not create tables on startup:

```bash
docker compose exec -T fastapi sh -c "cd /app && alembic upgrade head"
```

That's it. The API is at `http://localhost:8000`, with Swagger docs at [/api/docs](http://localhost:8000/api/docs).

---

## Running Locally (without Docker)

```bash
python -m venv venv
.\venv\Scripts\Activate.ps1     # Windows;  source venv/bin/activate on macOS/Linux
pip install -r requirements.txt
alembic upgrade head            # create/update the schema

# Terminal 1 — API + schedulers
uvicorn app.main:app --reload --port 8000

# Terminal 2 — Kafka consumer (required for incident detection)
python -m app.services.healthConsumer
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
docker compose up -d --build     # start detached
docker compose logs -f fastapi   # API + scheduler logs
docker compose logs -f consumer  # incident detection logs
docker compose down              # stop
docker compose down -v           # stop and wipe data volumes
```

Code is bind-mounted with `--reload` enabled — edits hot-reload without a rebuild.
