# PG → ClickHouse Sync — `app_user_visits_fact`

## Overview

Batch Apache Spark (PySpark) job that synchronises the `app_user_visits_fact`
table from **PostgreSQL** to **ClickHouse** every 30 minutes using a
high-water mark on `updated_at`.

---

## Files

| File | Purpose |
|------|---------|
| `sync_visits.py` | PySpark batch application (the main deliverable) |
| `clickhouse_ddl.sql` | ClickHouse `ReplacingMergeTree` DDL — run once |
| `docker-compose.yml` | Spin up PostgreSQL + ClickHouse locally for testing |
| `init-postgres.sql` | Creates the table and inserts sample data in PostgreSQL |
| `verify.sql` | ClickHouse queries to verify synced data |
| `run.sh` | One-command: start DBs → sync → verify → demo |
| `task_explanation.md` | Full Q&A — design decisions, alternatives, interview prep |
| `clickhouse-network.xml` | **Required config override** — ClickHouse 24.3+ Docker restricts the `default` user to localhost only. This file opens network access so the Spark job (running on the host) can connect. Mounted into the container by `docker-compose.yml`. |
| `sample data.sql` | Raw sample data (as provided in the task) |

---

## Quick Start (with Docker)

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Apache Spark](https://spark.apache.org/downloads.html) (`brew install apache-spark`)
- Python 3.10+ (`brew install python@3.11`)
- **JDK 17** (`brew install openjdk@17`) — Spark 4.x requires JDK 17 and does NOT work with JDK 21+ (Hadoop `Subject.getSubject()` incompatibility)

### Run everything

```bash
chmod +x run.sh
./run.sh
```

This will:
1. Start PostgreSQL and ClickHouse via Docker
2. Wait for both to be healthy
3. Run the Spark sync job (reads from PG, writes to CH)
4. Show verification results
5. **Demo:** update a row in PG → re-sync → show the updated value in CH

### Reset (wipe all data)

```bash
./run.sh --reset
```

### Stop databases

```bash
docker compose down
```

---

## Manual Run (without Docker)

### 1. Set up PostgreSQL

Create the table and insert sample data:

```bash
psql -h localhost -U postgres -d mydb -f init-postgres.sql
```

### 2. Set up ClickHouse

```bash
clickhouse-client --multiquery < clickhouse_ddl.sql
```

### 3. Set environment variables

```bash
export PG_URL="jdbc:postgresql://localhost:5432/mydb"
export PG_USER="postgres"
export PG_PASS="postgres"
export CH_URL="jdbc:clickhouse://localhost:8123/default"
export CH_USER="default"
export CH_PASS=""
```

### 4. Run the Spark job

```bash
PYSPARK_PYTHON=python3.11 spark-submit \
    --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.3.1 \
    sync_visits.py
```

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  PostgreSQL  │ ──> │  PySpark     │ ──> │  ClickHouse │
│  (source)    │     │  Batch Job   │     │  (target)   │
└─────────────┘     └──────────────┘     └─────────────┘
                           │
                     Runs every 30min
                     (cron / Airflow)
```

### How the watermark works

1. Query ClickHouse: `SELECT max(updated_at) FROM app_user_visits_fact`
2. If empty → watermark = `0` (full load on first run)
3. Read PostgreSQL: `WHERE updated_at >= watermark OR (updated_at IS NULL AND created_at >= watermark)`
   - Uses `>=` (not `>`) to avoid missing rows with the exact same timestamp as the watermark
   - ReplacingMergeTree deduplicates any duplicate rows this may re-read
4. Append to ClickHouse (`ReplacingMergeTree` deduplicates by `id`)

---

## Key Design Decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Sync mode | **Batch** (not streaming) | Runs every 30min — no need for Kafka/Debezium |
| ClickHouse engine | **ReplacingMergeTree** | Handles updates: same `id` keeps latest `updated_at` |
| Watermark location | **ClickHouse `max(updated_at)`** | Self-contained, no external tracking needed |
| Connectors | **JDBC** for both PG and CH | Standard Spark API, no custom connectors |
| Config | **Environment variables** | Works everywhere (Docker, cron, Kubernetes) |

---

## Demo Scenario (for interviews)

The script includes a built-in demo:

```
Step 1: Full sync    → 16 rows copied from PG → CH
Step 2: Update PG    → change points for a customer
Step 3: Incremental  → only the changed row is synced
Step 4: Verify       → CH shows updated value (ReplacingMergeTree dedup)
```

---

## Production Schedule (crontab)

```cron
*/30 * * * * cd /path/to/project && ./run.sh >> /var/log/sync_visits.log 2>&1
```

Using Airflow DAG is recommended for production (retries, alerting, UI).

---

## Troubleshooting

| Symptom | Likely cause |
|---------|-------------|
| `Connection refused` on port 5432 | PostgreSQL not running — start with `docker compose up -d` |
| `Connection refused` on port 8123 | ClickHouse not running — start with `docker compose up -d` |
| `getSubject is not supported` / Hadoop error | Using JDK 21+ — set `JAVA_HOME` to JDK 17 (see prerequisites) |
| `ClassNotFoundException: org.apache.hc.core5.http.ClassicHttpRequest` | ClickHouse JDBC missing optional Apache HTTP client dependency. Non-fatal — driver falls back to `HTTP_URL_CONNECTION` automatically |
| `Authentication failed` with ClickHouse | ClickHouse Docker restricted `default` user to localhost — solved by `clickhouse-network.xml` override |
| `Code: 169. BAD_TYPE_OF_FIELD` | `ReplacingMergeTree` version column cannot be `Nullable` — must be non-nullable integer type |
| `No new records to sync` but data exists | Watermark advanced past older rows — normal after first full sync |
