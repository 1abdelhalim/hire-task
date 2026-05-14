#!/bin/bash
set -e

SHOW_HELP() {
    echo "Usage: ./run.sh [--reset|-r] [--help]"
    echo ""
    echo "  (default)  Start databases and run Spark sync"
    echo "  --reset    Destroy all Docker volumes and restart fresh"
    echo "  --help     Show this help"
    exit 0
}

# ── Parse args
for arg in "$@"; do
    case "$arg" in
        --help|-h) SHOW_HELP ;;
        --reset|-r)
            echo "=== Resetting all Docker data ==="
            docker compose down -v
            ;;
    esac
done

# ── Prereqs
for cmd in docker spark-submit; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: '$cmd' not found. Install it first."
        exit 1
    fi
done

# Spark 4.x + Hadoop is incompatible with JDK 21+ (Subject.getSubject() removed).
# Force JDK 17 which is installed via brew.
export JAVA_HOME="/opt/homebrew/Cellar/openjdk@17/17.0.19/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
unset JDK_JAVA_OPTIONS

# ── Start databases
echo "=== Starting PostgreSQL & ClickHouse ==="
docker compose up -d

echo "Waiting for PostgreSQL..."
until docker compose exec -T postgres pg_isready -U postgres &>/dev/null; do sleep 1; done
echo "PostgreSQL ready"

echo "Waiting for ClickHouse..."
until docker compose exec -T clickhouse clickhouse-client --query "SELECT 1" &>/dev/null; do sleep 1; done
echo "ClickHouse ready"

# ── Verify tables exist
echo "=== Verifying tables ==="
docker compose exec -T postgres psql -U postgres -d mydb -c "\dt app_user_visits_fact"
docker compose exec -T clickhouse clickhouse-client --query "DESCRIBE default.app_user_visits_fact"

# ── Run Spark sync
echo "=== Running Spark sync job ==="
export PG_URL="jdbc:postgresql://localhost:5432/mydb"
export PG_USER="postgres"
export PG_PASS="postgres"
export CH_URL="jdbc:clickhouse://localhost:8123/default"
export CH_USER="default"
export CH_PASS=""

PYSPARK_PYTHON=python3.11 spark-submit \
    --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5 \
    sync_visits.py

# ── Verify
echo "=== Verifying data in ClickHouse ==="
docker compose exec -T clickhouse clickhouse-client --query "
SELECT count() AS total_rows FROM default.app_user_visits_fact FINAL
"

echo "=== Sample rows ==="
docker compose exec -T clickhouse clickhouse-client --query "
SELECT phone_number, points, remaining, state, created_at
FROM default.app_user_visits_fact FINAL
ORDER BY created_at LIMIT 10
"

# ── Demo: incremental sync (update a row)
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  DEMO: Update a row in PostgreSQL → re-sync         ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "Updating points for 0550693380 to 999 in PostgreSQL..."
docker compose exec -T postgres psql -U postgres -d mydb -c "
UPDATE app_user_visits_fact
SET points = 999, updated_at = 1800000000000
WHERE phone_number = '0550693380' AND id = '92128927-eaf7-4e31-9269-7f8c38e4d1cc';
"

echo "Re-running Spark sync..."
PYSPARK_PYTHON=python3.11 spark-submit \
    --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5 \
    sync_visits.py

echo "=== Verifying updated row in ClickHouse ==="
docker compose exec -T clickhouse clickhouse-client --query "
SELECT id, phone_number, points, updated_at
FROM default.app_user_visits_fact FINAL
WHERE phone_number = '0550693380'
ORDER BY updated_at DESC
LIMIT 3
"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  ALL DONE ✅                                        ║"
echo "║                                                     ║"
echo "║  Run 'docker compose down' to stop databases.       ║"
echo "║  Run './run.sh --reset' to wipe and restart.        ║"
echo "╚══════════════════════════════════════════════════════╝"
