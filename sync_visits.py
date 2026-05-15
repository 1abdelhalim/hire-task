#!/usr/bin/env python3
"""
Spark batch job: sync app_user_visits_fact from PostgreSQL to ClickHouse.
Runs every 30 minutes via cron / scheduler.

INTERVIEW NOTE: This is the core deliverable. The interviewer will likely
walk through each function. Know the flow:
  1. get_watermark()  → read max(updated_at) from ClickHouse
  2. read_new_records() → fetch delta from PostgreSQL
  3. write_to_clickhouse() → append to ClickHouse

Usage:
    spark-submit \
        --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5 \
        sync_visits.py
"""

# ── Standard library imports ──────────────────────────────────────────────
import os     # Read environment variables for DB credentials
import sys    # sys.exit(1) on failure — allows monitoring to detect errors
import logging

# ── PySpark imports ──────────────────────────────────────────────────────
from pyspark.sql import SparkSession
# coalesce: replace NULL with 0 — needed because ClickHouse ReplacingMergeTree
#           version column cannot be NULL (tricky part!)
# col:       reference a DataFrame column by name
# lit:       create a literal value (e.g., lit(0)) for use in expressions
from pyspark.sql.functions import coalesce, col, lit

# ── Logging setup ────────────────────────────────────────────────────────
# Interview tip: logging is important for production monitoring.
# Without it, you can't tell WHY a job failed.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("sync_visits")

# ── Configuration from environment variables ──────────────────────────────
# Interview tip: env vars are portable across Docker, Kubernetes, cron, CI/CD
# No hardcoded secrets in code — best practice for security.
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://localhost:5432/mydb")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
# Default points to localhost:5432 — works with Docker Compose out of the box

CH_URL = os.getenv("CH_URL", "jdbc:clickhouse://localhost:8123/default")
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASS", "")
# Default points to localhost:8123 — ClickHouse HTTP interface
# NOTE: ClickHouse 24.3+ restricts default user to localhost.
#       We fixed this with clickhouse-network.xml override.

# Table names — hardcoded for this single-table solution
# Interview question: "What if you need 20 tables?"
# → Answer: parameterize this, build a generic framework
PG_TABLE = "app_user_visits_fact"
CH_TABLE = "app_user_visits_fact"


# ══════════════════════════════════════════════════════════════════════════
# Function 1: get_watermark
# ══════════════════════════════════════════════════════════════════════════
# This is the HIGH-WATER MARK. It answers: "What's the latest timestamp
# we already synced?" Everything AFTER this timestamp needs to be fetched.
#
# Interview tip: THIS IS THE CORE CONCEPT. Explain it first, and the rest
# of the solution makes sense naturally.
def get_watermark(spark):
    """Return the max updated_at from ClickHouse as the high-water mark."""
    try:
        # Query: if table is empty, COALESCE returns 0
        # If table has data, returns the maximum updated_at value
        query = f"SELECT coalesce(max(updated_at), 0) AS watermark FROM {CH_TABLE}"

        # Use Spark's built-in JDBC data source — no custom connector needed
        # This is the SAME API we use for PostgreSQL, keeping code simple
        df = (
            spark.read.format("jdbc")
            .option("url", CH_URL)
            .option("user", CH_USER)
            .option("password", CH_PASS)
            .option("query", query)
            # ClickHouse JDBC driver class — must be in --packages
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .load()
        )
        # df.collect() returns an array of Row objects
        # [0][0] = first column of first row = the watermark value
        return df.collect()[0][0] or 0

    except Exception as e:
        # If ClickHouse is unreachable OR table doesn't exist yet (first run)
        # → log warning and return 0 (trigger a FULL LOAD)
        # Interview question: "What if ClickHouse is down?"
        # → Answer: watermark resets to 0, next run re-syncs everything
        logger.warning("ClickHouse table empty or unreachable, starting from 0: %s", e)
        return 0


# ══════════════════════════════════════════════════════════════════════════
# Function 2: read_new_records
# ══════════════════════════════════════════════════════════════════════════
# Fetches only rows that have changed since the last sync.
# Interview tip: Explain the WHERE clause carefully — it has TWO conditions
# for a reason (see tricky parts below).
def read_new_records(spark, watermark):
    """Read records from PostgreSQL where updated_at > watermark."""
    # ── Build the WHERE clause ────────────────────────────────────────────
    # Condition 1: updated_at >= watermark
    #   → Uses >= (not >) to avoid missing rows where updated_at equals the
    #     watermark exactly. This can happen when two rows share the same
    #     millisecond timestamp and only one was synced in the previous batch.
    #   → Using > would skip the second row permanently.
    #   → Using >= means we MIGHT re-read the row that set the watermark,
    #     but ReplacingMergeTree deduplication handles this safely.
    #
    # Condition 2: updated_at IS NULL AND created_at >= watermark
    #   → TRICKY PART! Rows with NULL updated_at (never updated) would be
    #     missed by condition 1 alone. This fallback uses created_at instead.
    #   → Without this, newly inserted rows with NULL updated_at would only
    #     be picked up on the very FIRST run (when watermark = 0).
    #   → After that, NULL >= any_number is FALSE → rows would be MISSED.
    where = (
        f"updated_at >= {watermark} "
        f"OR (updated_at IS NULL AND created_at >= {watermark})"
    )

    # ── Execute the query ────────────────────────────────────────────────
    # Uses Spark JDBC with a subquery — the subquery applies the WHERE filter
    # in PostgreSQL itself, so Spark only receives the delta (new/changed rows).
    # This is MORE EFFICIENT than reading all rows and filtering in Spark.
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        # dbtable takes a subquery — this is the "pushdown" optimization
        # The "(SELECT * FROM ...) AS sub" wrapper is needed for Spark JDBC syntax
        .option("dbtable", f"(SELECT * FROM {PG_TABLE} WHERE {where}) AS sub")
        .option("driver", "org.postgresql.Driver")
        # fetchsize=5000: Read 5000 rows at a time (performance tuning)
        # Without this, Spark fetches ALL rows at once — memory issue for large tables
        .option("fetchsize", 5000)
        .load()
    )

    # ── Handle NULL updated_at values ─────────────────────────────────────
    # TRICKY PART: ClickHouse ReplacingMergeTree version column (updated_at)
    # CANNOT be Nullable. If we pass NULL from PostgreSQL, ClickHouse rejects it.
    # Fix: Replace any NULL updated_at with 0 (default value).
    #
    # Why 0 and not something else?
    # → 0 is less than any real timestamp, so it won't interfere with the watermark
    # → On the NEXT sync, this row's updated_at is 0, which is NEVER > watermark
    #   → So it won't be re-read (correct — nothing changed)
    df = df.withColumn("updated_at", coalesce(col("updated_at"), lit(0)))
    return df


# ══════════════════════════════════════════════════════════════════════════
# Function 3: write_to_clickhouse
# ══════════════════════════════════════════════════════════════════════════
# Simple append to ClickHouse. No special merge/upsert logic needed —
# ReplacingMergeTree handles deduplication automatically.
def write_to_clickhouse(df):
    """Append DataFrame to the ClickHouse table."""
    (
        df.write.format("jdbc")
        .option("url", CH_URL)
        .option("user", CH_USER)
        .option("password", CH_PASS)
        .option("dbtable", CH_TABLE)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        # batchsize=5000: Insert 5000 rows per JDBC batch (performance tuning)
        .option("batchsize", 5000)
        .mode("append")  # Always append — ReplacingMergeTree deduplicates
        .save()
    )


# ══════════════════════════════════════════════════════════════════════════
# Main function — orchestrates the three steps
# ══════════════════════════════════════════════════════════════════════════
def main():
    # ── Initialize Spark ─────────────────────────────────────────────────
    # appName: shows up in Spark UI — helps with monitoring/debugging
    # fetchsize is set on the JDBC reader (read_new_records), not here.
    # The session-level config "spark.jdbc.fetchsize" has no effect —
    # Spark ignores it. Only the per-reader .option("fetchsize", 5000) works.
    spark = (
        SparkSession.builder.appName("pg_to_clickhouse_sync")
        .getOrCreate()
    )

    # ── Sync pipeline ────────────────────────────────────────────────────
    # Interview tip: This is a textbook ETL pipeline pattern:
    #   Extract (watermark from CH) → Transform (filter in PG) → Load (write to CH)
    try:
        # Step 1: Get the watermark (last synced timestamp from ClickHouse)
        watermark = get_watermark(spark)
        logger.info("Watermark set to %s", watermark)

        # Step 2: Read only NEW/CHANGED rows from PostgreSQL
        df = read_new_records(spark, watermark)
        count = df.count()  # Triggers Spark execution — actually runs the query
        logger.info("Records to sync: %s", count)

        # Step 3: Write to ClickHouse (only if there are new rows)
        # If count == 0, we skip the write — saves a JDBC connection
        if count > 0:
            write_to_clickhouse(df)
            logger.info("Sync completed — %s records written", count)
        else:
            logger.info("No new records to sync")

    except Exception as e:
        # Any failure → log and exit with code 1
        # Exit code 1 is important for cron/monitoring to detect failures
        logger.error("Sync failed: %s", e)
        sys.exit(1)

    finally:
        # ALWAYS stop Spark — releases memory and closes connections
        # Interview tip: This runs even if the job fails (finally block)
        spark.stop()


# ── Entry point ──────────────────────────────────────────────────────────
# This pattern ensures the Spark context is only created when the script
# is run directly (not when imported as a module).
if __name__ == "__main__":
    main()
