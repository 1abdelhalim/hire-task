# Task Explanation & Q&A

## Overview

Sync records from `app_user_visits_fact` in **PostgreSQL** to an identical table in **ClickHouse** using **Apache Spark (PySpark)**, running every 30 minutes.

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  PostgreSQL  │ ──> │  PySpark     │ ──> │  ClickHouse │
│  (source)    │     │  Batch Job   │     │  (target)   │
└─────────────┘     └──────────────┘     └─────────────┘
                           │
                     Runs every 30min
                     via cron / Airflow
```

---

## Files Delivered

| File | Purpose |
|------|---------|
| `clickhouse_ddl.sql` | ClickHouse table DDL (run once before first sync) |
| `sync_visits.py` | PySpark batch application |
| `README.md` | Environment config, `spark-submit` command, crontab schedule |
| `sample data.sql` | Sample data for testing (provided with task) |

---

## Design Decisions — Q&A

### 1. Why batch instead of streaming / CDC?

**Q: Why didn't you use Spark Structured Streaming with a CDC tool like Debezium or Kafka?**

**A:** The task requirement says the job runs every 30 minutes — that's a natural batch cadence. A batch approach is simpler to implement, debug, and operate:

- No extra infrastructure needed (no Kafka, no Debezium, no schema registry)
- No CDC connector configuration or monitoring
- Easier to backfill — just set the watermark to 0 and re-run
- The 30-minute window provides enough tolerance for near-real-time needs

If the requirement were sub-minute latency, CDC would be the right choice.

### 2. Why ReplacingMergeTree instead of plain MergeTree?

**Q: ClickHouse's MergeTree is append-only. How do you handle updates to existing rows?**

**A:** `ReplacingMergeTree(updated_at)` is used. When the same `id` appears multiple times, background merges keep only the row with the highest `updated_at`. This gives us the "latest state" mirror of PostgreSQL without losing data during the merge window.

- `ORDER BY id` — deduplication key
- `updated_at` as the version column — latest timestamp wins
- Queries use `SELECT ... FINAL` for a consistent current view
- If full history were needed, plain `MergeTree` would be used (but that is rare for fact tables like this)

### 3. How does the watermark work?

**Q: Explain the watermark mechanism step by step.**

**A:**

1. Before reading from PostgreSQL, the app queries ClickHouse:
   ```sql
   SELECT coalesce(max(updated_at), 0) FROM app_user_visits_fact
   ```
2. This returns the latest `updated_at` already in ClickHouse (or `0` if the table is empty).
3. PostgreSQL is queried:
   ```sql
   SELECT * FROM app_user_visits_fact
   WHERE updated_at >= <watermark>
      OR (updated_at IS NULL AND created_at >= <watermark>)
   ```
   Note: `>=` (not `>`) ensures rows with updated_at exactly equal to the watermark
   are not missed. ReplacingMergeTree deduplicates any duplicates this causes.
4. Those rows are appended to ClickHouse.
5. Next run, the watermark will be higher, so only newer/changed rows are fetched.

### 4. What happens on the very first run?

**Q: ClickHouse table is empty. How does the first run behave?**

**A:** `get_watermark()` catches the exception from the empty table and returns `0`. The PostgreSQL query then returns **all rows** (since all timestamps are > 0). This serves as the initial full load. Subsequent runs only pull new/changed rows.

### 5. How are NULL `updated_at` values handled?

**Q: Some sample rows have `updated_at IS NULL`. Won't they be missed on every run?**

**A:** The WHERE clause handles this:
```
(updated_at IS NULL AND created_at >= <watermark>)
```
Rows with `NULL` updated_at that were created after the watermark are still picked up. Once a row is synced once, it sits in ClickHouse. If later an update gives it a real `updated_at`, the update query will catch it.

**Edge case:** A row with `updated_at IS NULL` and a very old `created_at` (before watermark) will never be re-read. This is acceptable because:
- The row was already synced on an earlier run
- If it has no `updated_at`, it was never updated, so no sync is needed

### 6. What about deleted records?

**Q: If a row is deleted in PostgreSQL, how does ClickHouse know to remove it?**

**A:** This solution does **not** handle soft/hard deletes automatically. Two options depending on the use case:

| Option | How | When to use |
|--------|-----|-------------|
| **Ignore** | Do nothing. Deleted rows remain in ClickHouse. | If deletions are rare or historical accuracy matters |
| **Soft-delete column** | Use `is_deleted` flag. Include `WHERE is_deleted = 0` in queries. | If PostgreSQL uses soft deletes (it has an `is_deleted` column) |
| **CDC with Debezium** | Capture the delete event. | If hard deletes must be reflected in real time |

For this implementation, we assume **soft deletes** via the `is_deleted` column, which is already present in the schema. The data flows naturally — when PostgreSQL updates `is_deleted = 1`, the watermark-based query picks it up and ClickHouse gets the updated row.

### 7. What are the `--packages` in `spark-submit`?

**Q: What JDBC drivers are needed and why those specific versions?**

| Package | Purpose |
|---------|---------|
| `org.postgresql:postgresql:42.7.3` | PostgreSQL JDBC Type 4 driver |
| `com.clickhouse:clickhouse-jdbc:0.6.5` | ClickHouse JDBC driver (HTTP protocol) |

These are available from Maven Central. Spark downloads them automatically.

### 8. Why JDBC and not the native ClickHouse Spark connector?

**Q: ClickHouse offers a native Spark connector (`clickhouse-spark`). Why not use it?**

**A:** The JDBC approach uses Spark's built-in `jdbc` data source — no custom format, no additional API to learn. It works identically to the PostgreSQL side, making the code simpler and more maintainable.

The native connector offers better performance for bulk writes via the ClickHouse native protocol, but adds dependency complexity. For a 30-minute batch job with typical fact-table volumes, JDBC performance is sufficient.

### 9. How do you handle failures and retries?

**Q: What happens if a sync run fails partway through?**

**A:** The watermark is only read at the **start** of the run. If the write fails:
- Some rows may have been written, some not
- On the next run, the same watermark is used (since the max in CH hasn't changed)
- Rows already written are re-inserted (duplicate)
- `ReplacingMergeTree` deduplicates them by `id`

This gives **at-least-once** semantics. Exactly-once would require a transactional approach (e.g., two-phase commit), which is overkill for this use case.

### 10. What if a row is updated multiple times within 30 minutes?

**Q: Row A is updated at 10:01 and again at 10:15. Spark runs at 10:30. What happens?**

**A:** The watermark is, say, 10:00. The query `WHERE updated_at > 10:00` returns row A with its **latest state** (10:15 version). Only one row is written to ClickHouse. PostgreSQL's MVCC means the JDBC driver sees the committed state, which is the most recent update.

### 11. How do you configure this for production?

**Q: What needs to change for a real deployment?**

- **Credentials:** Use a secrets manager or encrypted env vars, not plain text
- **Spark cluster:** Adjust `--num-executors`, `--executor-memory`, `--executor-cores` based on data volume
- **Scheduling:** Use Airflow DAG, Oozie, or systemd timer instead of plain cron for better observability
- **Monitoring:** Add alerting on job failure (e.g., Spark History Server, Prometheus + Grafana)
- **Schema evolution:** If PostgreSQL adds columns, update both the ClickHouse DDL and the Spark query

### 12. Why are there two query conditions for watermark? And why `>=` not `>`?

**Q: Why not just `updated_at > watermark`?**

**A:** Two reasons:

1. **NULL updated_at handling**: Some rows in the sample data have `updated_at IS NULL` — rows that were inserted but never updated. Without the second condition, they would only be picked up on the very first run (when watermark = 0). On subsequent runs, they'd be missed because `NULL >= <any number>` is false. The fallback `(updated_at IS NULL AND created_at >= watermark)` ensures newly inserted rows (with no update timestamp) are picked up.

2. **`>=` instead of `>`**: We use `>=` to avoid missing rows where `updated_at` exactly equals the watermark. This can happen when two rows share the same millisecond timestamp and only one was synced in the previous batch. Using `>` would skip the second row permanently. The tradeoff is we might re-read the row that set the watermark, but `ReplacingMergeTree` deduplication handles this safely.

### 13. How would you test this?

**Q: How do you verify the solution works?**

1. **Unit test the watermark logic** — mock the Spark session and verify the SQL conditions generated
2. **Run against sample data** — insert the provided `sample data.sql` into a local PostgreSQL, run the Spark job, verify rows appear in ClickHouse
3. **Update a row in PostgreSQL** — change `points` for a row, re-run the job, verify ClickHouse reflects the change
4. **Check deduplication** — verify `SELECT count() FROM table FINAL` matches the distinct `id` count
5. **Edge case: empty source** — verify job exits cleanly with "No new records to sync"

### 14. What about time zones?

**Q: `created_at` and `updated_at` are epoch milliseconds. Does time zone matter?**

**A:** No — epoch milliseconds are timezone-independent (UTC everywhere). Both PostgreSQL and ClickHouse store and compare them as raw integers. This avoids any timezone conversion bugs.

### 15. Can the watermark drift or go backwards?

**Q: Could the watermark ever cause rows to be missed?**

**A:** The watermark is the **maximum** `updated_at` already in ClickHouse. It always moves forward (or stays the same). It cannot go backwards because:
- `max()` always returns the highest value
- New rows always have `updated_at >= 0`
- If no new rows are synced, the watermark stays the same

The only risk is clock skew — if PostgreSQL and ClickHouse servers have different system clocks, `updated_at` values from the future could advance the watermark prematurely. In practice, all servers should use NTP-synchronized clocks.

---

## Alternative Paths — What If We Chose Differently?

For each major decision, this section describes the alternative approach, how to implement it, and what would change.

---

### Alternative 1: Structured Streaming + CDC (instead of batch)

**The choice we made:** Batch reading every 30 minutes, using `updated_at` as a watermark.

**The alternative:** Use Debezium (CDC) → Kafka → Spark Structured Streaming.

**How to implement it:**

```
PostgreSQL WAL → Debezium Connector → Kafka Topic → Spark Stream → ClickHouse
```

1. **Debezium setup:** Deploy a Debezium PostgreSQL connector that reads the Write-Ahead Log (WAL) and publishes every insert, update, and delete event to a Kafka topic.
2. **Kafka topic:** One topic per table (e.g., `cdc.app_user_visits_fact`). Each message contains the full row image (before/after).
3. **Spark Structured Streaming:**
   ```python
   df = (spark.readStream
         .format("kafka")
         .option("subscribe", "cdc.app_user_visits_fact")
         .load())
   ```
4. **Transform CDC events:** Parse the Debezium JSON envelope, extract the `after` field for inserts/updates, handle `before` for deletes.
5. **Write to ClickHouse** using `foreachBatch()` with a JDBC or native sink.

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Infrastructure** | Requires Kafka cluster + Debezium + schema registry. Significantly more complex. |
| **Latency** | Near real-time (seconds instead of 30 minutes). |
| **Delete handling** | Debezium captures hard deletes natively — you can issue `ALTER TABLE DELETE` in ClickHouse. |
| **Backfill** | More difficult. You need a separate batch job for initial load, then switch to streaming. |
| **Schema evolution** | Debezium + schema registry handles this automatically. |
| **Operational cost** | Higher — more services to monitor, tune, and debug. |
| **Exactly-once** | Possible with Kafka offset tracking + idempotent sinks. |

**When to choose this alternative:**
- Requirement is sub-minute latency
- Hard deletes must be captured in real time
- Team already runs Kafka

---

### Alternative 2: Plain MergeTree (instead of ReplacingMergeTree)

**The choice we made:** `ReplacingMergeTree(updated_at)` to deduplicate and keep the latest row per `id`.

**The alternative:** Use plain `MergeTree`, appending every change as a new row — keep full history.

**How to implement it:**

DDL change only — remove the `ReplacingMergeTree` engine:

```sql
ENGINE = MergeTree
ORDER BY (id, created_at)
```

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Duplicate rows** | Every sync run that picks up the same `id` adds a new row. The same `id` can appear many times. |
| **Querying** | Every query must use `SELECT ... FINAL` or aggregate with `argMax()` to get the latest state. Without this, counts and sums will be wrong (inflated). |
| **History** | Full audit trail of every change is preserved by default. Good for analytics on state transitions. |
| **Storage** | Higher storage usage — every update adds a new row rather than replacing. The `updated_at` column tracks versions. |
| **Performance** | No dedup merge cost. Plain MergeTree merges are faster. |
| **Use case fit** | Better for **analytics on state changes** (e.g., "how did points change over time for this customer"). Worse for **OLTP-style lookups** (e.g., "what is the current points balance"). |

**When to choose this alternative:**
- You need a full change history / audit log
- Queries always use time-window aggregations rather than point lookups
- Storage is cheap and the table is append-mostly (few updates)

---

### Alternative 3: External Watermark Store (instead of querying ClickHouse)

**The choice we made:** Query `max(updated_at)` from the ClickHouse table itself at the start of each run.

**The alternative:** Store the watermark in an external location — a file, a separate metadata table in PostgreSQL, or a distributed key-value store like ZooKeeper/Redis.

**How to implement it:**

Using a PostgreSQL metadata table:

```sql
CREATE TABLE sync_watermarks (
    table_name VARCHAR(255) PRIMARY KEY,
    watermark BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- On each sync run:
-- 1. SELECT watermark FROM sync_watermarks WHERE table_name = 'app_user_visits_fact'
-- 2. Read from PG WHERE updated_at > watermark
-- 3. Write to ClickHouse
-- 4. UPDATE sync_watermarks SET watermark = <new_max> WHERE table_name = 'app_user_visits_fact'
```

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Decoupling** | Watermark does not depend on ClickHouse being available. If ClickHouse is down, the watermark is preserved. |
| **Manual override** | Easy to manually set the watermark to force a re-sync (e.g., set back to 0 to re-sync everything). |
| **Complexity** | One more component to manage. The tracking table or file must be backed up. |
| **Failure handling** | If the sync fails after reading PG but before writing CH, the watermark is not updated. Next run re-reads the same rows. Same at-least-once guarantee. |
| **Multi-table** | Scales cleanly to many tables. |

**When to choose this alternative:**
- Multiple tables are being synced
- ClickHouse availability is less reliable than PostgreSQL
- You need manual watermark manipulation for operational purposes

---

### Alternative 4: Native ClickHouse Spark Connector (instead of JDBC)

**The choice we made:** Use the ClickHouse JDBC driver via Spark's standard `jdbc` data source.

**The alternative:** Use the official `clickhouse-spark` native connector (`com.github.housepower:clickhouse-spark-runtime-3.4_2.12`).

**How to implement it:**

```python
df.write \
    .format("clickhouse") \
    .option("host", "ch-host") \
    .option("port", "8123") \
    .option("table", "app_user_visits_fact") \
    .option("user", "default") \
    .option("password", "") \
    .mode("append") \
    .save()
```

**packages:**
```
--packages com.github.housepower:clickhouse-spark-runtime-3.4_2.12:0.6.1
```

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Performance** | Native protocol is significantly faster for bulk writes (uses ClickHouse block data format instead of INSERT SQL over HTTP). |
| **API** | Uses Spark's custom data source API instead of standard JDBC. Different options, different behavior. |
| **Dependency** | Tied to specific Spark and Scala versions (e.g., `spark-runtime-3.4_2.12`). Upgrading Spark requires upgrading the connector. |
| **Features** | Supports native ClickHouse types (DateTime64, Decimal, etc.) without conversion issues. Supports `INSERT INTO ... VALUES` batching. |
| **Complexity** | One more connector to understand. JDBC is more universal and developers are more familiar with it. |

**When to choose this alternative:**
- High-volume tables (millions of rows per sync)
- Performance is critical and JDBC write speeds are insufficient
- You are already using ClickHouse as a primary analytics store

---

### Alternative 5: Config File (instead of environment variables)

**The choice we made:** Use environment variables for all configuration.

**The alternative:** Use a YAML or properties config file.

**How to implement it:**

```yaml
# config.yaml
postgres:
  url: jdbc:postgresql://localhost:5432/mydb
  user: postgres
  password: postgres
  table: app_user_visits_fact

clickhouse:
  url: jdbc:clickhouse://localhost:8123/default
  user: default
  password: ""
  table: app_user_visits_fact

sync:
  fetch_size: 5000
  batch_size: 5000
```

```python
import yaml
with open("config.yaml") as f:
    config = yaml.safe_load(f)
```

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Security** | Config files with passwords should not be committed to git. Environment variables are slightly safer (they live in the process, not on disk). |
| **Flexibility** | YAML supports nested config, lists, and types (integers, booleans). Environment variables are always strings. |
| **Portability** | Environment variables work everywhere (Docker, Kubernetes, cron). Config files need a path or a config server. |
| **Multi-environment** | With env vars: `PG_URL=prod:...` vs `PG_URL=dev:...`. With config files: you need separate files per environment (`config.prod.yaml`, `config.dev.yaml`). |

**When to choose this alternative:**
- Many configuration parameters (more than 5-6)
- Configuration is complex (nested, typed, includes lists)
- You are using a secrets manager (Vault) that writes to files

---

### Alternative 6: Airflow DAG (instead of cron)

**The choice we made:** Schedule via cron (simple crontab entry).

**The alternative:** Use Apache Airflow to orchestrate the Spark job as a DAG.

**How to implement it:**

```python
# airflow_dag.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    "pg_to_clickhouse_sync",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    sync_task = SparkSubmitOperator(
        task_id="sync_visits",
        application="sync_visits.py",
        packages="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5",
        ...
    )
```

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Observability** | Airflow provides UI, logs, retry history, alerting. Cron only provides exit codes. |
| **Dependencies** | You need an Airflow cluster (webserver, scheduler, worker, database). Significant infra cost. |
| **Retries** | Airflow retries failed tasks automatically with backoff. Cron requires a wrapper script. |
| **Backfill** | Airflow can backfill historical runs. Cron cannot — you run manually. |
| **Complexity** | Overkill for a single job. Cron is 1 line. Airflow is a full platform. |

**When to choose this alternative:**
- You already run Airflow for other pipelines
- You need SLA monitoring and automatic retries
- The pipeline grows to include multiple dependent tasks

---

### Alternative 7: CDC-Based Hard Delete Capture (instead of soft-delete via is_deleted)

**The choice we made:** Assume soft deletes — PostgreSQL sets `is_deleted = 1`, the watermark picks up the change naturally.

**The alternative:** Use Debezium CDC to capture and propagate hard `DELETE` operations to ClickHouse.

**How to implement it:**

1. Debezium publishes delete events to Kafka:
   ```json
   {
     "op": "d",
     "before": {"id": "...", ...}
   }
   ```
2. Spark stream reads delete events and issues ClickHouse delete:
   ```python
   # In foreachBatch:
   for row in deletes:
       spark.sql(f"ALTER TABLE {CH_TABLE} DELETE WHERE id = '{row.id}'")
   ```
   Or use a separate "delete log" table in ClickHouse, then merge on query.

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Completeness** | ClickHouse exactly mirrors PostgreSQL — deleted rows are removed. |
| **Complexity** | Requires full CDC pipeline (see Alternative 1). Not feasible with batch. |
| **ClickHouse limitation** | `ALTER TABLE ... DELETE` is an async heavyweight operation in ClickHouse. Not designed for high-frequency per-row deletes. |
| **Alternative strategy** | Instead of deleting rows, use a `_is_deleted` flag and filter in queries. This avoids the CDC complexity entirely. |

**When to choose this alternative:**
- Hard deletes are frequent and must be reflected for compliance/accuracy
- You already have a CDC pipeline
- Delete volume is low enough for ClickHouse's async DELETE mechanism (or you use `ReplacingMergeTree` with a `sign` column + `ALTER TABLE ... DROP PARTITION` for bulk)

---

### Alternative 8: Exactly-Once Semantics (instead of at-least-once)

**The choice we made:** At-least-once — duplicates are resolved by `ReplacingMergeTree` deduplication.

**The alternative:** Exactly-once using idempotent writes and transactional watermark commits.

**How to implement it:**

Using a **two-phase** approach:
1. Read from PG, compute the maximum `updated_at` from the fetched rows.
2. Write to ClickHouse.
3. Only update the watermark **after** the write succeeds — atomically in the same transaction as the read, if possible.

Or using a **write-ahead log**:
1. Write the batch of rows to a staging table in ClickHouse.
2. Atomically swap partitions or run `ALTER TABLE ... REPLACE PARTITION`.
3. Commit the watermark.

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Correctness** | No duplicates, even without deduplication. Each row is synced exactly once. |
| **Complexity** | Significantly harder. Distributed transactions across PostgreSQL and ClickHouse are not natively supported. Requires application-level coordination. |
| **Performance** | Slower — you need to track which rows have been committed. |
| **Necessity** | Usually overkill for analytics. A duplicate row that is deduplicated at query time is acceptable. |

**When to choose this alternative:**
- You are syncing to a system that does NOT support deduplication (e.g., plain MergeTree)
- Duplicate-sensitive aggregations are run directly on the table without dedup
- Regulatory requirements demand exactly-once

---

### Alternative 9: Multi-Table Generic Sync (instead of single-table)

**The choice we made:** One script, one table — hardcoded `app_user_visits_fact`.

**The alternative:** Build a generic sync framework that accepts any table name and schema, dynamically reads metadata from PostgreSQL, auto-creates the ClickHouse table, and syncs.

**How to implement it:**

```python
def get_table_schema(spark, table_name):
    """Read column names and types from PostgreSQL information_schema."""
    query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
    """
    return spark.read.jdbc(url=PG_URL, table=query, ...)

def create_clickhouse_table(schema):
    """Generate and execute ClickHouse CREATE TABLE from schema."""
    ...

def sync_table(table_name):
    schema = get_table_schema(spark, table_name)
    create_clickhouse_table(schema)
    # ... watermark + sync logic
```

**What changes / is affected:**

| Aspect | Impact |
|--------|--------|
| **Reusability** | One framework syncs any table. No need to write a new script per table. |
| **Type mapping** | Must handle PG→ClickHouse type mapping for all data types (text→String, int4→Int32, int8→Int64, jsonb→String, etc.). |
| **Testing** | More complex — must test with multiple schemas. |
| **Custom logic** | Per-table nuances (watermark column, dedup key, partition key) are harder to express generically. |

**When to choose this alternative:**
- You need to sync 10+ tables, not just one
- Tables share the same sync pattern
- You want a reusable framework for future tables

---

## Running & Demo — Interview Q&A

### Q1: How do I run this solution from scratch on a new machine?

**A:** The `run.sh` script does everything:

```bash
git clone <repo>
cd hiring-task
chmod +x run.sh
./run.sh
```

This installs nothing — it assumes Docker and Spark are already installed. Under the hood:
1. `docker compose up -d` starts PostgreSQL and ClickHouse
2. PostgreSQL auto-runs `init-postgres.sql` (creates table + inserts 16 sample rows)
3. ClickHouse auto-runs `clickhouse_ddl.sql` (creates `ReplacingMergeTree` table)
4. The Spark job reads from PG → writes to CH
5. Verification queries confirm the data arrived

### Q2: Walk me through what happens during a sync run.

**A:**
1. Spark session starts and loads JDBC drivers for PostgreSQL and ClickHouse
2. `get_watermark()` queries ClickHouse: `SELECT max(updated_at) FROM app_user_visits_fact`
   - First run → ClickHouse is empty → exception caught → returns `0`
3. `read_new_records()` queries PostgreSQL with watermark:
   ```sql
   SELECT * FROM app_user_visits_fact
   WHERE updated_at >= 0
      OR (updated_at IS NULL AND created_at >= 0)
   ```
   - Returns all 16 rows (all timestamps > 0)
4. `df.count()` logs "Records to sync: 16"
5. `write_to_clickhouse()` appends all 16 rows to ClickHouse via JDBC
6. Spark stops. Done.

### Q3: How do you demonstrate that incremental sync works?

**A:** The `run.sh` script has a built-in demo:

```bash
# After the first full sync, update a row in PostgreSQL:
docker compose exec -T postgres psql -U postgres -d mydb -c "
  UPDATE app_user_visits_fact
  SET points = 999, updated_at = 1800000000000
  WHERE id = '92128927-...';
"

# Re-run the Spark job
spark-submit ... sync_visits.py

# Verify in ClickHouse
docker compose exec -T clickhouse clickhouse-client --query "
  SELECT id, phone_number, points, updated_at
  FROM app_user_visits_fact FINAL
  WHERE phone_number = '0550693380'
"
```

The output shows:
- Only **1 record** synced (not 16) — the watermark filtered out unchanged rows
- The `points` value is `999` — the updated value
- ReplacingMergeTree kept the latest version by `updated_at`

### Q4: How do you handle duplicates in ClickHouse?

**A:** The table uses `ReplacingMergeTree(updated_at)` with `ORDER BY id`. When ClickHouse merges data parts, it keeps only the row with the highest `updated_at` for each `id`. Queries use `FINAL`:

```sql
SELECT count() FROM app_user_visits_fact FINAL
```

Without `FINAL`, the count would include duplicate rows from partial merges. To demonstrate:

```sql
-- Without FINAL (may show duplicates)
SELECT count() FROM app_user_visits_fact;

-- With FINAL (deduplicated)
SELECT count() FROM app_user_visits_fact FINAL;
```

### Q5: What happens if you run the sync twice without any changes?

**A:** The watermark doesn't advance (no new max `updated_at`), so the PostgreSQL query returns 0 rows. The log shows: "No new records to sync". The ClickHouse table is unchanged.

### Q6: How do you handle failures?

**A:** If the job crashes midway:
- **At-least-once semantics**: On the next run, the same watermark is used (because ClickHouse `max(updated_at)` didn't change). Already-written rows are re-inserted, but `ReplacingMergeTree` deduplicates them.
- **No data loss**: Every row is eventually written.
- **No data corruption**: The watermark never moves backwards.

### Q7: How would you handle large volumes (millions of rows)?

**A:**
- **Spark partitioning**: Use `partitionColumn`, `lowerBound`, `upperBound` on the JDBC read to parallelize
- **Fetch/batch size**: Already set to 5000 in the code — tune based on row size
- **ClickHouse optimization**: Add a `PARTITION BY` clause (e.g., by month on `created_at`) and order by `(store_id, created_at)` instead of just `id`
- **Incremental always**: The watermark ensures we never re-read old data. Only new/changed rows are processed

### Q8: How would you add monitoring?

**A:**
- **Logging**: Already logs watermark, record count, and errors
- **Metrics**: Track rows synced, sync duration, error rate
- **Alerting**: Pipe Spark exit codes to a monitoring system (e.g., PagerDuty, OpsGenie)
- **Data quality**: Add a verification step after sync (compare row counts or checksums)

### Q9: What tests would you add before production?

**A:**
| Test | What it verifies |
|------|-----------------|
| Full load | All rows from PG appear in CH after first run |
| Incremental load | Only changed rows are picked up on subsequent runs |
| NULL `updated_at` | Rows with `updated_at IS NULL` are still captured |
| Update propagation | Changed `points` or `state` appears in CH |
| Deduplication | `SELECT count() ... FINAL` matches distinct `id` count |
| Empty source | Job exits cleanly with "No new records" |
| Connection failure | Job handles DB downtime gracefully (logs warning, exits) |

### Q10: Tell me about the practical issues you encountered while running the demo.

**A:** Three issues came up when running locally:

1. **JDK version (Java 25 → 17)**: Spark 4.x on macOS bundles Hadoop 3.4.2 which calls `Subject.getSubject()` — a method that throws `UnsupportedOperationException` on JDK 21+. We fixed it by setting `JAVA_HOME` to JDK 17 (`brew install openjdk@17`).

2. **ClickHouse network restriction**: The ClickHouse Docker image auto-generated a `default-user.xml` that restricted the `default` user to localhost only. We mounted a `clickhouse-network.xml` override that allows connections from any IP (`::/0`).

3. **ReplacingMergeTree Nullable restriction**: The version column in `ReplacingMergeTree` cannot be `Nullable`. We changed `updated_at Nullable(Int64)` to `updated_at Int64 DEFAULT 0` in the DDL and used `COALESCE(updated_at, 0)` in the Spark query.

These are all environment/Docker issues — they don't affect the core solution design. In a real deployment, the ops team would handle these.

### Q11: What are the limitations of this solution?

**A:**
- **Hard deletes not captured**: If a row is deleted in PostgreSQL, it stays in ClickHouse. Requires CDC or soft-delete via `is_deleted`.
- **Schema changes**: Adding a column in PostgreSQL requires manual DDL update in ClickHouse. A generic solution would use `information_schema` to auto-detect schema.
- **Clock skew**: If servers have different clocks, `updated_at` values from the future could advance the watermark prematurely. Use NTP-synchronized clocks.
- **ClickHouse JDBC performance**: For very high volumes (100M+ rows per sync), the native ClickHouse Spark connector would be faster than JDBC.
- **JDK compatibility**: Spark 4.x on JDK 21+ has a known Hadoop `Subject.getSubject()` incompatibility. The solution uses JDK 17. Always check Spark's JDK compatibility matrix before deployment.

---

## Interview Tips — How to Talk About This Solution

This section is your interview prep. For each layer of the solution, it explains what to say, how to say it, and what traps to avoid.

---

### 1. The Big Picture (30-second elevator pitch)

**Say this:**
> "We needed to sync a fact table from PostgreSQL to ClickHouse every 30 minutes. I chose a batch approach using a watermark on `updated_at` — it's simple, requires no extra infrastructure like Kafka, and the 30-minute window means near-real-time is not a requirement. The Spark job queries ClickHouse for the last synced timestamp, reads only new or changed rows from PostgreSQL, and appends them to ClickHouse using `ReplacingMergeTree` which deduplicates by ID."

**Why this works:** It shows you understand the *why* behind every choice — not just the *what*.

---

### 2. The Watermark — This Is the Core Question

They **will** ask about this. Be ready to draw it or trace through it.

**What to say:**

> "The watermark is the `max(updated_at)` from ClickHouse. Before each sync, we read it. Then we query PostgreSQL for rows where `updated_at > watermark`. On the first run, ClickHouse is empty, so we catch the exception and use `0` — that gives us a full load. After that, only changed rows come through."

**Common follow-up:** "What about rows with `NULL updated_at`?"

> "Good catch — some rows in the sample data have `NULL`, meaning they were never updated. I handle this with an `OR` condition: `(updated_at IS NULL AND created_at > watermark)`. This ensures newly inserted rows without an update timestamp are still picked up."

**Trap to avoid:** Don't say "the watermark is stored in PostgreSQL" — it's stored in ClickHouse. If they ask why, say: "It's self-contained — we don't need a separate tracking table. The source of truth for what we've synced lives where the data lives."

---

### 3. Why Batch, Not Streaming?

**Say this:**

> "Batch is the right call here because the requirement is every 30 minutes. Streaming with Debezium and Kafka would add significant operational complexity — Kafka clusters, Debezium connectors, schema registry — for zero benefit at this cadence. Batch also makes backfills trivial: set the watermark to `0` and re-run."

**If they push back:** "But what if they want real-time later?"

> "The architecture supports that evolution. The ClickHouse table and PostgreSQL schema don't change. You'd only need to replace the Spark job with a streaming pipeline. The table design (`ReplacingMergeTree`) already handles the upsert semantics that streaming would need."

---

### 4. Why ReplacingMergeTree?

**Say this:**

> "ClickHouse's default MergeTree is append-only — it never modifies existing rows. If a row is updated in PostgreSQL and we re-sync it, we'd get a duplicate. `ReplacingMergeTree(updated_at)` solves this: during background merges, it keeps only the row with the highest `updated_at` for each `id`. Queries use `FINAL` to get the latest state."

**If they ask about performance:**

> "There's a small merge cost, but it's negligible for our volume. The alternative is plain `MergeTree` + `argMax()` in every query, which is error-prone and harder to maintain."

---

### 5. Handling Failures — At-Least-Once

**Say this:**

> "We have at-least-once semantics. If the job crashes after writing some rows but before completing, the watermark doesn't advance. The next run re-reads the same rows from PostgreSQL and re-inserts them. `ReplacingMergeTree` deduplicates the duplicates. This means no data loss, and the system self-heals on the next run."

**If they ask about exactly-once:**

> "Exactly-once would require distributed transactions across PostgreSQL and ClickHouse, or a two-phase commit. That's significant complexity. For analytics, at-least-once with deduplication is the standard pattern — it's what production systems like this typically use."

---

### 6. The "What About Deletes?" Question

**This is a known weakness. Be upfront about it.**

**Say this:**

> "This solution doesn't capture hard deletes. If a row is deleted from PostgreSQL, it stays in ClickHouse. In practice, for this table, there's an `is_deleted` column — so soft deletes are handled naturally by the watermark. For hard deletes, you'd need CDC with Debezium, or a periodic reconciliation job that compares the two tables."

**Why this answer is good:** You're not hiding the limitation. You're showing you've thought about it and have a practical solution (soft deletes with `is_deleted`).

---

### 7. How to Walk Through the Code

If they ask you to open `sync_visits.py` and explain it:

| Function | What to say |
|----------|-------------|
| `get_watermark()` | "Reads `max(updated_at)` from ClickHouse. Empty table → exception → returns 0." |
| `read_new_records()` | "Builds a WHERE clause with the watermark. Handles NULL `updated_at` with a fallback to `created_at`." |
| `write_to_clickhouse()` | "Standard JDBC append. `ReplacingMergeTree` handles dedup." |
| `main()` | "Orchestrates the three steps. Wraps everything in try/except so failures are logged and exit code is non-zero for monitoring." |

**Key detail to point out:** The `config("spark.jdbc.fetchsize", "5000")` — this shows you've thought about performance, not just correctness.

---

### 8. Testing — Show You Care About Quality

**Say this:**

> "I'd test five scenarios:
> 1. **Full load** — first sync, all rows arrive in ClickHouse
> 2. **Incremental load** — second sync with no changes, zero rows synced
> 3. **Update propagation** — change a row in PostgreSQL, re-sync, verify ClickHouse has the new value
> 4. **NULL `updated_at`** — insert a row with no `updated_at`, verify it's still picked up
> 5. **Deduplication** — verify `SELECT count() FINAL` matches distinct IDs"

**This shows you think like an engineer, not just a coder.**

---

### 9. Production Readiness — Don't Stop at the Code

**Say this:**

> "For production, I'd add:
> - **Secrets management** — pull credentials from Vault or AWS Secrets Manager, not env vars
> - **Monitoring** — track rows synced, sync duration, error rate
> - **Scheduling with Airflow** — retries, alerting, backfills built in
> - **Schema evolution** — if PostgreSQL adds a column, we need a process to update ClickHouse too"

**This shows you've deployed things before.**

---

### 10. Common Interview Traps — How to Avoid Them

| Trap | Bad answer | Good answer |
|------|-----------|-------------|
| "Why not just use a simple script?" | "Spark is overkill but the task said to use it." | "Spark handles large volumes, parallelizes reads/writes, and integrates with our existing data infrastructure. For 16 rows it's overkill, but the solution scales to millions." |
| "What if `updated_at` has duplicates?" | "That won't happen." | "If two rows have the same `updated_at`, both are synced. The watermark is `>= watermark_value` not `>`, so in edge cases we might re-read one row. That's fine — dedup handles it." |
| "Why not just use Postgres FDW or a materialized view?" | "I didn't think of that." | "Foreign Data Wrappers don't work with ClickHouse natively, and materialized views don't solve the cross-database sync problem." |
| "What's the data type for `Int64` in ClickHouse?" | (hesitation) | "`Int64` in ClickHouse maps to PostgreSQL's `int8` and Python's `int` — it's a 64-bit signed integer." |
| "How would you make this work for 100 tables?" | "I'd copy the script 100 times." | "I'd build a generic framework that reads `information_schema.columns`, auto-generates the ClickHouse DDL, and accepts table name as a parameter. The Alternative 9 section in my docs covers this." |

---

### 11. The Demo — What to Show and Say

If you're asked to run the demo (`./run.sh`):

| Step | What happens | What to say |
|------|-------------|-------------|
| Docker starts | PG + CH containers boot | "This gives us a clean, reproducible test environment." |
| First sync | 16 rows copied | "Full load — watermark was 0, all rows qualified." |
| Update PG | Points changed to 999 | "Simulating a business operation — a customer earned bonus points." |
| Second sync | 1 row copied | "Incremental load — watermark filtered out the 15 unchanged rows." |
| Verify CH | Shows 999 points | "ReplacingMergeTree kept the latest version. The dedup worked." |

**End with:** "And that's it — simple, reliable, and production-ready."

---

### 12. Questions You Should Ask THEM

At the end of the interview, asking good questions shows confidence:

> - "What's the current data volume and growth rate for this table? That affects whether we need partitioning."
> - "Do you use soft deletes or hard deletes in production? That determines whether we need CDC."
> - "Is this intended for reporting dashboards or ad-hoc analytics? That would affect the ClickHouse query patterns."
> - "What's the team's existing infrastructure — do you already run Kafka or Airflow?"

These questions show you're thinking beyond the task to the real-world deployment.

---

## Deep Dive: Current Data Flow (Trace a Single Row)

This section traces **exactly one row** through the entire pipeline — from PostgreSQL insert, to Spark detection, to ClickHouse storage. Understanding this at a granular level is crucial for interviews.

### The Row

Let's follow this row from `sample data.sql`:

```
id:        '92128927-eaf7-4e31-9269-7f8c38e4d1cc'
phone:     '0550693380'
points:    11.0
created_at: 1758628843010
updated_at: 1759341646369
```

### Step-by-step trace

```
Step 1: Row is INSERTED into PostgreSQL
        ↓
Step 2: Spark job starts (every 30 min)
        ↓
Step 3: get_watermark() queries ClickHouse:
        "SELECT coalesce(max(updated_at), 0) FROM app_user_visits_fact"
        → Returns 1759341646000 (some earlier watermark)
        ↓
Step 4: read_new_records() builds WHERE clause:
        "WHERE updated_at > 1759341646000
           OR (updated_at IS NULL AND created_at > 1759341646000)"
        ↓
Step 5: PostgreSQL query returns this row because:
        1759341646369 > 1759341646000  ✅
        ↓
Step 6: Spark reads the row into a DataFrame
        df.count() = 1  (among possibly other rows)
        ↓
Step 7: COALESCE(updated_at, 0) converts any NULL to 0
        (this row already has a value, so no change)
        ↓
Step 8: write_to_clickhouse() appends to ClickHouse via JDBC
        ↓
Step 9: Row lands in ClickHouse ReplacingMergeTree table
        - INSERT into the active data part
        - ORDER BY id determines sort order
        - updated_at = 1759341646369 is the version
        ↓
Step 10: Later, background merge runs:
         - If another row with same id exists
         - Keeps only the one with highest updated_at
         - The losing version is discarded
```

### What happens on each subsequent run?

| Run # | Watermark | Row picked up? | Why |
|-------|-----------|----------------|-----|
| 1 | 0 | Yes | First run, full load |
| 2 | 1759341646369 | No | updated_at == watermark (not >) |
| 3 (after row updated) | 1759341646369 | Yes | New updated_at > old watermark |
| 4 | newer value | No | Watermark advanced past the row |

### Memory & state diagram

```
                    ┌──────────────────┐
                    │   ClickHouse     │
                    │  max(updated_at) │────┐
                    └──────────────────┘    │
                     ▲                      │
                     │ read watermark       │ read watermark
                     │                      ▼
              ┌──────┴───────┐      ┌───────────────┐
              │  PostgreSQL   │      │     Spark     │
              │  rows where   │◄─────│  compare and  │
              │  updated_at   │      │  fetch delta  │
              │  > watermark  │─────►│               │
              └──────────────┘      └───────┬───────┘
                                            │
                                            │ write delta
                                            ▼
                                    ┌───────────────┐
                                    │   ClickHouse   │
                                    │  ReplacingMT   │
                                    │  (auto dedup)  │
                                    └───────────────┘
```

---

## Change Scenarios — "What Would You Change If..."

Interviewers love asking variations of this. Below are the most common change requests and how to handle them.

---

### Scenario 1: "We need to sync 20 tables, not just one."

**What they're testing:** Whether you designed for scale or just hardcoded one table.

**Answer:**
> "The current script is hardcoded to one table. To scale to 20, I'd build a **generic sync framework**:
> 1. Accept table name as a parameter (e.g., `--table app_user_visits_fact`)
> 2. Dynamically read schema from `information_schema.columns`
> 3. Auto-generate the ClickHouse DDL with correct type mapping
> 4. Accept a configurable watermark column name (not hardcoded to `updated_at`)
> 5. Run one job per table, or batch them in a single Spark application
>
> The challenge is type mapping — PostgreSQL has `jsonb`, `uuid`, `numeric(x,y)` that don't have direct ClickHouse equivalents. I'd need a mapping table."

**Impact analysis:**
| Factor | Change |
|--------|--------|
| Code | Add config for which tables to sync, schema detection |
| DDL | Auto-generated from `information_schema` instead of manual |
| Monitoring | Need per-table tracking (watermarks, row counts, errors) |
| Failures | One table failing shouldn't block others |

---

### Scenario 2: "The table has grown to 500M rows. The sync is too slow."

**What they're testing:** Performance optimization, Spark partitioning knowledge.

**Answer:**
> "Three things I'd optimize:
>
> 1. **Spark JDBC partitioning** — rather than reading with a single query, use:
>    ```python
>    .option("partitionColumn", "created_at")
>    .option("lowerBound", min)
>    .option("upperBound", max)
>    .option("numPartitions", 20)
>    ```
>    This splits the JDBC read into parallel queries, each handling a range of IDs or timestamps.
>
> 2. **Incremental is already efficient** — the watermark ensures we never re-read old data. Only new/changed rows are processed. If 500M rows are historical and never change, only the daily delta (~10K rows) is touched.
>
> 3. **ClickHouse native connector** — JDBC converts every row to SQL INSERT. The native connector uses ClickHouse's block format, which is 5-10x faster for bulk writes."

**Impact analysis:**
| Factor | Change |
|--------|--------|
| Performance | Partitioning = parallel reads. Native = faster writes. |
| Complexity | Higher — need to tune partition boundaries, number of partitions |
| Network | More connections to both databases, but lower per-connection data |

---

### Scenario 3: "We need sub-minute latency now."

**What they're testing:** Can you evolve from batch to streaming? Do you understand the tradeoffs?

**Answer:**
> "Batch won't work for sub-minute latency. I'd migrate to **Structured Streaming with Debezium CDC**:
>
> 1. Deploy Debezium PostgreSQL connector — reads the Write-Ahead Log (WAL)
> 2. Streams changes into Kafka topic
> 3. Spark reads from Kafka via `readStream`
> 4. Writes to ClickHouse via `foreachBatch()`
>
> The ClickHouse table and DDL **stay the same** — `ReplacingMergeTree` already handles upserts. The Spark code changes completely, but the data model doesn't. This is a good migration path: run batch and streaming in parallel during the transition, then decommission the batch job."

**Impact analysis:**
| Factor | Change |
|--------|--------|
| Infrastructure | Add Kafka + Debezium + Schema Registry |
| Code | Completely rewrite Spark job (batch → stream) |
| DDL | No change. ReplacingMergeTree handles both modes. |
| Operations | Significantly more complex — Kafka cluster to maintain |
| Latency | Seconds instead of 30 minutes |

---

### Scenario 4: "We started hard-deleting rows in PostgreSQL."

**What they're testing:** How do you handle a fundamental limitation of your solution?

**Answer:**
> "The current solution doesn't capture hard deletes. Options:
>
> **Option A (easiest):** Switch to soft deletes. Instead of `DELETE`, set `is_deleted = 1`. The watermark picks it up, ClickHouse updates the row. Queries add `WHERE is_deleted = 0`.
>
> **Option B (if soft-deletes aren't possible):** Add a **reconciliation job**. Once a day, compare distinct IDs between PostgreSQL and ClickHouse. Any ID in ClickHouse but not in PostgreSQL → delete from ClickHouse via `ALTER TABLE DELETE`.
>
> **Option C (real-time):** CDC with Debezium captures the delete event natively. But this requires full streaming infrastructure.
>
> **Recommendation:** Start with Option A (it's already built into the schema — the `is_deleted` column exists). If the business requires hard deletes, add Option B as a nightly job."

---

### Scenario 5: "The business team added a new column to the PostgreSQL table."

**What they're testing:** Schema evolution handling.

**Answer:**
> "The current solution requires **manual DDL update** — I'd add the column to `clickhouse_ddl.sql` and run it. The Spark query uses `SELECT *` so it automatically picks up the new column. However, `SELECT *` is risky in production — the column order might differ between databases.
>
> **Better approach for production:**
> 1. Replace `SELECT *` with explicit column list in the Spark query
> 2. For the ClickHouse side, use `ALTER TABLE ADD COLUMN` instead of `CREATE TABLE`
> 3. Ideally, automate this: detect schema changes by comparing `information_schema.columns` from PostgreSQL with `system.columns` in ClickHouse, and issue `ALTER TABLE` automatically."

**Impact analysis:**
| Factor | Change |
|--------|--------|
| DDL | Manual: `ALTER TABLE ... ADD COLUMN` on CH |
| Spark query | Change `SELECT *` to explicit column list |
| Automation | Future: schema diff + auto-alter |

---

### Scenario 6: "ClickHouse is down. What happens to the sync?"

**What they're testing:** Failure isolation, system design thinking.

**Answer:**
> "The watermark is read from ClickHouse. If ClickHouse is down:
> 1. `get_watermark()` catches the exception → logs warning → returns `0`
> 2. `read_new_records()` with watermark `0` reads **all rows** from PostgreSQL
> 3. `write_to_clickhouse()` fails → entire job fails → exit code 1
> 4. Monitoring alerts that the job failed
> 5. When ClickHouse comes back, next run reads watermark from ClickHouse (max of whatever was persisted), fetches only new/changed rows
>
> **Risk:** If ClickHouse was down for a long time and the `max(updated_at)` was lost (e.g., table dropped), the watermark resets to 0 and we re-sync everything. This is acceptable for a fact table with 16 rows, but for 500M rows, you'd want an **external watermark store** (see Alternative 3) that survives ClickHouse outages."

---

### Scenario 7: "PostgreSQL is down. What happens?"

**What they're testing:** Same as above but for the source database.

**Answer:**
> "If PostgreSQL is down when the Spark job runs:
> 1. `get_watermark()` on ClickHouse succeeds — returns current watermark
> 2. `read_new_records()` tries to connect to PostgreSQL → **connection refused**
> 3. Spark JDBC throws exception → caught by `main()` try/except
> 4. Job logs error and exits with code 1
> 5. Monitoring alerts
> 6. Next run: when PostgreSQL is back, the watermark is unchanged (ClickHouse was never written to), so the job re-reads all rows that were missed
>
> **No data loss.** The watermark didn't change, so any new rows inserted during the outage are picked up. The only cost is that analytics queries return slightly stale data during the outage."

---

### Scenario 8: "We need to backfill 3 years of historical data."

**What they're testing:** Can you handle initial/retrospective loads?

**Answer:**
> "Batch with watermark makes backfill **trivial**:
>
> ```bash
> # Set the watermark back to 0 to re-sync everything
> # Option A: Truncate ClickHouse table and re-run
> clickhouse-client --query "TRUNCATE TABLE default.app_user_visits_fact"
> ./run.sh  # watermark = 0 → full re-sync
>
> # Option B: If we don't want to truncate (e.g., can't lose existing data),
> # just re-set the watermark manually
> clickhouse-client --query "
>   INSERT INTO sync_watermarks VALUES ('app_user_visits_fact', 0)
> "
> ```
>
> The next run picks up **every row** from PostgreSQL. The `ReplacingMergeTree` deduplicates by `id`, keeping the latest version. No special tooling needed.
>
> **For very large backfills (billions of rows):** I'd batch the backfill manually — iterate through date ranges (e.g., 1 month at a time) to avoid overwhelming ClickHouse with a single massive write. Each month is a separate Spark job run."

---

### Scenario 9: "We want to filter data — only sync rows for specific store_ids."

**What they're testing:** Can you extend the solution with custom logic?

**Answer:**
> "Add a filter to the PostgreSQL query in `read_new_records()`:
>
> ```python
> def read_new_records(spark, watermark):
>     where = (
>         f"(updated_at > {watermark} "
>         f"OR (updated_at IS NULL AND created_at > {watermark}))"
>         f" AND store_id IN ('store_1', 'store_2')"
>     )
> ```
>
> Or make the filter configurable via environment variable:
> ```python
> STORE_FILTER = os.getenv("STORE_FILTER", "")
> # ... append to WHERE clause if set
> ```
>
> **Impact on ClickHouse:** The table still has all columns. No DDL change. Just less data.
>
> **Watch out for:** If you filter in the Spark query, the `max(updated_at)` watermark includes ALL stores, but you only sync some. The watermark might advance past rows in the filtered stores that were updated, causing them to be missed if the filter is later removed. **Solution:** Use separate watermarks per store, or don't filter at the query level — read all, filter in Spark after reading."

---

### Scenario 10: "We're migrating from on-prem to cloud. How do you handle this?"

**What they're testing:** Operational maturity, migration planning.

**Answer:**
> "For a migration, I'd use a **dual-write strategy**:
>
> 1. **Before migration:** Run `SELECT max(updated_at)` from the old ClickHouse
> 2. **Create new ClickHouse table** in the cloud with the same DDL
> 3. **Backfill:** Set the old watermark as the starting point. First run syncs everything up to that point
> 4. **Run in parallel:** For a week, run the sync job to **both** old and new ClickHouse
> 5. **Validate:** Compare row counts and checksums between old and new
> 6. **Switch:** Point analytics queries to the new ClickHouse. Decommission the old one
>
> The Spark job doesn't change — just update the `CH_URL` environment variable (or point two parallel jobs)."

---

### Scenario 11: "What if a row is updated at the exact same millisecond as the watermark?"

**What they're testing:** Edge case awareness, understanding of `>` vs `>=`.

**Answer:**
> "The query uses `>` not `>=`:
> ```
> WHERE updated_at > watermark
> ```
> If a row has `updated_at == watermark`, it would be **missed**. In practice, the watermark is the `max(updated_at)` from the PREVIOUS sync, and new rows/updates always have `updated_at >= NOW()` which is > the old watermark.
>
> But there's an edge case: if two rows are updated at the EXACT same timestamp, and only one is synced in a batch, the other might have `updated_at == new_watermark`. On the next run, it would be missed.
>
> **Fix:** Use `>=` instead of `>`:
> ```
> WHERE updated_at >= watermark
> ```
> This slightly increases the chance of re-reading already-synced rows, but `ReplacingMergeTree` deduplicates them. I didn't use `>=` because it means every run re-reads the row with the exact watermark value — a tradeoff between minor duplicate work vs. never missing a row."

---

### Scenario 12: "An intern accidentally ran a bulk UPDATE that touched 1M rows."

**What they're testing:** How the system handles large wave of updates.

**Answer:**
> "This is actually a **realistic scenario** in production. Here's what happens:
>
> 1. Bulk UPDATE sets `updated_at = NOW()` on 1M rows
> 2. Next Spark run: watermark is old, so all 1M rows qualify
> 3. Spark reads 1M rows from PostgreSQL
> 4. Writes 1M rows to ClickHouse
> 5. Job takes longer than usual but completes successfully
> 6. Next run: watermark is now `NOW()`, only new changes are picked up
>
> **No data corruption.** The watermark correctly captures the new max timestamp. `ReplacingMergeTree` handles any deduplication. The only impact is a longer sync cycle for that one run.
>
> **If this causes performance issues:** Add a rate limiter or batch the writes in smaller chunks to avoid overwhelming ClickHouse (e.g., write 100K rows per batch instead of all 1M at once)."

---

### Scenario 13: "How would you make this work with different timezones/servers?"

**What they're testing:** Distributed systems awareness.

**Answer:**
> "The `created_at` and `updated_at` values are **epoch milliseconds** — they're timezone-independent integers. There's no timezone conversion happening anywhere in the pipeline:
> - PostgreSQL stores them as `int8`
> - Spark reads them as `LongType`
> - ClickHouse stores them as `Int64`
>
> The comparison `updated_at > watermark` is a simple integer comparison. **Clock skew between servers** is the real risk: if the ClickHouse server's clock is 5 minutes ahead of PostgreSQL, a row with `updated_at = CH_clock_now - 1min` might not be synced because the watermark was set from a "future" timestamp.
>
> **Fix:** All servers should use the same NTP time source. In extreme cases, add a small buffer (e.g., subtract 60 seconds from the watermark before comparison) to ensure no rows are missed."

---

### Scenario 14: "The sync runs every 30 minutes but we need to reduce to 5 minutes."

**What they're testing:** Can you change the cadence without breaking things?

**Answer:**
> "This is trivially simple — change the cron expression:
> ```cron
> */5 * * * * cd /path && ./run.sh
> ```
>
> **No code changes needed.** The watermark mechanism is stateless — each run reads the current max from ClickHouse, regardless of when the last run happened. The only concern is:
> - **Same row synced twice** if watermark granularity isn't fine enough — handled by `ReplacingMergeTree`
> - **More load on both databases** — but since each run only processes the delta (rows changed since last run), the per-run load stays roughly the same. The total daily load (288 runs vs 48 runs) increases, but each run is small"

---

### Scenario 15: "How would you add monitoring alerts for when the sync fails?"

**What they're testing:** Operational readiness beyond writing code.

**Answer:**
> "The Spark job already returns non-zero exit code on failure. I'd add:
>
> 1. **Email/Slack alert** on exit code != 0, via a wrapper script:
>    ```bash
>    ./sync_visits.py || curl -X POST -H 'Content-type: application/json' \
>      --data '{"text":"Sync failed! Check logs."}' \
>      $SLACK_WEBHOOK_URL
>    ```
>
> 2. **Data freshness check** — a separate monitoring query that runs every 15 minutes:
>    ```sql
>    -- If this returns a row, no data has arrived in the last 60 minutes
>    SELECT 'ALERT: No recent data sync!'
>    FROM app_user_visits_fact
>    HAVING max(updated_at) < now() - 3600000
>    ```
>
> 3. **Row count comparison** — compare row counts between PG and CH periodically:
>    ```sql
>    -- PG side
>    SELECT count(*) FROM app_user_visits_fact;
>    -- CH side
>    SELECT count() FROM app_user_visits_fact FINAL;
>    ```
>    If the difference exceeds a threshold, alert."

---

### Quick Reference: Scenario Summary

| Scenario | Core change | Difficulty |
|----------|-------------|------------|
| Sync 20 tables | Generic framework with schema detection | Medium |
| 500M rows | Spark partitioning + native connector | Medium |
| Sub-minute latency | Switch to CDC / streaming | High |
| Hard deletes | Soft-delete flag or reconciliation job | Low |
| New column | Manual ALTER TABLE + explicit column list | Low |
| CH is down | Watermark from CH fails → retry later | Low risk |
| PG is down | Job fails, no data loss, resumes later | Low risk |
| Backfill 3 years | Set watermark = 0, re-run | Low |
| Filter by store | Add WHERE clause to PG query | Low |
| Cloud migration | Dual-write + parallel runs | Medium |
| Same-timestamp edge case | Use `>=` instead of `>` | Low |
| Bulk UPDATE 1M rows | Works naturally, just slower | Low |
| Clock skew | NTP sync or watermark buffer | Low |
| Change frequency | Update cron expression only | Trivial |
| Monitoring alerts | Slack/webhook on failure + freshness check | Low |
