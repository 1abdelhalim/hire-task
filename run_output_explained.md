# Run Output — `./run.sh --reset` — Explained

This file shows the expected terminal output when running the full demo from scratch,
with explanations for each section. Spark's internal INFO/WARN logs are omitted for clarity.

---

## Step 1: Reset and Start Databases

```
=== Resetting all Docker data ===
 Container hiringtask-clickhouse-1  ... Removed
 Container hiringtask-postgres-1     ... Removed
 Network hiringtask_default          ... Removed
=== Starting PostgreSQL & ClickHouse ===
 Network hiringtask_default  Created
 Container hiringtask-postgres-1    Started
 Container hiringtask-clickhouse-1  Started
Waiting for PostgreSQL...
PostgreSQL ready
Waiting for ClickHouse...
ClickHouse ready
```

**What happened:**
- `--reset` ran `docker compose down -v` — destroyed old containers + volumes
- `docker compose up -d` — started fresh PostgreSQL and ClickHouse
- Init scripts ran automatically:
  - PostgreSQL: `init-postgres.sql` created the table and inserted 16 sample rows
  - ClickHouse: `clickhouse_ddl.sql` created the `ReplacingMergeTree` table

---

## Step 2: Verify Tables Exist

```
=== Verifying tables ===
                List of relations
 Schema |         Name         | Type  |  Owner
--------+----------------------+-------+----------
 public | app_user_visits_fact | table | postgres
(1 row)

id              String
phone_number    Nullable(String)
seen            Nullable(Int32)
state           Nullable(Int32)
points          Nullable(Float64)
receipt         Nullable(Float64)
countryCode     Nullable(String)
remaining       Nullable(Float64)
customer_id     String
branch_id       String
store_id        String
cashier_id      String
created_at      Nullable(Int64)
updated_at      Int64    DEFAULT 0
expired         Nullable(Int32)
expires_at      Nullable(Int64)
order_id        Nullable(String)
is_deleted      Int16    DEFAULT 0
is_fraud        Int16    DEFAULT 0
sync_mechanism  Nullable(String)
is_bulk_points  Nullable(String)
```

**What happened:**
- Verified PostgreSQL has the `app_user_visits_fact` table with 21 columns
- Verified ClickHouse has the matching table with `ReplacingMergeTree` engine
- Note `updated_at` is `Int64 DEFAULT 0` (non-nullable) — this is required because `ReplacingMergeTree` version column cannot be `Nullable`

---

## Step 3: First Spark Sync (Full Load)

```
2026-05-14 18:41:23,305 [INFO] Watermark set to 0
2026-05-14 18:41:24,373 [INFO] Records to sync: 16
2026-05-14 18:41:24,668 [INFO] Sync completed — 16 records written
```

**What happened:**
| Log line | Meaning |
|----------|---------|
| `Watermark set to 0` | ClickHouse was empty → query failed → caught by `get_watermark()` → returns 0 |
| `Records to sync: 16` | `read_new_records()` queried PG with `WHERE updated_at > 0 OR (updated_at IS NULL AND created_at > 0)` — returned all 16 sample rows |
| `Sync completed — 16 records written` | `write_to_clickhouse()` appended all 16 rows via JDBC. `ReplacingMergeTree` stored them sorted by `id` |

---

## Step 4: Verify Data in ClickHouse

```
=== Verifying data in ClickHouse ===
=== Sample rows ===
0552377277  13  13  2  1753583922000
0534923005  1   1   2  1753584115000
0502499988  9   0   2  1753584471000
0550759954  41  41  2  1753584614000
0567224428  9   9   2  1753584843000
0534412326  12  12  2  1753584939405
0550693380  11  0   2  1758628843010
0550693380  9   0   2  1758736479787
0550693380  11  0   2  1758914040646
0550693380  7   2   2  1759173190174
```

**What happened:**
- Queried ClickHouse with `FINAL` to get deduplicated state
- Displayed 10 sample rows (phone, points, remaining, state, created_at)
- Note `0550693380` appears 4 times — these are different visits (different `id` values) by the same customer at different timestamps. Not duplicates — they have different `created_at` and `id` values

---

## Step 5: Demo — Update a Row in PostgreSQL

```
Updating points for 0550693380 to 999 in PostgreSQL...
```

**SQL executed:**
```sql
UPDATE app_user_visits_fact
SET points = 999, updated_at = 1800000000000
WHERE phone_number = '0550693380' AND id = '92128927-eaf7-4e31-9269-7f8c38e4d1cc';
```

**What happened:**
- Simulates a business operation: customer earned bonus points
- Changes `points` from `11.0` to `999.0`
- Sets `updated_at` to a far-future timestamp (`1800000000000`) so the watermark will definitely pick it up

---

## Step 6: Second Spark Sync (Incremental)

```
2026-05-14 18:41:31,751 [INFO] Watermark set to 1759356318000
2026-05-14 18:41:32,690 [INFO] Records to sync: 1
2026-05-14 18:41:32,840 [INFO] Sync completed — 1 records written
```

**What happened:**
| Log line | Meaning |
|----------|---------|
| `Watermark set to 1759356318000` | `get_watermark()` read `max(updated_at)` from ClickHouse — this is the highest timestamp from the first sync |
| `Records to sync: 1` | Only the updated row qualified (`updated_at = 1800000000000 > 1759356318000`) |
| `Sync completed — 1 records written` | Only 1 row was written to ClickHouse — **incremental sync works** |

**Key demonstration:** 15 rows were filtered out by the watermark. Only the 1 changed row was re-read and re-inserted.

---

## Step 7: Final Verification

```
=== Verifying updated row in ClickHouse ===
92128927-eaf7-4e31-9269-7f8c38e4d1cc  0550693380  999  1800000000000
89ea0a7a-52ef-4290-b681-c4463db4d35c  0550693380  7    1759341646839
5bfb3f5d-4160-49e2-9e20-0d4b64786537  0550693380  11   1759341646653
```

**What happened:**
- Query: `SELECT id, phone_number, points, updated_at FROM app_user_visits_fact FINAL WHERE phone_number = '0550693380' ORDER BY updated_at DESC LIMIT 3`
- Shows 3 rows for customer `0550693380`:
  1. **points = 999, updated_at = 1800000000000** ← the updated row ✅
  2. points = 7, updated_at = 1759341646839 ← older visit
  3. points = 11, updated_at = 1759341646653 ← oldest visit
- `ReplacingMergeTree` kept the row with highest `updated_at` for each `id`
- The original row (`92128927-...`) now shows `points = 999` instead of `11`

---

## Step 8: Done

```
╔══════════════════════════════════════════════════════╗
║  ALL DONE ✅                                        ║
║                                                     ║
║  Run 'docker compose down' to stop databases.       ║
║  Run './run.sh --reset' to wipe and restart.        ║
╚══════════════════════════════════════════════════════╝
```

---

## Summary of What Was Proven

| # | Test | Result |
|---|------|--------|
| 1 | Full load: 16 rows from PG → CH on first run | ✅ |
| 2 | Watermark: set to `0` initially, then advances | ✅ |
| 3 | NULL `updated_at` handling: `OR created_at > watermark` fallback | ✅ (implicit) |
| 4 | Incremental sync: only 1 row on second run (not 16) | ✅ |
| 5 | Update propagation: points changed from 11 to 999 | ✅ |
| 6 | ReplacingMergeTree dedup: kept latest version by `updated_at` | ✅ |
| 7 | Clean shutdown: Spark stops without errors | ✅ |
