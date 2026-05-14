-- ══════════════════════════════════════════════════════════════════════════
-- ClickHouse DDL for app_user_visits_fact
--
-- INTERVIEW NOTES:
-- 1. This mirrors the PostgreSQL table schema for compatibility
-- 2. Uses ReplacingMergeTree (not plain MergeTree) to handle UPDATES
-- 3. The version column (updated_at) must be NON-NULLABLE — this is a
--    ClickHouse requirement that caught us during testing (Code: 169)
-- ══════════════════════════════════════════════════════════════════════════

-- IF NOT EXISTS: Safe to re-run. On first start, Docker auto-runs this script.
-- If the table already exists, this line is a no-op (doesn't overwrite data).
CREATE TABLE IF NOT EXISTS default.app_user_visits_fact
(
    -- ── Primary key ─────────────────────────────────────────────────────
    -- Maps to PostgreSQL: id text NOT NULL PRIMARY KEY
    id String,

    -- ── Nullable fields ─────────────────────────────────────────────────
    -- Nullable(String) allows NULL values (matches PostgreSQL's text NULL)
    -- Non-nullable: If PostgreSQL has NOT NULL, use String (without Nullable)
    phone_number Nullable(String),
    seen Nullable(Int32),       -- int4 in PG, Int32 in CH
    state Nullable(Int32),
    points Nullable(Float64),   -- float8 in PG, Float64 in CH
    receipt Nullable(Float64),

    -- countryCode is quoted in PostgreSQL because of the capital C:
    -- In CH, column names are case-sensitive, so "countryCode" stays lowercase
    countryCode Nullable(String),

    remaining Nullable(Float64),

    -- ── Non-nullable foreign key fields ─────────────────────────────────
    -- These reference other tables (branches, cashiers, app_users, stores)
    -- Not Nullable because PostgreSQL defines them as NOT NULL
    customer_id String,
    branch_id String,
    store_id String,
    cashier_id String,

    -- ── Timestamps (epoch milliseconds) ─────────────────────────────────
    -- Stored as Int64 (bigint). NOT DateTime because the values are
    -- raw epoch milliseconds from Java (System.currentTimeMillis()).
    -- Advantage: No timezone issues — pure integer comparison.
    created_at Nullable(Int64),

    -- ⚠️ TRICKY PART ⚠️
    -- updated_at is the VERSION COLUMN for ReplacingMergeTree.
    -- ClickHouse requires the version column to be NON-NULLABLE.
    -- In PostgreSQL, updated_at can be NULL (rows that were never updated).
    -- Fix: Use Int64 (not Nullable) with DEFAULT 0.
    -- The Spark code uses COALESCE(updated_at, 0) to handle NULLs from PG.
    -- DEFAULT 0: If a row has no updated_at, ClickHouse stores it as 0.
    updated_at Int64 DEFAULT 0,

    expired Nullable(Int32),
    expires_at Nullable(Int64),
    order_id Nullable(String),

    -- ── Flags ───────────────────────────────────────────────────────────
    -- Int16 maps to PostgreSQL's int2/smallint
    -- DEFAULT 0 means "not deleted" / "not fraud"
    is_deleted Int16 DEFAULT 0,
    is_fraud Int16 DEFAULT 0,

    sync_mechanism Nullable(String),
    is_bulk_points Nullable(String)
)
-- ══════════════════════════════════════════════════════════════════════════
-- ENGINE: ReplacingMergeTree (NOT plain MergeTree)
-- ══════════════════════════════════════════════════════════════════════════
-- Why ReplacingMergeTree?
--   PostgreSQL supports UPDATE — rows change over time. ClickHouse MergeTree
--   is append-only (it never modifies existing rows). If we just append every
--   change, the same id will appear multiple times.
--
--   ReplacingMergeTree solves this: during background merges, it compares rows
--   with the same ORDER BY key (id) and keeps only the row with the highest
--   version column (updated_at).
--
-- How to query:
--   SELECT ... FROM table FINAL   → deduplicated view (latest rows)
--   SELECT ... FROM table         → includes duplicates from unmerged parts
--
-- The version column (updated_at) as Int64:
--   Higher value = newer version. Timestamps are epoch milliseconds, so a
--   later timestamp is numerically larger → wins the dedup.
--
-- Interview question: "What about plain MergeTree?"
--   If you want a FULL HISTORY of every change, use plain MergeTree instead.
--   But queries become harder — every SELECT must use argMax() to get the
--   latest state. ReplacingMergeTree with FINAL is much simpler.
ENGINE = ReplacingMergeTree(updated_at)

-- ══════════════════════════════════════════════════════════════════════════
-- ORDER BY: Defines the sort order AND the deduplication key
-- ══════════════════════════════════════════════════════════════════════════
-- ReplacingMergeTree deduplicates rows that have the same ORDER BY columns.
-- Here, ORDER BY id means: "if two rows have the same id, keep the latest one."
--
-- Performance tip: For large tables, consider ordering by (store_id, created_at)
-- instead of just id. This makes queries that filter by store much faster.
--
-- Example alternative:
--   ORDER BY (store_id, created_at)
--   This would deduplicate by (...) pair, not globally by id.
--   Not correct for this use case since id is the true primary key.
ORDER BY id
-- ══════════════════════════════════════════════════════════════════════════
-- PARTITION BY: Speeds up queries by skipping irrelevant partitions
-- ══════════════════════════════════════════════════════════════════════════
-- Without PARTITION BY, ClickHouse scans the ENTIRE table for every query.
-- For large tables (100M+ rows), this is slow and expensive.
--
-- We partition by MONTH based on created_at (which is epoch milliseconds).
-- The formula:
--   1. intDiv(created_at, 1000)  → convert milliseconds to seconds
--   2. toDateTime(...)           → convert to ClickHouse DateTime
--   3. toYYYYMM(...)             → extract year+month (e.g., 202605)
--
-- This means:
--   - Queries with WHERE created_at IN (range) scan only relevant months
--   - Old partitions can be DROPPED for data retention
--   - Backfill only affects the target month's partition
--
-- Chosen over: PARTITION BY id (too many partitions, each too small)
-- Chosen over: PARTITION BY store_id (skew — some stores have more data)
PARTITION BY toYYYYMM(toDateTime(intDiv(created_at, 1000)));
