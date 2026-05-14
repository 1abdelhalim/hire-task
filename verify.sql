-- Verify data was synced into ClickHouse
-- Run with: clickhouse-client --multiquery < verify.sql

SELECT '--- Row count ---' AS info;
SELECT count() AS total_rows FROM default.app_user_visits_fact FINAL;

SELECT '--- Distinct IDs (should match previous count) ---' AS info;
SELECT uniqExact(id) AS distinct_ids FROM default.app_user_visits_fact FINAL;

SELECT '--- Sample rows ---' AS info;
SELECT id, phone_number, points, receipt, remaining, state, created_at, updated_at
FROM default.app_user_visits_fact FINAL
ORDER BY created_at
LIMIT 10;

SELECT '--- Points by phone (high to low) ---' AS info;
SELECT phone_number, count() AS visits, sum(points) AS total_points
FROM default.app_user_visits_fact FINAL
GROUP BY phone_number
ORDER BY total_points DESC
LIMIT 10;

SELECT '--- Duplicate check: IDs that appear more than once ---' AS info;
SELECT id, count() AS versions
FROM default.app_user_visits_fact
GROUP BY id
HAVING versions > 1
ORDER BY versions DESC;
