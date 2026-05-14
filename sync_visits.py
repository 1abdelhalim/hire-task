#!/usr/bin/env python3
"""
Spark batch job: sync app_user_visits_fact from PostgreSQL to ClickHouse.
Runs every 30 minutes via cron / scheduler.

Usage:
    spark-submit \
        --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5 \
        sync_visits.py
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("sync_visits")

PG_URL = os.getenv("PG_URL", "jdbc:postgresql://localhost:5432/mydb")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

CH_URL = os.getenv("CH_URL", "jdbc:clickhouse://localhost:8123/default")
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASS", "")

PG_TABLE = "app_user_visits_fact"
CH_TABLE = "app_user_visits_fact"


def get_watermark(spark):
    """Return the max updated_at from ClickHouse as the high-water mark."""
    try:
        query = f"SELECT coalesce(max(updated_at), 0) AS watermark FROM {CH_TABLE}"
        df = (
            spark.read.format("jdbc")
            .option("url", CH_URL)
            .option("user", CH_USER)
            .option("password", CH_PASS)
            .option("query", query)
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .load()
        )
        return df.collect()[0][0] or 0
    except Exception as e:
        logger.warning("ClickHouse table empty or unreachable, starting from 0: %s", e)
        return 0


def read_new_records(spark, watermark):
    """Read records from PostgreSQL where updated_at > watermark."""
    where = (
        f"updated_at > {watermark} "
        f"OR (updated_at IS NULL AND created_at > {watermark})"
    )
    df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("dbtable", f"(SELECT * FROM {PG_TABLE} WHERE {where}) AS sub")
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 5000)
        .load()
    )
    df = df.withColumn("updated_at", coalesce(col("updated_at"), lit(0)))
    return df


def write_to_clickhouse(df):
    """Append DataFrame to the ClickHouse table."""
    (
        df.write.format("jdbc")
        .option("url", CH_URL)
        .option("user", CH_USER)
        .option("password", CH_PASS)
        .option("dbtable", CH_TABLE)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", 5000)
        .mode("append")
        .save()
    )


def main():
    spark = (
        SparkSession.builder.appName("pg_to_clickhouse_sync")
        .config(
            "spark.jdbc.fetchsize", "5000"
        )
        .getOrCreate()
    )

    try:
        watermark = get_watermark(spark)
        logger.info("Watermark set to %s", watermark)

        df = read_new_records(spark, watermark)
        count = df.count()
        logger.info("Records to sync: %s", count)

        if count > 0:
            write_to_clickhouse(df)
            logger.info("Sync completed — %s records written", count)
        else:
            logger.info("No new records to sync")

    except Exception as e:
        logger.error("Sync failed: %s", e)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
