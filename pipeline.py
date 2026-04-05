#!/usr/bin/env python3
"""
End-to-end medallion pipeline for Project 2 + trip-speed scenario.

Jupyter: cd ~/project (repo root — there is no project/ subfolder) then:
  python produce.py
  python pipeline.py bronze | silver-gold | stats

Host docker exec (Conda Python):
  docker exec -it project2_jupyter /opt/conda/bin/python /home/jovyan/project/pipeline.py …
  (plain `docker exec … python` may miss Conda → "No module named 'pyspark'".)

Bronze uses a fixed checkpoint path so restarts do not duplicate rows.
"""

from __future__ import annotations

import argparse
import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse_spark import build_spark_session

# Same layout as produce.py / existing scripts
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
ZONES_PARQUET = os.path.join(DATA_DIR, "taxi_zone_lookup.parquet")

# Durable inside the container; survives until the container is removed.
BRONZE_CHECKPOINT = "/tmp/iceberg-checkpoints/taxi-bronze"

BOOTSTRAP = "kafka:9092"
TOPIC = "taxi-trips"

# Parsed JSON schema (matches yellow taxi parquet / produce.py)
TAXI_JSON_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
        StructField("cbd_congestion_fee", DoubleType(), True),
    ]
)


def _ensure_zones_path() -> None:
    if not os.path.isfile(ZONES_PARQUET):
        sys.exit(
            f"Missing zone lookup parquet: {ZONES_PARQUET}\n"
            "Copy taxi_zone_lookup.parquet into data/ as described in README."
        )


def cmd_bronze(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.bronze (
            VendorID INT,
            tpep_pickup_datetime STRING,
            tpep_dropoff_datetime STRING,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            RatecodeID DOUBLE,
            store_and_fwd_flag STRING,
            PULocationID INT,
            DOLocationID INT,
            payment_type INT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            Airport_fee DOUBLE,
            cbd_congestion_fee DOUBLE
        ) USING ICEBERG
        """
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"), TAXI_JSON_SCHEMA).alias("data")
    ).select("data.*")

    print("Bronze streaming query starting…")
    print(f"  checkpoint: {BRONZE_CHECKPOINT}")
    print("  trigger: processingTime 10 seconds (micro-batches)")
    print("  outputMode: append")
    print("Stop with Ctrl+C when the producer has finished or you have enough rows.\n")

    query = (
        parsed.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .toTable("lakehouse.taxi.bronze")
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping bronze query…")
        query.stop()
        query.awaitTermination()


def build_silver_dataframe(spark: SparkSession):
    _ensure_zones_path()

    bronze = spark.table("lakehouse.taxi.bronze")

    pickup_ts = F.to_timestamp(F.col("tpep_pickup_datetime"))
    dropoff_ts = F.to_timestamp(F.col("tpep_dropoff_datetime"))

    trip_duration_minutes = (
        F.unix_timestamp(dropoff_ts) - F.unix_timestamp(pickup_ts)
    ) / F.lit(60.0)

    dist = F.col("trip_distance")
    invalid_speed = (
        trip_duration_minutes.isNull()
        | dist.isNull()
        | (trip_duration_minutes <= F.lit(0.0))
        | (dist <= F.lit(0.0))
    )

    trip_speed_kmh = F.when(invalid_speed, F.lit(None).cast("double")).otherwise(
        dist / (trip_duration_minutes / F.lit(60.0))
    )

    zones = spark.read.parquet(ZONES_PARQUET)
    pu = zones.select(
        F.col("LocationID").alias("_pu_id"),
        F.col("Zone").alias("pickup_zone"),
    )
    do = zones.select(
        F.col("LocationID").alias("_do_id"),
        F.col("Zone").alias("dropoff_zone"),
    )

    joined = (
        bronze.join(pu, F.col("PULocationID") == F.col("_pu_id"), "left")
        .drop("_pu_id")
        .join(do, F.col("DOLocationID") == F.col("_do_id"), "left")
        .drop("_do_id")
    )

    silver = (
        joined.withColumn("pickup_ts", pickup_ts)
        .withColumn("dropoff_ts", dropoff_ts)
        .withColumn("trip_duration_minutes", trip_duration_minutes)
        .withColumn("trip_speed_kmh", trip_speed_kmh)
        .filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
        .filter(F.col("dropoff_ts") >= F.col("pickup_ts"))
        .withColumn(
            "passenger_count",
            F.col("passenger_count").cast("int"),
        )
        .dropDuplicates(
            [
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "trip_distance",
                "fare_amount",
            ]
        )
    )

    out = silver.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("pickup_ts").cast(TimestampType()).alias("pickup_datetime"),
        F.col("dropoff_ts").cast(TimestampType()).alias("dropoff_datetime"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("pickup_zone"),
        F.col("dropoff_zone"),
        F.col("trip_duration_minutes").cast("double").alias("trip_duration_minutes"),
        F.col("trip_speed_kmh").cast("double").alias("trip_speed_kmh"),
    )

    return out


def cmd_silver_gold(spark: SparkSession) -> None:
    _ensure_zones_path()
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

    try:
        bronze_count = (
            spark.sql("SELECT COUNT(*) AS c FROM lakehouse.taxi.bronze").collect()[0]["c"]
        )
    except Exception:
        sys.exit(
            "Bronze table not found or not readable. From ~/project run "
            "`python pipeline.py bronze` after Kafka topic creation and `python produce.py`."
        )
    if bronze_count == 0:
        sys.exit(
            "lakehouse.taxi.bronze is empty. Run the Kafka producer and "
            "`python pipeline.py bronze` first (from repo root ~/project)."
        )

    print(f"\n>>> Bronze rows read from Iceberg: {bronze_count}")
    print(">>> Building silver (parse times, zones, trip speed) — may take 1–2 minutes on first Spark startup…")
    silver_df = build_silver_dataframe(spark)
    silver_n = silver_df.count()
    print(f">>> Silver rows after clean/dedup: {silver_n}")

    print(">>> Writing lakehouse.taxi.silver to Iceberg…")
    try:
        silver_df.writeTo("lakehouse.taxi.silver").using("iceberg").createOrReplace()
    except Exception as exc:
        sys.exit(f"Silver Iceberg write failed: {exc}\n" "Fix the error above, then run silver-gold again.")

    print(">>> Silver table registered. Building gold aggregates…")
    gold_df = (
        spark.table("lakehouse.taxi.silver")
        .filter(F.col("pickup_zone").isNotNull())
        .filter(F.col("trip_speed_kmh").isNotNull())
        .groupBy("pickup_zone")
        .agg(
            F.avg("trip_speed_kmh").alias("avg_speed_kmh"),
            F.max("trip_speed_kmh").alias("max_speed_kmh"),
        )
    )

    print(">>> Writing lakehouse.taxi.gold (partitioned by pickup_zone)…")
    try:
        (
            gold_df.writeTo("lakehouse.taxi.gold")
            .using("iceberg")
            .partitionedBy(F.col("pickup_zone"))
            .createOrReplace()
        )
    except Exception as exc:
        sys.exit(f"Gold Iceberg write failed: {exc}\n" "Silver exists; fix gold write and re-run silver-gold.")

    print("\n>>> Done. Row counts:")
    spark.sql("SELECT COUNT(*) AS silver_rows FROM lakehouse.taxi.silver").show(
        truncate=False
    )
    spark.sql("SELECT COUNT(*) AS gold_rows FROM lakehouse.taxi.gold").show(
        truncate=False
    )
    print(">>> You can now run: python query_bronze.py \"SELECT COUNT(*) FROM lakehouse.taxi.silver\"\n")


def cmd_stats(spark: SparkSession) -> None:
    try:
        spark.sql("SELECT 1 FROM lakehouse.taxi.silver LIMIT 1").collect()
    except Exception:
        sys.exit(
            "Silver table missing. From ~/project run `python pipeline.py silver-gold` first."
        )

    print("=== Trip speed (silver, non-null) ===")
    spark.sql(
        """
        SELECT
          MAX(trip_speed_kmh) AS max_trip_speed_kmh,
          AVG(trip_speed_kmh) AS avg_trip_speed_kmh,
          SUM(CASE WHEN trip_speed_kmh IS NULL THEN 1 ELSE 0 END) AS null_speed_rows,
          COUNT(*) AS total_rows
        FROM lakehouse.taxi.silver
        """
    ).show(truncate=False)

    print("=== Gold sample (top 10 by max_speed_kmh) ===")
    try:
        spark.sql(
            """
            SELECT pickup_zone, avg_speed_kmh, max_speed_kmh
            FROM lakehouse.taxi.gold
            ORDER BY max_speed_kmh DESC
            LIMIT 10
            """
        ).show(truncate=False)
    except Exception as exc:
        print(f"(Could not read lakehouse.taxi.gold: {exc})")

    print("=== Iceberg snapshots: gold ===")
    try:
        spark.sql(
            """
            SELECT snapshot_id, committed_at, operation
            FROM lakehouse.taxi.gold.snapshots
            ORDER BY committed_at DESC
            LIMIT 5
            """
        ).show(truncate=False)
    except Exception as exc:
        print(f"(Could not read lakehouse.taxi.gold.snapshots: {exc})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Taxi lakehouse pipeline")
    parser.add_argument(
        "command",
        choices=["bronze", "silver-gold", "stats"],
        help="bronze: Kafka→Iceberg stream | silver-gold: batch refresh | stats: queries",
    )
    args = parser.parse_args()

    spark = build_spark_session("taxi-pipeline")

    if args.command == "bronze":
        cmd_bronze(spark)
    elif args.command == "silver-gold":
        cmd_silver_gold(spark)
    elif args.command == "stats":
        cmd_stats(spark)

    spark.stop()


if __name__ == "__main__":
    main()
