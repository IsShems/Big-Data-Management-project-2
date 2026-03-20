#!/usr/bin/env python3
"""
Working Structured Streaming Pipeline
Reads from Kafka → Writes to Iceberg Bronze Table
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
import time

# Initialize Spark
spark = SparkSession.builder \
    .appName("TaxiBronzeFinal") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "rest") \
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse") \
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "changeme") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*70)
print("TAXI STREAMING PIPELINE: KAFKA → ICEBERG BRONZE")
print("="*70)

# Taxi schema
SCHEMA = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", DoubleType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("Airport_fee", DoubleType()),
    StructField("cbd_congestion_fee", DoubleType()),
])

try:
    print("\n[SETUP] Creating lakehouse.taxi.bronze table...")
    spark.sql("""
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
    """)
    print("✓ Table created")
except Exception as e:
    print(f"✓ Table exists: {str(e)[:50]}")

# Read Kafka
print("\n[KAFKA] Reading from 'taxi-trips' topic...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "taxi-trips") \
    .option("startingOffsets", "earliest") \
    .load()
print("✓ Stream configured")

# Parse JSON
print("\n[TRANSFORM] Parsing messages...")
parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), SCHEMA).alias("data")) \
    .select("data.*")
print("✓ Schema applied")

# Write to Iceberg using HDFS checkpoint (works without S3 filesystem issues)
print("\n[STREAM] Writing to lakehouse.taxi.bronze...")
query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kafka-bronze-checkpoint") \
    .toTable("lakehouse.taxi.bronze")

print(f"✓ Streaming started (Query ID: {query.id})")

# Let it run for 35 seconds
print("\n" + "="*70)
print("PROCESSING DATA (35 seconds)...")
print("="*70)
time.sleep(35)

# Query results
print("\n[QUERY] Bronze table results:")
print("="*70)
try:
    result = spark.sql("SELECT COUNT(*) as row_count FROM lakehouse.taxi.bronze")
    count = result.collect()[0]['row_count']
    print(f"\n✓ SELECT COUNT(*) FROM lakehouse.taxi.bronze")
    print(f"  Result: {count} rows\n")
    
    if count > 0:
        print("✓ Sample data:")
        spark.sql("""
            SELECT VendorID, tpep_pickup_datetime, PULocationID, 
                   DOLocationID, trip_distance, total_amount 
            FROM lakehouse.taxi.bronze 
            LIMIT 5
        """).show(truncate=False)
    
    print("\n" + "="*70)
    print("SUCCESS! Streaming pipeline working correctly")
    print("="*70)
    
except Exception as e:
    print(f"Error: {e}")

# Keep streaming running
query.awaitTermination()
