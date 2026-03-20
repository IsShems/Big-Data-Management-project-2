#!/usr/bin/env python3
"""
Minimal Structured Streaming query that:
1. Reads taxi trips from Kafka topic 'taxi-trips'
2. Writes to Iceberg table 'lakehouse.taxi.bronze'
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark Session with Iceberg and Kafka support
spark = SparkSession.builder \
    .appName("TaxiTripsStreaming") \
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

# Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "taxi-trips") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON value from Kafka
from pyspark.sql.functions import from_json, col

schema = StructType([
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

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Write to Iceberg table in append mode
query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://warehouse/checkpoints/taxi-bronze") \
    .toTable("lakehouse.taxi.bronze")

print("Streaming query started. Waiting for data...")
print("Query ID:", query.id)
print("Status:", query.status)

# Keep the stream running
query.awaitTermination()
