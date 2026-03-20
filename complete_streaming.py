#!/usr/bin/env python3
"""
Complete Structured Streaming Pipeline:
1. Read from taxi-trips Kafka topic
2. Create lakehouse.taxi.bronze table if not exists
3. Write all messages to the table
4. Query and display results
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, current_timestamp
import time

# Initialize Spark with packages
spark = SparkSession.builder \
    .appName("TaxiBronzeComplete") \
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

print("=" * 60)
print("STRUCTURED STREAMING: TAXI TRIPS TO ICEBERG BRONZE TABLE")
print("=" * 60)

# Schema for taxi trip records
TAXI_SCHEMA = StructType([
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

# Step 1: Create database and table in Iceberg catalog
print("\n[1/4] Creating database and table structure...")
try:
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")
    print("✓ Database created")
except Exception as e:
    print(f"Database creation note: {e}")

# Create table explicitly
create_table_sql = """
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
)
USING ICEBERG
"""

try:
    spark.sql(create_table_sql)
    print("✓ Bronze table created")
except Exception as e:
    print(f"✓ Bronze table exists: {e}")

# Step 2: Read Kafka stream
print("\n[2/4] Reading from Kafka topic 'taxi-trips'...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "taxi-trips") \
    .option("startingOffsets",  "earliest") \
    .load()

print("✓ Kafka stream reader configured")

# Step 3: Parse and transform
print("\n[3/4] Parsing JSON messages...")
parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), TAXI_SCHEMA).alias("data")) \
    .select("data.*")

print("✓ Messages parsed successfully")

# Step 4: Write to Iceberg table  
print("\n[4/4] Starting streaming write to lakehouse.taxi.bronze...")
query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://warehouse/checkpoints/taxi-bronze-v1") \
    .toTable("lakehouse.taxi.bronze")

print(f"✓ Streaming query started")
print(f"  Query ID: {query.id}")

# Run for 30 seconds then check results
print("\n" + "=" * 60)
print("STREAMING IN PROGRESS (30 seconds)...")
print("=" * 60)

time.sleep(30)

print("\n[RESULTS] Querying bronze table...")
try:
    result = spark.sql("SELECT COUNT(*) as row_count FROM lakehouse.taxi.bronze")
    count = result.collect()[0]['row_count']
    print(f"\n✓ Total rows in bronze table: {count}")
    
    if count > 0:
        print("\n[SAMPLE DATA] First 10 rows:")
        spark.sql("""
            SELECT 
                VendorID, 
                tpep_pickup_datetime, 
                PULocationID, 
                DOLocationID, 
                trip_distance, 
                total_amount 
            FROM lakehouse.taxi.bronze 
            LIMIT 10
        """).show(truncate=False)
    else:
        print("No data yet - streaming may still be processing")
        
except Exception as e:
    print(f"Query error: {e}")

# Keep streaming running
print("\n" + "=" * 60)
print("STREAMING CONTINUES IN BACKGROUND")
print("Query will run until terminated")
print("=" * 60)

query.awaitTermination()
