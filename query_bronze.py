#!/usr/bin/env python3
"""
Query the lakehouse.taxi.bronze table with Iceberg configuration
Usage: Pass SQL query as argument
  docker exec project2_jupyter python /home/jovyan/project/query_bronze.py "SELECT COUNT(*) FROM lakehouse.taxi.bronze"
  docker exec project2_jupyter python /home/jovyan/project/query_bronze.py "SELECT * FROM lakehouse.taxi.bronze LIMIT 3"
"""

import sys
from pyspark.sql import SparkSession

# Create Spark session with Iceberg configuration
spark = SparkSession.builder \
    .appName("QueryBronze") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "rest") \
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "changeme") \
    .getOrCreate()

# Suppress verbose logging
spark.sparkContext.setLogLevel("ERROR")

# Execute query passed as argument
if len(sys.argv) > 1:
    query = sys.argv[1]
    print(f"\n{'='*70}")
    print(f"Query: {query}")
    print(f"{'='*70}\n")
    
    result = spark.sql(query)
    result.show(truncate=False)
else:
    print("Usage: python query_bronze.py \"SELECT ... FROM lakehouse.taxi.bronze\"")

spark.stop()
