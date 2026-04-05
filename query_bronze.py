#!/usr/bin/env python3
"""
Run ad-hoc SQL against the Iceberg catalog (any lakehouse.taxi.* table).

Usage from ~/project in Jupyter (or docker exec … /home/jovyan/project/query_bronze.py):
  python query_bronze.py "SELECT COUNT(*) FROM lakehouse.taxi.bronze"
  python query_bronze.py "SELECT * FROM lakehouse.taxi.silver LIMIT 3"
"""

import sys

from lakehouse_spark import build_spark_session


def main() -> None:
    spark = build_spark_session("query-lakehouse")
    spark.sparkContext.setLogLevel("ERROR")

    if len(sys.argv) > 1:
        query = sys.argv[1]
        print(f"\n{'=' * 70}\nQuery: {query}\n{'=' * 70}\n")
        spark.sql(query).show(truncate=False)
    else:
        print('Usage: python query_bronze.py "SELECT ... FROM lakehouse.taxi.<table>"')

    spark.stop()


if __name__ == "__main__":
    main()
