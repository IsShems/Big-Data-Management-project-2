# Project 2 — Streaming Lakehouse Pipeline

Build a streaming pipeline that consumes NYC taxi events from Kafka, processes them
with Spark Structured Streaming, and lands data into an Apache Iceberg lakehouse
using the medallion architecture (bronze → silver → gold).

### Custom scenario: trip speed (this repository)

The pipeline implements the **Trip Speed Calculation** scenario:

- **Silver** adds:
  - `trip_duration_minutes` = `(dropoff_datetime - pickup_datetime)` in minutes
  - `trip_speed_kmh` = `trip_distance / (trip_duration_minutes / 60)`, set to **null** when `trip_duration_minutes ≤ 0` **or** `trip_distance ≤ 0` (or either input is null / unparseable)
- **Gold** table `lakehouse.taxi.gold`: `pickup_zone`, `avg_speed_kmh`, `max_speed_kmh` (partitioned by `pickup_zone`).
- **Note on units:** NYC TLC Yellow taxi `trip_distance` is recorded in **miles**. The scenario formula is implemented exactly as specified; numerically, `distance ÷ (duration in hours)` is therefore **miles per hour** for TLC miles. The column name remains `trip_speed_kmh` as required. See **Maximum observed trip speed** below for interpreting realism.

---

## Pipeline (bronze → silver → gold)

| Path                 | Description                                                                         |
| -------------------- | ----------------------------------------------------------------------------------- |
| `pipeline.py`        | **Main entry:** Kafka → bronze stream; batch silver + gold with trip-speed scenario |
| `lakehouse_spark.py` | Shared `SparkSession` (Iceberg REST + MinIO, env-based credentials)                 |
| `query_bronze.py`    | Run arbitrary SQL against `lakehouse.taxi.*`                                        |
| `produce.py`         | Replays a parquet file as JSON into the `taxi-trips` Kafka topic                    |

After the stack is up and the Kafka topic exists, run the producer and pipeline.

**Important — paths in Jupyter:** `compose.yml` mounts your repo at **`~/project`**, which **is** the repo root (there is no `~/project/project/` folder). After `cd ~/project`, run **`python pipeline.py`** — not `python project/pipeline.py`.

```bash
cd ~/project

# Terminal 1: producer
python produce.py

# Terminal 2 (or after producer): bronze stream — Ctrl+C when done
python pipeline.py bronze

python pipeline.py silver-gold
python pipeline.py stats
```

Bronze checkpoint location (fixed path, **do not delete** between restarts if you want exactly-once semantics from Kafka): `/tmp/iceberg-checkpoints/taxi-bronze`.

**From Windows / host PowerShell** (not the Jupyter terminal), `docker exec … python` often hits the wrong interpreter and fails with `No module named 'pyspark'`. Use the Conda Python inside the image:

```bash
docker exec -it project2_jupyter /opt/conda/bin/python /home/jovyan/project/pipeline.py bronze
docker exec project2_jupyter /opt/conda/bin/python /home/jovyan/project/pipeline.py silver-gold
docker exec project2_jupyter /opt/conda/bin/python /home/jovyan/project/pipeline.py stats
```

Equivalent: `docker exec -it project2_jupyter bash -lc "python /home/jovyan/project/pipeline.py bronze"` (login shell picks up Conda).

---

## Maximum observed trip speed (for the report)

After `silver-gold` has run, execute (from `~/project` in Jupyter):

```bash
cd ~/project && python pipeline.py stats
```

The output includes `max_trip_speed_kmh` over `lakehouse.taxi.silver`.

**Realism:** In real urban driving, sustained **ground** speeds above ~50–60 mph are uncommon; short **averages** can look higher when `trip_distance` or timestamps are wrong (e.g. very small duration with non-zero distance, meter/GPS glitches, or drop-off before pick-up corrected in silver). **Very large** maxima (hundreds+) almost always indicate **bad rows** rather than taxis breaking speed limits. Use `stats` and spot-check those trips in silver (pickup/dropoff times and distance) when writing `REPORT.md`.

---

## What's in this template

| Path           | Description                                                       |
| -------------- | ----------------------------------------------------------------- |
| `compose.yml`  | Kafka, MinIO, Iceberg REST catalog, Jupyter/PySpark               |
| `produce.py`   | Replays a parquet file as JSON into the `taxi-trips` Kafka topic  |
| `REPORT.md`    | Report template (fill per course instructions)                    |
| `.env.example` | Template for credentials — copy to `.env` and fill in values      |
| `data/`        | **Not in git.** Place the provided parquet files here (see Setup) |

The `data/` directory is git-ignored. You will use the same files as in project 1:

| File                                   | Description                                                           |
| -------------------------------------- | --------------------------------------------------------------------- |
| `data/yellow_tripdata_2025-01.parquet` | NYC Yellow Taxi trips — January 2025                                  |
| `data/yellow_tripdata_2025-02.parquet` | NYC Yellow Taxi trips — February 2025                                 |
| `data/taxi_zone_lookup.parquet`        | Pickup/dropoff zone names (join with `PULocationID` / `DOLocationID`) |

---

## Setup

### 1. Configure credentials

Copy the example env file and set your own secrets:

```bash
cp .env.example .env
# Edit .env — change the passwords before starting the stack
```

The `.env` file is git-ignored and never committed.

You need to change all the default secrets, and provide them in `REPORT.md` section 7 in your project submission.

### 2. Place the data files

Put the three parquet files provided for the project into the `data/` directory:

```
project_2/
└── data/
    ├── yellow_tripdata_2025-01.parquet
    ├── yellow_tripdata_2025-02.parquet
    └── taxi_zone_lookup.parquet
```

### 3. Start the stack

```bash
docker compose up -d
```

Boot order is enforced automatically: MinIO starts → bucket is created → Iceberg REST
catalog starts → Jupyter starts. Allow ~20 seconds for all services to become ready.

### 4. Verify services

```bash
docker ps
```

All five services (`kafka`, `minio`, `minio_init`, `iceberg-rest`, `jupyter`) should
show **running** (or **exited** for `minio_init`, which is a one-shot job).

### 5. Create the topic and start the producer

Create the Kafka topic (do this once after the stack is up):

```bash
docker exec kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic taxi-trips --partitions 3 --replication-factor 1"
```

Then open a **Jupyter terminal** (File → New Terminal in JupyterLab) and run:

```bash
cd ~/project
python produce.py             # 5 events/s, single pass (January data)
python produce.py --loop      # replay indefinitely
python produce.py --rate 20   # faster replay
python produce.py --data data/yellow_tripdata_2025-02.parquet  # February data
```

Or from a host terminal (use Conda’s `python` — see note above if you see `No module named 'pyspark'`):

```bash
docker exec -it project2_jupyter /opt/conda/bin/python /home/jovyan/project/produce.py --loop
```

### 6. Open Jupyter

Navigate to **http://localhost:8888** — token is set in your `.env` file (`JUPYTER_TOKEN`).

Your repo is mounted at **`~/project/`** (that directory **is** the project root — run `python produce.py` and `python pipeline.py` from there, not `python project/...`).

### 7. Stop the stack

```bash
docker compose down          # keeps MinIO data (named volume)
docker compose down -v       # also deletes stored Iceberg tables
```

---

## Service URLs

| Service          | URL                                 | Credentials                                           |
| ---------------- | ----------------------------------- | ----------------------------------------------------- |
| Jupyter          | http://localhost:8888               | token: value of `JUPYTER_TOKEN` in `.env`             |
| Spark UI         | http://localhost:4040               | —                                                     |
| MinIO Console    | http://localhost:9001               | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| Iceberg REST API | http://localhost:8181/v1/namespaces | —                                                     |

---

## Kafka topic schema

`produce.py` publishes one JSON message per taxi trip. Each message has:

| Field                   | Type            | Notes                                    |
| ----------------------- | --------------- | ---------------------------------------- |
| `VendorID`              | int             | 1 or 2 — used as Kafka message key       |
| `tpep_pickup_datetime`  | ISO-8601 string | event time for windowing                 |
| `tpep_dropoff_datetime` | ISO-8601 string |                                          |
| `passenger_count`       | int             |                                          |
| `trip_distance`         | float           | miles                                    |
| `PULocationID`          | int             | join with `taxi_zone_lookup.parquet`     |
| `DOLocationID`          | int             | join with `taxi_zone_lookup.parquet`     |
| `fare_amount`           | float           |                                          |
| `tip_amount`            | float           |                                          |
| `total_amount`          | float           |                                          |
| `payment_type`          | int             | 1=Credit, 2=Cash, 3=No charge, 4=Dispute |
| `congestion_surcharge`  | float           |                                          |
| _(+ other TLC fields)_  |                 |                                          |

---

## SparkSession — starter configuration

Paste this into the first cell of your notebook. All Iceberg and Kafka settings are
pre-wired to the services in `compose.yml`. Credentials are read from the container
environment (set automatically from your `.env` file).

```python
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Packages are loaded via PYSPARK_SUBMIT_ARGS set in compose.yml.
# pyspark-notebook:2025-12-31 ships Spark 4.1.0 — print spark.version to confirm.

spark = (
    SparkSession.builder
    .appName("project2")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")

    # ── Iceberg ──────────────────────────────────────────────────────────────
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Catalog named 'lakehouse' — use it as: lakehouse.<database>.<table>
    .config("spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type",      "rest")
    .config("spark.sql.catalog.lakehouse.uri",       "http://iceberg-rest:8181")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
    # S3FileIO writes data files directly to MinIO
    .config("spark.sql.catalog.lakehouse.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint",          "http://minio:9000")
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakehouse.s3.access-key-id",     os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")

    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"Spark {spark.version}   catalog: lakehouse")

# ── Create your database once ──────────────────────────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")
```

### Reading from Kafka

```python
BOOTSTRAP = "kafka:9092"
TOPIC     = "taxi-trips"

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)
```

### Loading the zone lookup table

```python
zones = spark.read.parquet("data/taxi_zone_lookup.parquet")
zones.show(5)
```

---

## Grading checklist (self-review before submission)

- [x] `docker compose up` + from `~/project`: `python produce.py` + `python pipeline.py bronze` → `silver-gold` without errors
- [x] Bronze table exists and contains parsed trip fields (same shape as Kafka JSON)
- [x] Restarting the bronze job from the same checkpoint does **not** add duplicate rows
- [x] Silver has typed timestamps, zone names, `trip_duration_minutes`, `trip_speed_kmh` per scenario rules
- [x] Gold table `lakehouse.taxi.gold` has `pickup_zone`, `avg_speed_kmh`, `max_speed_kmh` and Iceberg partition on `pickup_zone`
- [x] Iceberg snapshot history is shown in `REPORT.md` (query or screenshot)
- [x] REPORT.md answers all required questions (see project brief)
- [x] Trip-speed scenario documented (README + REPORT)

---

## Troubleshooting

**`lakehouse.taxi.silver` not found but bronze has rows**  
Silver is created only when **`python pipeline.py silver-gold` finishes successfully**. The first Spark run can sit on Ivy/JAR downloads for **several minutes** with little output — wait until you see lines like `>>> Silver rows after clean/dedup` and `>>> Done. Row counts`. If you query silver while silver-gold is still starting, or if silver-gold exited with an error earlier, the table will not exist. Re-run `silver-gold` and scroll for `Silver Iceberg write failed` / Python errors.

**`ModuleNotFoundError: No module named 'pyspark'` (docker exec from host)**  
The Jupyter image installs PySpark in **Conda** (`/opt/conda`). Plain `docker exec … python` may run system Python without PySpark. Use `/opt/conda/bin/python` or `bash -lc "python …"` (see Pipeline section).

**`Failed to find data source: kafka`**
The Kafka connector jar failed to download. Check `PYSPARK_SUBMIT_ARGS` in
`compose.yml` — the version must match your Spark version (see `spark.version`).

**`Failed to find data source: iceberg`**
Same as above but for the Iceberg runtime jar.

**`Connection refused` to MinIO or iceberg-rest**
Services may still be starting. Wait 20–30 seconds and retry.
Check `docker compose logs iceberg-rest` for errors.

**Iceberg table not found after restart**
Tables are stored in MinIO (persistent named volume). They survive container restarts
as long as you don't run `docker compose down -v`.

**Wrong Spark / Scala version**
`pyspark-notebook:2025-12-31` ships Spark 4.1.0 (Scala 2.13). If `spark.version` shows a
different version, update the package coordinates in `PYSPARK_SUBMIT_ARGS` in `compose.yml`.
Spark 3.5.x uses `_2.12`; Spark 4.x uses `_2.13`. Also update the Iceberg artifact:
`iceberg-spark-runtime-4.0_2.13` for Spark 4.x, `iceberg-spark-runtime-3.5_2.12` for Spark 3.5.x.
