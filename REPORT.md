# Project 2 — Streaming Lakehouse Report

## 1. Medallion layer schemas

### Bronze (`lakehouse.taxi.bronze`)

Kafka JSON is parsed into typed columns matching the Yellow taxi parquet / producer payload: vendor, `tpep_*` datetimes as strings, passenger count, trip distance, rate and flag fields, PU/DO location IDs, payment type, fare components, totals, surcharges. Bronze keeps the **ingested shape** (string timestamps) so the stream writer stays simple and the table matches the wire format.

### Silver (`lakehouse.taxi.silver`)

- **Typed time:** `pickup_datetime`, `dropoff_datetime` as timestamps (parsed from ISO strings).
- **Business fields:** `vendor_id`, `passenger_count`, `trip_distance`, location IDs, payment type, `fare_amount`, `tip_amount`, `total_amount`.
- **Enrichment:** `pickup_zone`, `dropoff_zone` from `taxi_zone_lookup.parquet` (`LocationID` → `Zone`).
- **Scenario columns:**
  - `trip_duration_minutes` = \((\text{dropoff} - \text{pickup})\) in minutes (Spark `unix_timestamp` difference / 60).
  - `trip_speed_kmh` = `trip_distance / (trip_duration_minutes / 60)`, set to **null** if `trip_duration_minutes ≤ 0`, `trip_distance ≤ 0`, or either underlying value is null / unparseable.

Silver also applies **deduplication** on `(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, trip_distance, fare_amount)` and drops rows with null timestamps or `dropoff < pickup`.

### Gold (`lakehouse.taxi.gold`)

One row per **pickup_zone** (zones with at least one non-null speed):

- `pickup_zone`
- `avg_speed_kmh` = average of `trip_speed_kmh` in silver (null speeds excluded by definition)
- `max_speed_kmh` = maximum of `trip_speed_kmh` in silver

## 2. Cleaning rules and enrichment

| Rule                                                                                             | Rationale                                                 |
| ------------------------------------------------------------------------------------------------ | --------------------------------------------------------- |
| Parse ISO pickup/dropoff strings to timestamp                                                    | Correct arithmetic for duration and windows               |
| Drop rows with null pickup or dropoff                                                            | Cannot compute duration or speed reliably                 |
| Require `dropoff_datetime ≥ pickup_datetime`                                                     | Negative durations are invalid for speed                  |
| Set `trip_speed_kmh` null when duration ≤ 0 or distance ≤ 0                                      | Per scenario; avoids divide-by-zero and meaningless speed |
| `dropDuplicates` on vendor + time + locations + distance + fare                                  | Reduces duplicate Kafka / replay artifacts                |
| Left join zone lookup; aggregate gold only where `pickup_zone` and `trip_speed_kmh` are non-null | Keeps aggregates meaningful                               |

**Enrichment:** `taxi_zone_lookup.parquet` joined twice: PU → `pickup_zone`, DO → `dropoff_zone`.

## 3. Streaming configuration (bronze)

| Setting          | Value                                  | Why                                                                                                     |
| ---------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Checkpoint       | `/tmp/iceberg-checkpoints/taxi-bronze` | Fixed path so **restart resumes offsets** and Iceberg + Kafka sources do not re-append the same batches |
| Trigger          | `processingTime('10 seconds')`         | Micro-batches; balances latency and overhead                                                            |
| Output mode      | `append`                               | Each Kafka batch appends new rows to Iceberg                                                            |
| Starting offsets | `earliest`                             | First run consumes full topic; later runs use checkpoint                                                |

**Watermark:** Not used (no session/window aggregation on the bronze stream).

## 4. Gold table partitioning and Iceberg snapshots

**Partitioning:** `PARTITIONED BY (pickup_zone)` (Iceberg identity). The gold table is **one row per zone**; partitioning by `pickup_zone` aligns physical layout with the primary filter key for “per-zone speed” queries and keeps each partition small.

**Snapshot history (example query — run after `silver-gold`):**

```sql
SELECT snapshot_id, committed_at, operation
FROM lakehouse.taxi.gold.snapshots
ORDER BY committed_at DESC
LIMIT 10;
```

Equivalent from Jupyter (`cd ~/project`): `python pipeline.py stats` (includes a snapshot sample).

**Observed on a completed run:** `lakehouse.taxi.gold` snapshot `7499800018795992510`, `committed_at` `2026-04-05 00:03:27.32`, `operation` `overwrite` (only one snapshot until the next `silver-gold`).

## 5. Restart proof (bronze)

1. Note `SELECT COUNT(*) FROM lakehouse.taxi.bronze` (or `spark.sql(...)`).
2. Stop the bronze job (Ctrl+C) — the streaming query stops, but the checkpoint remains.
3. Restart `python pipeline.py bronze` (from `~/project`) **without deleting** `/tmp/iceberg-checkpoints/taxi-bronze`.
4. With no new producer traffic, the bronze row count stays the same after restart because Spark resumes from the saved Kafka offsets instead of re-reading old messages.

**Concrete restart check:** bronze stayed at **1044 rows before restart** and **1044 rows after restart** when the job was restarted from the same checkpoint.

**Row counts after a full ingest + `silver-gold` run:** bronze **1044**, silver **1044**, gold **67** (one row per pickup zone with at least one non-null speed).

_(If you rerun the producer while testing, the count can still increase, but only for newly produced offsets. The point of the checkpoint is that replaying the same Kafka offsets does not create duplicates.)_

## 6. Custom scenario — trip speed

Implemented in `pipeline.py` (`build_silver_dataframe` and `cmd_silver_gold`):

- Silver: `trip_duration_minutes`, `trip_speed_kmh` with nulling rules exactly as specified.
- Gold: `pickup_zone`, `avg_speed_kmh`, `max_speed_kmh`.

**`python pipeline.py stats` (this run):** `max_trip_speed_kmh` **≈ 48.68**, `avg_trip_speed_kmh` **≈ 11.11**, **6** silver rows with null `trip_speed_kmh`, **1044** total silver rows. Top zones by `max_speed_kmh` included JFK Airport (~48.7), LaGuardia (~39.6), Flushing Meadows-Corona Park (~39.0).

**Is ~48.7 realistic?** With TLC distance in **miles**, the scenario formula gives an **average speed in miles per hour** over each trip. **~49 mph** as a trip-level average is **plausible** for trips with significant highway/airport access (e.g. JFK/LGA); it is **not** an implausible physics-defying value. By contrast, **hundreds** of “mph” from the same formula usually indicate bad timestamps or zero/negative duration rows (here we null those speeds — only **6** nulls in this sample).

## 7. How to run

```bash
docker compose up -d
# Create topic once (see README)
docker exec kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic taxi-trips --partitions 3 --replication-factor 1"

# Producer (Jupyter terminal or docker exec)
cd ~/project
python produce.py

# Pipeline (repo root is ~/project — not python project/...)
python pipeline.py bronze        # Ctrl+C when done
python pipeline.py silver-gold
python pipeline.py stats
```

**Grader `.env`:** Use the values you submitted for the course (same as `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `JUPYTER_TOKEN` in `.env`); the Jupyter container exports MinIO credentials as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` for Spark S3 access.

**Host `docker exec`:** If you run scripts from PowerShell/CMD, use `/opt/conda/bin/python` (not bare `python`) so `pyspark` is found — see `README.md`.
