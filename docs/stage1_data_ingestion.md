# Stage 1: Data Collection, Audit, and Ingestion

## TL;DR

Stage 1 prepares the raw Vehicle Energy Dataset, audits local files, loads PostgreSQL tables, benchmarks sample ingest, and imports the final `vehicles` and `trips` tables into HDFS as Parquet.

## Goal

The goal of Stage 1 is to create a reliable distributed storage layer for later Spark processing.

Stage 1 converts raw local files into:

```text
/user/team15/project/warehouse/vehicles
/user/team15/project/warehouse/trips
```

These HDFS outputs are the main inputs for Stage 2 and profiling.

## Inputs

| Input | Location | Purpose |
|---|---|---|
| Raw VED telemetry CSV files | `data/*_week.csv` | Sensor-level trip records. |
| Static vehicle metadata spreadsheets | `data/VED_Static_Data_ICE&HEV.xlsx`, `data/VED_Static_Data_PHEV&EV.xlsx` | Vehicle type, class, weight, engine, and drivetrain metadata. |
| PostgreSQL password | `secrets/.psql.pass` | Password used for PostgreSQL and Sqoop connection. |

## Main scripts

| Script | Responsibility |
|---|---|
| `scripts/stage1.sh` | Orchestrates complete Stage 1. |
| `scripts/stage1_prepare_data.sh` | Downloads raw data if missing and generates `data/vehicles.csv` if missing. |
| `scripts/data_collection.sh` | Downloads and unpacks raw VED files. |
| `scripts/preprocess_dataset.py` | Converts static vehicle spreadsheets into cleaned `data/vehicles.csv`. |
| `scripts/stage1_data_audit.py` | Audits downloaded local files and writes `output/data_audit.csv`. |
| `scripts/benchmarks/run_ingest_benchmark.sh` | Creates a 100k-row sample and benchmarks ingest operations. |
| `scripts/stage1_final_ingest.sh` | Builds PostgreSQL tables and imports them to HDFS with Sqoop. |
| `scripts/build_projectdb.py` | Executes SQL and loads CSV files into PostgreSQL. |

## PostgreSQL schema

Stage 1 creates two main PostgreSQL tables.

### `vehicles`

One row describes one vehicle.

Important fields:

- `vehid` — vehicle identifier and primary key;
- `vehtype` — powertrain type;
- `vehclass` — vehicle class;
- `transmission`;
- `drive_wheels`;
- `gen_weight`;
- `eng_type`;
- `eng_dis`;
- `eng_conf`.

### `trips`

One row describes one telemetry timestamp inside a trip.

Important fields:

- identifiers: `daynum`, `vehid`, `tripid`, `time`;
- location: `lat`, `long`;
- driving signals: `speed`, `maf`, `rpm`, `abs_load`, `oat`;
- fuel and HVAC signals: `fuel_rate`, `air_cp_kw`, `air_cp_watts`, `heater_power`;
- hybrid battery signals: `hv_battery_current`, `hv_battery_soc`, `hv_battery_vol`;
- fuel-trim signals: `stfb_1`, `stfb_2`, `ltfb_1`, `ltfb_2`.

A foreign-key constraint links `trips.vehid` to `vehicles.vehid`.

## Processing flow

1. `stage1_prepare_data.sh` checks whether raw data exists.
2. If raw data is missing, it calls `data_collection.sh`.
3. If `data/vehicles.csv` is missing, it calls `preprocess_dataset.py`.
4. `stage1_data_audit.py` writes file-level audit metrics to `output/data_audit.csv`.
5. `run_ingest_benchmark.sh` benchmarks sample ingest on `data/sample/trips_sample_100k.csv`.
6. `stage1_final_ingest.sh` calls `build_projectdb.py` to create and populate PostgreSQL tables.
7. Sqoop imports PostgreSQL tables into HDFS as Parquet with Snappy compression.
8. A local copy of the HDFS warehouse is copied to `output/warehouse/`.

## HDFS output

```text
/user/team15/project/warehouse/vehicles
/user/team15/project/warehouse/trips
```

Sqoop configuration:

| Table | Format | Compression | Mappers | Split |
|---|---|---|---|---|
| `vehicles` | Parquet | Snappy | 1 | none |
| `trips` | Parquet | Snappy | 4 | `vehid` |

## Local outputs

```text
output/data_audit.csv
output/benchmarks/ingest_benchmark.csv
output/warehouse/
```

## How to run

```bash
bash scripts/stage1.sh
```

## How to validate

Check local data:

```bash
ls data
head data/vehicles.csv
cat output/data_audit.csv
cat output/benchmarks/ingest_benchmark.csv
```

Check PostgreSQL:

```sql
SELECT * FROM vehicles LIMIT 10;
SELECT * FROM trips LIMIT 10;
```

Check HDFS warehouse:

```bash
hdfs dfs -ls /user/team15/project/warehouse
hdfs dfs -ls /user/team15/project/warehouse/vehicles
hdfs dfs -ls /user/team15/project/warehouse/trips
```
