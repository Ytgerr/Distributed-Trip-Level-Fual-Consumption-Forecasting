# Stage 1: Data Collection and Ingestion

## Goal

Stage 1 builds the storage foundation for the project. It collects the raw Vehicle Energy Dataset files, prepares vehicle metadata, loads data into PostgreSQL, and imports the PostgreSQL tables into HDFS with Sqoop.

The goal is to make the raw dataset available as distributed Parquet files for later Spark processing.

## Inputs

| Input | Location | Purpose |
|---|---|---|
| Raw VED telemetry CSV files | `data/*_week.csv` | Sensor-level trip records. |
| Static vehicle metadata spreadsheets | `data/VED_Static_Data_ICE&HEV.xlsx`, `data/VED_Static_Data_PHEV&EV.xlsx` | Vehicle type, class, weight, engine, and drivetrain metadata. |
| PostgreSQL password | `secrets/.psql.pass` | Required for loading data into `team15_projectdb`. |

## Main scripts

| Script | Responsibility |
|---|---|
| `scripts/data_collection.sh` | Downloads and unpacks the raw dataset into `data/`. |
| `scripts/preprocess_dataset.py` | Creates cleaned `data/vehicles.csv` from static metadata spreadsheets. |
| `scripts/build_projectdb.py` | Creates PostgreSQL tables and loads CSV files into PostgreSQL. |
| `scripts/stage1.sh` | Runs the complete Stage 1 workflow and imports PostgreSQL tables to HDFS via Sqoop. |

## PostgreSQL schema

Stage 1 creates two main tables.

### `vehicles`

Each row describes one vehicle.

Main fields:

- `vehid` — vehicle identifier and primary key;
- `vehtype` — powertrain type, for example ICE or HEV;
- `vehclass` — vehicle class;
- `transmission`;
- `drive_wheels`;
- `gen_weight`;
- `eng_type`;
- `eng_dis`;
- `eng_conf`.

### `trips`

Each row describes one telemetry timestamp inside a trip.

Main fields:

- trip identifiers: `daynum`, `vehid`, `tripid`, `time`;
- geospatial fields: `lat`, `long`;
- driving signals: `speed`, `maf`, `rpm`, `abs_load`, `oat`;
- fuel and HVAC signals: `fuel_rate`, `air_cp_kw`, `air_cp_watts`, `heater_power`;
- hybrid battery signals: `hv_battery_current`, `hv_battery_soc`, `hv_battery_vol`;
- fuel-trim signals: `stfb_1`, `stfb_2`, `ltfb_1`, `ltfb_2`.

A foreign-key constraint links `trips.vehid` to `vehicles.vehid`.

## Processing flow

1. `stage1.sh` checks whether raw VED files already exist.
2. If data is missing, it calls `scripts/data_collection.sh`.
3. If `data/vehicles.csv` is missing, it calls `scripts/preprocess_dataset.py`.
4. `scripts/build_projectdb.py` runs:
   - `sql/create_tables.sql`;
   - `sql/import_data.sql`;
   - `sql/test_database.sql`.
5. Sqoop imports all PostgreSQL tables into HDFS as compressed Parquet files.
6. A local copy of the HDFS warehouse is saved under `output/warehouse/`.

## HDFS output

```text
/user/team15/project/warehouse/vehicles
/user/team15/project/warehouse/trips
```

The Sqoop import uses:

- Parquet format;
- Snappy compression;
- one mapper (`--m 1`).

## How to run

From the repository root:

```bash
bash scripts/stage1.sh
```

## How to validate

Check local data:

```bash
ls data
head data/vehicles.csv
```

Check PostgreSQL loading through the script output or manually through SQL:

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
