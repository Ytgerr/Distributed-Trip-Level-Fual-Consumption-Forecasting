# SQL Directory

This directory contains SQL and HiveQL files used by the pipeline.

## PostgreSQL scripts

| File | Used by | Description |
|---|---|---|
| `create_tables.sql` | `scripts/build_projectdb.py` | Drops and recreates the PostgreSQL schema for `vehicles` and `trips`; adds the foreign-key relation from trips to vehicles. |
| `import_data.sql` | `scripts/build_projectdb.py` | Contains `COPY` commands for loading `vehicles.csv` and telemetry CSV files into PostgreSQL. |
| `test_database.sql` | `scripts/build_projectdb.py` | Runs sample `SELECT` queries to verify that PostgreSQL tables were populated. |

## Hive/Superset scripts

| File | Used by | Description |
|---|---|---|
| `stage2_superset_tables.hql` | Manual Beeline/Superset preparation | Creates external Hive tables over Stage 2 analytical Parquet outputs, so Superset can query them as datasets. |

## Database objects

PostgreSQL database:

```text
team15_projectdb
```

Main PostgreSQL tables:

```text
vehicles
trips
```

Main Stage 2 Hive/Superset tables include:

```text
stage2_eda_data_characteristics
stage2_eda_missingness
stage2_vehicle_type_distribution
stage2_fuel_by_vehicle_type
stage2_fuel_by_speed_bin
stage2_stop_go_vs_fuel
stage2_temperature_hvac_fuel
stage2_engine_displacement_fuel
stage2_geo_activity
stage2_trip_features
```
