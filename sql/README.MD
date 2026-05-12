# SQL Directory

## TL;DR

This directory contains PostgreSQL SQL and HiveQL scripts used to build database tables and expose Spark/HDFS outputs to Hive/Superset.

## PostgreSQL scripts

| File | Used by | Description |
|---|---|---|
| `create_tables.sql` | `scripts/build_projectdb.py` | Drops/recreates PostgreSQL tables `vehicles` and `trips`; adds the foreign-key relation from `trips` to `vehicles`. |
| `import_data.sql` | `scripts/build_projectdb.py` | Contains `COPY` commands for loading `vehicles.csv` and telemetry CSV files into PostgreSQL. |
| `test_database.sql` | `scripts/build_projectdb.py` | Runs sample `SELECT` queries to verify that PostgreSQL tables were populated. |

PostgreSQL database:

```text
team15_projectdb
```

Main PostgreSQL tables:

```text
vehicles
trips
```

## Hive/Superset scripts

| File | Used by | Description |
|---|---|---|
| `stage2_superset_tables.hql` | Manual Beeline/Superset preparation | Creates external Hive tables over Stage 2 analytical Parquet outputs. |
| `stage4_superset_tables.hql` | `scripts/stage4.sh` | Creates external Hive tables over Stage 4 dashboard-ready CSV outputs. |

## Stage 2 Hive tables

Main Stage 2 tables:

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

## Stage 4 Hive tables

Main Stage 4 tables:

```text
stage4_data_audit
stage4_table_overview
stage4_column_types
stage4_missingness
stage4_numeric_summary
stage4_numeric_quantiles
stage4_categorical_summary
stage4_feature_selection
stage4_train_test_metadata
stage4_model_evaluation
stage4_best_model
stage4_model_training_log
stage4_model_cv_results
stage4_specific_prediction
stage4_model_prediction_samples
stage4_ingest_benchmark
stage4_storage_benchmark
stage4_processing_benchmark
```

## How to run HiveQL manually

```bash
beeline -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001" \
  -n team15 \
  -p "$(cat secrets/.psql.pass)" \
  -f sql/stage4_superset_tables.hql
```
