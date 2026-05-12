# Stage 4: Dashboard Preparation and Hive Tables

## TL;DR

Stage 4 takes generated local CSV artifacts from previous stages, uploads them to HDFS under `/user/team15/project/stage4`, and creates Hive external tables for Superset.

## Goal

The goal is to expose final project artifacts to Superset as queryable Hive datasets.

Stage 4 does not design the Superset dashboard layout automatically. It prepares the data layer used by the dashboard.

## Main scripts

| Script | Responsibility |
|---|---|
| `scripts/stage4.sh` | Uploads CSV artifacts to HDFS and runs HiveQL table creation. |
| `sql/stage4_superset_tables.hql` | Defines `stage4_*` Hive external tables over uploaded CSV files. |

## Input artifacts

Stage 4 expects these files to exist locally before it runs:

```text
output/data_audit.csv
output/profiling/table_overview.csv
output/profiling/column_types.csv
output/profiling/missingness.csv
output/profiling/numeric_summary.csv
output/profiling/numeric_quantiles.csv
output/profiling/categorical_summary.csv
output/feature_selection.csv
output/train_test_metadata.csv
output/evaluation.csv
output/best_model.csv
output/model_training_log.csv
output/model_cv_results.csv
output/specific_prediction.csv
output/model1_predictions.csv
output/model2_predictions.csv
output/model3_predictions.csv
output/benchmarks/ingest_benchmark.csv
output/benchmarks/storage_benchmark.csv
output/benchmarks/processing_benchmark.csv
```

If a file is missing, `stage4.sh` skips the corresponding upload with a warning, except for best-model prediction sampling, which needs `output/best_model.csv` and the matching `output/modelN_predictions.csv`.

## HDFS output

Stage 4 writes dashboard-ready CSV folders to:

```text
/user/team15/project/stage4
```

Example paths:

```text
/user/team15/project/stage4/model_evaluation
/user/team15/project/stage4/best_model
/user/team15/project/stage4/feature_selection
/user/team15/project/stage4/storage_benchmark
```

## Hive tables

`sql/stage4_superset_tables.hql` creates external Hive tables with the prefix:

```text
stage4_
```

Main tables:

| Table | Purpose |
|---|---|
| `stage4_data_audit` | Raw file audit. |
| `stage4_table_overview` | Warehouse table row/column counts. |
| `stage4_missingness` | Column-level missingness. |
| `stage4_numeric_summary` | Numeric summary statistics. |
| `stage4_feature_selection` | Feature-selection scores and selected features. |
| `stage4_train_test_metadata` | Train/test dataset sizes and paths. |
| `stage4_model_evaluation` | Model RMSE, MAE, R2. |
| `stage4_best_model` | Best model metadata and parameters. |
| `stage4_model_training_log` | Model runtime and training summary. |
| `stage4_model_cv_results` | Cross-validation RMSE by parameter grid. |
| `stage4_specific_prediction` | One selected prediction example. |
| `stage4_model_prediction_samples` | Sample predictions from the best model. |
| `stage4_ingest_benchmark` | Ingest benchmark results. |
| `stage4_storage_benchmark` | Storage-format benchmark results. |
| `stage4_processing_benchmark` | Processing-stage runtime benchmark results. |

## How to run

```bash
bash scripts/stage4.sh
```

## How to validate

Check HDFS folders:

```bash
hdfs dfs -ls /user/team15/project/stage4
hdfs dfs -ls /user/team15/project/stage4/model_evaluation
```

Check Hive tables through Beeline:

```sql
USE team15_projectdb;
SHOW TABLES LIKE 'stage4_*';
SELECT * FROM stage4_model_evaluation LIMIT 10;
SELECT * FROM stage4_best_model LIMIT 10;
```

## Dashboard use

Superset should use `stage4_*` tables for final reporting blocks and `stage2_*` tables for Stage 2 EDA charts.
