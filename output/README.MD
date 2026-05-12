# Output Directory

## TL;DR

This directory stores local generated artifacts: audits, profiling CSVs, EDA exports, figures, benchmark results, model predictions, and evaluation files. Most files here are reproducible outputs, not source code.

## Stage 1 outputs

| Path | Description |
|---|---|
| `data_audit.csv` | File-level audit of downloaded raw data. |
| `warehouse/` | Local copy of Sqoop-imported HDFS warehouse tables. |
| `benchmarks/ingest_benchmark.csv` | Ingest benchmark on a 100k-row sample. |

## Stage 2 and profiling outputs

| Path | Description |
|---|---|
| `profiling/table_overview.csv` | Table row/column counts. |
| `profiling/column_types.csv` | Table column types. |
| `profiling/missingness.csv` | Column-level missingness. |
| `profiling/numeric_summary.csv` | Numeric summary statistics. |
| `profiling/numeric_quantiles.csv` | Numeric quantiles. |
| `profiling/categorical_summary.csv` | Approximate categorical cardinalities. |
| `eda/` | Small CSV files with EDA tables and trip-feature samples. |
| `figures/` | Generated EDA figures for documentation and dashboard storytelling. |

Expected Stage 2 EDA CSV files:

```text
vehicles_raw.csv
eda_data_characteristics.csv
eda_missingness.csv
insight_01_vehicle_type_distribution.csv
insight_02_fuel_by_vehicle_type.csv
insight_03_fuel_by_speed_bin.csv
insight_04_stop_go_vs_fuel.csv
insight_05_temperature_hvac_fuel.csv
insight_06_engine_displacement_fuel.csv
insight_07_geo_activity.csv
trip_features_sample.csv
```

## Benchmark outputs

| File | Description |
|---|---|
| `benchmarks/ingest_benchmark.csv` | Ingest benchmark result. |
| `benchmarks/storage_benchmark_raw.csv` | Raw storage benchmark result from Spark. |
| `benchmarks/storage_sizes.csv` | HDFS size by storage format/compression. |
| `benchmarks/storage_benchmark.csv` | Storage benchmark joined with HDFS sizes. |
| `benchmarks/processing_benchmark.csv` | Processing-stage runtime benchmark. |
| `benchmarks/ml_benchmark.csv` | Optional ML benchmark output. |

## Stage 3 outputs

| File | Description |
|---|---|
| `feature_selection.csv` | Numeric feature-selection scores and selected flags. |
| `train_test_metadata.csv` | Train/test row counts, column counts, HDFS paths. |
| `model1_predictions.csv` | Predictions from Linear Regression. |
| `model2_predictions.csv` | Predictions from Random Forest in the current pipeline. |
| `model3_predictions.csv` | Predictions from GBT in the current pipeline. |
| `evaluation.csv` | RMSE, MAE, and R2 comparison. |
| `model_training_log.csv` | Runtime, CV size, metrics, and best params. |
| `model_cv_results.csv` | Cross-validation RMSE per parameter set. |
| `best_model.csv` | Best model selected by minimum RMSE. |
| `specific_prediction.csv` | One prediction example from the best model. |

Snapshot note: this archive currently contains an older two-model `evaluation.csv`. Rerun current Stage 3 to produce the full current output set.

## How to regenerate

```bash
bash scripts/stage1.sh
bash scripts/stage2_data_profiling.sh
bash scripts/stage2.sh
bash scripts/benchmarks/run_storage_benchmark.sh
bash scripts/benchmarks/run_processing_benchmark.sh
bash scripts/stage3_feature_selection.sh
bash scripts/stage3_train_models.sh
bash scripts/stage4.sh
```

## Tracking policy

Recommended policy:

- commit this `README.MD`;
- commit only small, intentional snapshot outputs if required;
- do not commit large generated artifacts;
- regenerate outputs from scripts whenever possible.
