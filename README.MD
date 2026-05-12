# Distributed Trip-Level Fuel Consumption Forecasting

## TL;DR

This repository contains an end-to-end distributed data pipeline for trip-level fuel-consumption analytics and forecasting from large-scale Vehicle Energy Dataset telemetry.

The pipeline downloads and audits raw data, ingests it into PostgreSQL and HDFS, builds Spark-based EDA and trip-level features, runs storage/processing benchmarks, performs feature selection, trains Spark ML regression models, and prepares Hive/Superset dashboard tables.

## Dashboard

Published Apache Superset dashboard:

<http://hadoop-03.uni.innopolis.ru:8808/superset/dashboard/221/>

The dashboard is available from the IU Hadoop/Superset environment. It summarizes data quality, fleet composition, fuel-rate behavior, temperature/HVAC effects, engine-displacement effects, feature selection, model performance, benchmark results, and prediction examples.

## Pipeline overview

| Step | Script | Purpose | Main outputs |
|---|---|---|---|
| Stage 1.1 | `scripts/stage1_prepare_data.sh` | Download raw VED files if missing and build `data/vehicles.csv`. | Local raw data and cleaned vehicle metadata. |
| Stage 1.2 | `scripts/stage1_data_audit.py` | Audit downloaded files: size, row count, columns, Excel sheets. | `output/data_audit.csv`. |
| Stage 1.3 | `scripts/benchmarks/run_ingest_benchmark.sh` | Benchmark sample ingest operations on 100k telemetry rows. | `output/benchmarks/ingest_benchmark.csv`. |
| Stage 1.4 | `scripts/stage1_final_ingest.sh` | Load PostgreSQL tables and import them to HDFS using Sqoop. | HDFS warehouse `/user/team15/project/warehouse`. |
| Stage 2.5 | `scripts/stage2_data_profiling.sh` | Profile HDFS warehouse tables with Spark. | `output/profiling/*.csv`, `/user/team15/project/profiling`. |
| Stage 2 | `scripts/stage2.sh` | Run Spark EDA and create trip-level analytical features. | `/user/team15/project/analytics`, `/user/team15/project/analytics_csv`, `output/eda`. |
| Benchmarks | `scripts/benchmarks/run_storage_benchmark.sh`, `scripts/benchmarks/run_processing_benchmark.sh` | Compare storage formats and processing runtime. | `output/benchmarks/storage_benchmark.csv`, `output/benchmarks/processing_benchmark.csv`. |
| Stage 3.5 | `scripts/stage3_feature_selection.sh` | Select predictive numeric features and create train/test splits. | `/user/team15/project/ml_prepared`, `output/feature_selection.csv`, `output/train_test_metadata.csv`. |
| Stage 3.6 | `scripts/stage3_train_models.sh` | Train and evaluate three Spark ML regression models. | `models/model1..3`, `output/evaluation.csv`, `output/best_model.csv`, prediction CSVs. |
| Stage 4 | `scripts/stage4.sh` | Upload dashboard-ready CSVs to HDFS and create Hive external tables. | `/user/team15/project/stage4`, `stage4_*` Hive tables. |

## Repository structure

```text
.
├── data/                  # Local raw data placeholders and small ML split samples
├── docs/                  # Documentation, data story, benchmarking notes, figures
│   └── figures/stage2/    # Report-ready Stage 2 figures
├── models/                # Local Spark ML model artifacts copied from HDFS
├── notebooks/             # Exploratory notebooks; not required for the automated pipeline
├── output/                # Local generated reports, benchmarks, predictions, and evaluation files
├── scripts/               # Bash and Python scripts for data, EDA, ML, benchmarks, dashboard prep
│   └── benchmarks/        # Ingest, storage, processing, and ML benchmark scripts
├── sql/                   # PostgreSQL SQL and HiveQL external-table definitions
├── main.sh                # Runs the current full pipeline in stage order
├── requirements.txt       # Cluster-compatible Python dependencies
├── pyproject.toml         # Local project metadata; keep aligned with cluster runtime
└── README.MD              # Project overview and run instructions
```

## Data and prediction task

The project uses the Vehicle Energy Dataset (VED). Raw telemetry is sensor-level: each row describes one timestamp inside a vehicle trip.

Stage 2 aggregates sensor rows into trip-level features. Stage 3 predicts:

```text
fuel_l_per_100km
```

This is a regression task. Models are evaluated with RMSE, MAE, and R2.

## Prerequisites

Run the project from the repository root on the IU Hadoop cluster. The environment must provide:

- HDFS CLI: `hdfs`
- Sqoop: `sqoop`
- Spark on YARN: `spark-submit --master yarn`
- Hive/Beeline access for Stage 4
- PostgreSQL access to `team15_projectdb`
- Python packages from `requirements.txt`
- `yadisk-direct` for dataset download
- secret file `secrets/.psql.pass`

Install dependencies if needed:

```bash
pip install -r requirements.txt
```

## How to run

Run every command from the repository root:

```bash
cd Distributed-Trip-Level-Fuel-Consumption-Forecasting
```

Run the full current pipeline:

```bash
bash main.sh
```

Run stages separately:

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

Legacy Stage 3 entrypoint:

```bash
bash scripts/stage3.sh
```

`stage3.sh` runs the older two-model Spark ML pipeline. The current full pipeline uses `stage3_feature_selection.sh` and `stage3_train_models.sh` instead.

## HDFS locations

| Path | Description |
|---|---|
| `/user/team15/project/warehouse` | Raw PostgreSQL tables imported by Sqoop as compressed Parquet. |
| `/user/team15/project/profiling` | Warehouse profiling outputs. |
| `/user/team15/project/analytics` | Stage 2 analytical Parquet outputs. |
| `/user/team15/project/analytics_csv` | Stage 2 small CSV exports for reports and dashboard checks. |
| `/user/team15/project/benchmarks` | Benchmark datasets and benchmark result folders. |
| `/user/team15/project/ml_prepared` | Feature-selected train/test datasets. |
| `/user/team15/project/models` | Trained Spark ML models. |
| `/user/team15/project/output` | Model evaluation, CV results, prediction outputs. |
| `/user/team15/project/stage4` | Dashboard-ready CSV tables used by Hive/Superset. |

## Local outputs to check

After a successful full run, check:

```bash
cat output/data_audit.csv
ls output/profiling
ls output/eda
ls output/benchmarks
cat output/feature_selection.csv
cat output/train_test_metadata.csv
cat output/evaluation.csv
cat output/best_model.csv
head output/model1_predictions.csv
head output/model2_predictions.csv
head output/model3_predictions.csv
```

## Documentation

- `docs/stage1_data_ingestion.md` — Stage 1 data collection, audit, PostgreSQL loading, and Sqoop ingestion.
- `docs/stage2_data_story.md` — Stage 2 EDA story and dashboard-ready insights.
- `docs/stage3_ml_modeling.md` — Feature selection, train/test split, Spark ML training, evaluation, and outputs.
- `docs/stage4_dashboard.md` — Stage 4 dashboard-table preparation and Hive/Superset outputs.
- `docs/benchmarking.md` — Ingest, storage, processing, and ML benchmark documentation.
- `docs/repository_structure_review.md` — Repository structure review and cleanup recommendations.
