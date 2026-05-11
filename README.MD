# Distributed Trip-Level Fuel Consumption Forecasting

This repository contains an end-to-end distributed pipeline for trip-level fuel-consumption analytics and forecasting from large-scale vehicle telemetry.

The project ingests raw Vehicle Energy Dataset (VED) telemetry into PostgreSQL and HDFS, prepares analytical tables with Spark, builds trip-level fuel-consumption models with Spark ML, and presents the results in an Apache Superset dashboard.

## Dashboard

Published Apache Superset dashboard:

<http://hadoop-03.uni.innopolis.ru:8808/superset/dashboard/221/>

The dashboard is available from the IU Hadoop/Superset environment. It summarizes data quality, fleet composition, fuel-rate behavior, temperature/HVAC effects, engine-displacement effects, and ML-ready trip-level records.

## Pipeline overview

| Stage | Script | Purpose | Main outputs |
|---|---|---|---|
| Stage 1 | `scripts/stage1.sh` | Download and preprocess VED data, build PostgreSQL tables, load data into PostgreSQL, import tables to HDFS with Sqoop. | PostgreSQL tables `vehicles`, `trips`; HDFS Parquet warehouse `/user/team15/project/warehouse`. |
| Stage 2 | `scripts/stage2.sh` | Run Spark EDA and trip-level feature engineering on YARN. Export small analytical CSVs for reporting and dashboarding. | HDFS Parquet `/user/team15/project/analytics`; HDFS CSV `/user/team15/project/analytics_csv`. |
| Stage 3 | `scripts/stage3.sh` | Train distributed Spark ML models for trip-level fuel consumption prediction. | `data/train.json`, `data/test.json`, `models/model1/`, `models/model2/`, `output/model1_predictions.csv`, `output/model2_predictions.csv`, `output/evaluation.csv`. |
| Stage 4 | `scripts/stage4.sh` | Presentation stage. The Superset dashboard is created/published manually; the script is currently a placeholder because Superset layout actions are not automated. | Published Superset dashboard and screenshots/report descriptions. |

## Repository structure

```text
.
├── data/                 # Local data inputs and small ML split samples
├── docs/                 # Stage documentation, data story, dashboard/report notes, figures
├── models/               # Spark ML model artifacts copied from HDFS
├── notebooks/            # Exploratory notebooks; not required by the automated pipeline
├── output/               # Local exported EDA, figures, predictions, and evaluation files
├── scripts/              # Bash and Python scripts for the four pipeline stages
├── sql/                  # PostgreSQL SQL and HiveQL scripts
├── main.sh               # Runs the full pipeline in stage order
├── requirements.txt      # Python dependencies used on the cluster
└── README.MD             # Project overview and run instructions
```

## Data and task

The project uses the Vehicle Energy Dataset (VED). The raw telemetry is sensor-level: each row describes one timestamp inside a vehicle trip. Stage 2 aggregates these records into trip-level features, and Stage 3 predicts:

```text
fuel_l_per_100km
```

The prediction task is a regression problem. The current model comparison uses RMSE, MAE, and R2.

## Prerequisites

Run the project from the repository root on the IU Hadoop cluster. The cluster environment must provide:

- HDFS command-line client: `hdfs`
- Sqoop: `sqoop`
- Spark on YARN: `spark-submit --master yarn`
- PostgreSQL access to `team15_projectdb`
- Python environment with packages from `requirements.txt`
- `yadisk-direct` command for downloading the dataset from Yandex Disk
- secret files:
  - `secrets/.psql.pass`

Install Python dependencies if needed:

```bash
pip install -r requirements.txt
```

## How to run

Run every command from the repository root:

```bash
cd Distributed-Trip-Level-Fual-Consumption-Forecasting
```

Run the full pipeline:

```bash
bash main.sh
```

Run stages separately:

```bash
bash scripts/stage1.sh
bash scripts/stage2.sh
bash scripts/stage3.sh
bash scripts/stage4.sh
```

Recommended order during development:

1. Run `bash scripts/stage1.sh` after confirming dataset download and PostgreSQL credentials.
2. Run `bash scripts/stage2.sh` after HDFS warehouse tables exist.
3. Run `bash scripts/stage3.sh` after Stage 2 creates `/user/team15/project/analytics/trip_features`.
4. Open the Superset dashboard and verify that charts and text blocks are published.

## HDFS locations

| Path | Description |
|---|---|
| `/user/team15/project/warehouse` | Raw PostgreSQL tables imported by Sqoop as compressed Parquet. |
| `/user/team15/project/analytics` | Stage 2 analytical Parquet outputs. |
| `/user/team15/project/analytics_csv` | Stage 2 small CSV exports for reports/dashboard checks. |
| `project/data/train`, `project/data/test` | Stage 3 train/test JSON splits in HDFS. |
| `project/models/model1`, `project/models/model2` | Stage 3 best Spark ML models in HDFS. |
| `project/output/model1_predictions`, `project/output/model2_predictions`, `project/output/evaluation` | Stage 3 prediction and evaluation outputs in HDFS. |

## Local outputs to check

After successful execution, check:

```bash
ls output/eda
ls output/figures
cat output/evaluation.csv
head output/model1_predictions.csv
head output/model2_predictions.csv
```

The current Stage 3 output compares:

- `model1`: Linear Regression
- `model2`: Gradient-Boosted Trees Regressor

## Documentation

- `docs/stage1_data_ingestion.md` — Stage 1 database and HDFS ingestion documentation.
- `docs/stage2_data_story.md` — Stage 2 EDA story and dashboard-ready insights.
- `docs/stage3_ml_modeling.md` — Stage 3 Spark ML modeling documentation.
- `models/README.MD` — model artifacts and feature summary.
