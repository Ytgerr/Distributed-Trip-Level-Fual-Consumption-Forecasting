# Models – Stage 3 Artefacts

This directory contains the trained Spark ML model artefacts produced by Stage 3
(`scripts/stage3_spark_ml.py`).

## Task

**Regression** – predict trip-level fuel consumption in **L/100 km**
(`fuel_l_per_100km`) from sensor-aggregated trip features.

## Models

| Directory   | Algorithm                        | Tuned hyperparameters              |
|-------------|----------------------------------|------------------------------------|
| `model1/`   | Linear Regression (pyspark.ml)   | `elasticNetParam`, `regParam`      |
| `model2/`   | Gradient-Boosted Trees Regressor | `maxDepth`, `stepSize`             |

Both models are selected via 3-fold cross-validation + grid search on the
**training set only**.  The best model from each grid search is saved here.

## HDFS locations

```
project/models/model1   ← LinearRegression best model
project/models/model2   ← GBTRegressor best model
```

The Python script downloads these directories locally with:

```bash
hdfs dfs -get project/models/model1 models/model1
hdfs dfs -get project/models/model2 models/model2
```

## Features used

**Numeric** (imputed with median, then StandardScaler):
`duration_min`, `observed_seconds`, `distance_km`, `speed_mean`, `speed_median`,
`speed_p95`, `stop_go_ratio`, `idle_time_min`, `maf_mean`, `maf_p95`,
`rpm_mean`, `rpm_p95`, `abs_load_mean`, `oat_mean`, `hv_current_mean`,
`hv_soc_mean`, `hv_voltage_mean`, `gen_weight`, `eng_dis`

**Categorical** (StringIndexer → OneHotEncoder):
`vehtype`, `vehclass`, `transmission`, `drive_wheels`, `eng_type`, `eng_conf`

**Excluded (leakage)**:
`fuel_used_l`, `fuel_rate_mean_lhr`, `fuel_valid_points`

## Output files

| File                              | Description                                  |
|-----------------------------------|----------------------------------------------|
| `output/model1_predictions.csv`   | `label`, `prediction` columns for model 1   |
| `output/model2_predictions.csv`   | `label`, `prediction` columns for model 2   |
| `output/evaluation.csv`           | RMSE / MAE / R2 comparison of both models   |
| `data/train.json`                 | Training split (features + label)            |
| `data/test.json`                  | Test split (features + label)                |

## How to run

```bash
bash scripts/stage3.sh
```
