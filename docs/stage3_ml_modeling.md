# Stage 3: Feature Selection and Spark ML Modeling

## TL;DR

Stage 3 predicts trip-level fuel consumption in `L/100km`. The current pipeline first runs feature selection and train/test splitting, then trains three Spark ML regressors with cross-validation: Linear Regression, Random Forest, and Gradient-Boosted Trees.

## Goal

The target variable is:

```text
fuel_l_per_100km
```

The model predicts trip-level fuel consumption from trip behavior, vehicle metadata, and environmental features created in Stage 2.

## Current Stage 3 flow

| Step | Script | Responsibility |
|---|---|---|
| Stage 3.5 | `scripts/stage3_feature_selection.sh` | Runs feature selection and creates train/test Parquet datasets. |
| Stage 3.5 Python | `scripts/stage3_feature_selection.py` | Uses `SelectKBest` with `mutual_info_regression` for numeric features; keeps categorical metadata. |
| Stage 3.6 | `scripts/stage3_train_models.sh` | Trains three models, exports predictions, evaluation, CV results, and best-model metadata. |
| Stage 3.6 Python | `scripts/stage3_train_models.py` | Builds Spark ML pipelines, runs 3-fold CV, evaluates on the test split, saves models and outputs. |

Legacy note: `scripts/stage3.sh` and `scripts/stage3_spark_ml.py` still exist as an older two-model pipeline. The current `main.sh` does not use them.

## Input

Stage 3 reads the Stage 2 trip-level feature table:

```text
/user/team15/project/analytics/trip_features
```

This table contains one row per aggregated trip.

## Feature selection

Numeric candidate features:

```text
duration_min, observed_seconds, distance_km,
speed_mean, speed_median, speed_p95,
stop_go_ratio, idle_time_min,
maf_mean, maf_p95,
rpm_mean, rpm_p95,
abs_load_mean, oat_mean,
hv_current_mean, hv_soc_mean, hv_voltage_mean,
gen_weight, eng_dis
```

Feature-selection method:

```text
SelectKBest(score_func=mutual_info_regression, k=10)
```

Categorical features are kept for Spark categorical encoding:

```text
vehtype, vehclass, transmission, drive_wheels, eng_type, eng_conf
```

Target and trip filters:

- target must be non-null and positive;
- target is filtered by approximate 1st and 99th percentiles;
- `distance_km >= 0.5`;
- `duration_min > 0`;
- `0 <= speed_mean <= 160`.

Train/test split:

```text
70% train
30% test
random seed = 42
```

## Preprocessing pipeline

For each model, Spark ML performs:

1. median imputation for numeric columns;
2. `StringIndexer(handleInvalid="keep")` for categorical columns;
3. `OneHotEncoder` for categorical features;
4. `VectorAssembler` into `raw_features`;
5. optional `StandardScaler` for Linear Regression;
6. model training and prediction.

## Models

| Model index | Algorithm | Spark class | Local output |
|---:|---|---|---|
| 1 | Linear Regression | `LinearRegression` | `models/model1/` |
| 2 | Random Forest Regressor | `RandomForestRegressor` | `models/model2/` |
| 3 | Gradient-Boosted Trees Regressor | `GBTRegressor` | `models/model3/` |

All models use 3-fold cross-validation with RMSE as the optimization metric.

## Hyperparameter grids

### Model 1: Linear Regression

| Parameter | Values |
|---|---|
| `regParam` | `0.0`, `0.01`, `0.1` |
| `elasticNetParam` | `0.0`, `0.5`, `1.0` |
| `aggregationDepth` | `2`, `3`, `4` |

### Model 2: Random Forest Regressor

| Parameter | Values |
|---|---|
| `maxDepth` | `4`, `6`, `8` |
| `numTrees` | `20`, `40`, `60` |
| `subsamplingRate` | `0.7`, `0.85`, `1.0` |

### Model 3: GBT Regressor

| Parameter | Values |
|---|---|
| `maxDepth` | `3`, `4`, `5` |
| `maxBins` | `32`, `64`, `128` |
| `stepSize` | `0.03`, `0.05`, `0.1` |

## Outputs

### HDFS outputs

```text
/user/team15/project/ml_prepared/train
/user/team15/project/ml_prepared/test
/user/team15/project/ml_prepared/feature_selection.csv
/user/team15/project/ml_prepared/train_test_metadata.csv
/user/team15/project/models/model1
/user/team15/project/models/model2
/user/team15/project/models/model3
/user/team15/project/output/model1_predictions
/user/team15/project/output/model2_predictions
/user/team15/project/output/model3_predictions
/user/team15/project/output/evaluation.csv
/user/team15/project/output/model_training_log.csv
/user/team15/project/output/model_cv_results.csv
/user/team15/project/output/best_model.csv
/user/team15/project/output/specific_prediction.csv
```

### Local outputs

```text
output/feature_selection.csv
output/train_test_metadata.csv
models/model1/
models/model2/
models/model3/
output/model1_predictions.csv
output/model2_predictions.csv
output/model3_predictions.csv
output/evaluation.csv
output/model_training_log.csv
output/model_cv_results.csv
output/best_model.csv
output/specific_prediction.csv
```

## Resilts

The archived repository currently includes an older `output/evaluation.csv` with three models:

| Model | RMSE | MAE | R2 |
|---|---:|---:|---:|
| Linear Regression | 1.9562 | 1.0059 | 0.7804 |
| Random Forest Regressor | 1.3968 | 0.7564 | 0.8882 |
| GBT Regressor | 1.4696 | 0.7656 | 0.8762 |


## Interpretation

Linear Regression is a useful baseline because it is fast and interpretable. Tree-based models are more appropriate for tabular telemetry because fuel consumption depends on non-linear interactions between speed, engine load, vehicle type, temperature, and engine displacement.

The best model is selected by minimum test RMSE and written to:

```text
output/best_model.csv
```

## How to run

Current pipeline:

```bash
bash scripts/stage3_feature_selection.sh
bash scripts/stage3_train_models.sh
```

Legacy two-model pipeline:

```bash
bash scripts/stage3.sh
```

## How to validate

Check Stage 2 input:

```bash
hdfs dfs -ls /user/team15/project/analytics/trip_features
```

Check feature selection output:

```bash
cat output/feature_selection.csv
cat output/train_test_metadata.csv
hdfs dfs -ls /user/team15/project/ml_prepared
```

Check model outputs:

```bash
ls models/model1 models/model2 models/model3
cat output/evaluation.csv
cat output/best_model.csv
head output/model1_predictions.csv
head output/model2_predictions.csv
head output/model3_predictions.csv
```
