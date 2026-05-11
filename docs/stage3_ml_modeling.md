# Stage 3: Predictive Data Analytics with Spark ML

## Goal

Stage 3 trains distributed regression models for trip-level fuel-consumption forecasting. The target variable is:

```text
fuel_l_per_100km
```

The model predicts fuel consumption in liters per 100 km from trip-level driving, vehicle, and environmental features created in Stage 2.

## Input

Stage 3 reads the Stage 2 trip-level feature table from HDFS:

```text
/user/team15/project/analytics/trip_features
```

This table contains one row per trip after aggregation by vehicle and trip identifiers.

## Main scripts

| Script | Responsibility |
|---|---|
| `scripts/stage3.sh` | Cleans old Stage 3 outputs, runs Spark ML on YARN, verifies local outputs. |
| `scripts/stage3_spark_ml.py` | Builds the Spark ML pipeline, trains models, evaluates them, and exports results. |

## Feature groups

### Numeric features

The current pipeline uses trip-level numeric features such as:

- trip duration and observed seconds;
- distance in km;
- mean, median, and p95 speed;
- stop-go ratio and idle time;
- MAF, RPM, engine-load statistics;
- outside air temperature;
- hybrid battery statistics;
- vehicle weight and engine displacement.

### Categorical features

Categorical vehicle metadata is encoded with `StringIndexer` and `OneHotEncoder`:

- `vehtype`;
- `vehclass`;
- `transmission`;
- `drive_wheels`;
- `eng_type`;
- `eng_conf`.

### Excluded leakage features

The model intentionally avoids direct target-leakage fields such as:

- `fuel_used_l`;
- `fuel_rate_mean_lhr`;
- `fuel_valid_points`.

These fields are useful for analysis but should not be used as predictors when the target is derived from fuel consumption.

## Preprocessing pipeline

The Spark ML preprocessing pipeline performs:

1. median imputation for numeric fields;
2. string indexing for categorical fields;
3. one-hot encoding for categorical indices;
4. vector assembly into `features_raw`;
5. standard scaling into `features`.

The data is split into:

```text
70% training
30% testing
```

with fixed random seed `42`.

## Models

| Model | Spark class | Output folder |
|---|---|---|
| Model 1 | `LinearRegression` | `models/model1/` |
| Model 2 | `GBTRegressor` | `models/model2/` |

Both models are selected using cross-validation and grid search on the training split only.

## Hyperparameter tuning

### Linear Regression

Tuned parameters:

- `elasticNetParam`: `0.0`, `0.5`, `1.0`;
- `regParam`: `0.01`, `0.1`, `1.0`.

### GBT Regressor

Tuned parameters:

- `maxDepth`: `3`, `5`;
- `stepSize`: `0.05`, `0.1`.

Cross-validation:

```text
numFolds = 3
optimization metric = RMSE
```

## Outputs

### HDFS outputs

```text
project/data/train
project/data/test
project/models/model1
project/models/model2
project/output/model1_predictions
project/output/model2_predictions
project/output/evaluation
```

### Local outputs

```text
data/train.json
data/test.json
models/model1/
models/model2/
output/model1_predictions.csv
output/model2_predictions.csv
output/evaluation.csv
```

## Results

The current `output/evaluation.csv` contains:

| Model | RMSE | MAE | R2 |
|-------|------|-----|----|
| LinearRegression (numFeatures=66) | 2.1483 | 1.1333 | 0.7545 |
| GBTRegression (numTrees=50, numFeatures=66) | 1.0352 | 0.5222 | 0.9430 |

At the time of this repository snapshot, the GBT model has lower error and higher R2 than the Linear Regression model. This means the non-linear model captures trip-level fuel-consumption patterns better than the linear baseline.

## Interpretation

Linear Regression is a useful baseline because it is simple, fast, and interpretable. However, fuel consumption depends on non-linear interactions between speed regime, engine load, temperature, vehicle type, and engine displacement. Gradient-Boosted Trees usually fit this type of tabular telemetry problem better because they can model non-linear thresholds and feature interactions without manually specifying them.

## How to run

From the repository root:

```bash
bash scripts/stage3.sh
```

## How to validate

Check that Stage 2 input exists:

```bash
hdfs dfs -ls /user/team15/project/analytics/trip_features
```

Check generated local outputs:

```bash
ls models/model1 models/model2
cat output/evaluation.csv
head output/model1_predictions.csv
head output/model2_predictions.csv
```

Check HDFS outputs:

```bash
hdfs dfs -ls project/models/model1
hdfs dfs -ls project/models/model2
hdfs dfs -ls project/output/evaluation
```
