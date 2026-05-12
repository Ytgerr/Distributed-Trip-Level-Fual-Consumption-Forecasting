# Models Directory

## TL;DR

This directory stores local copies of trained Spark ML model artifacts downloaded from HDFS. Model folders are generated artifacts and should usually be recreated by running Stage 3.

## Current models

The current training pipeline is:

```bash
bash scripts/stage3_feature_selection.sh
bash scripts/stage3_train_models.sh
```

Expected model folders after a successful current run:

| Directory | Algorithm | Spark class |
|---|---|---|
| `model1/` | Linear Regression | `LinearRegression` |
| `model2/` | Random Forest Regressor | `RandomForestRegressor` |
| `model3/` | Gradient-Boosted Trees Regressor | `GBTRegressor` |

Legacy note: the older `scripts/stage3.sh` pipeline creates only `model1/` and `model2/`.

## Task

Regression target:

```text
fuel_l_per_100km
```

The model predicts trip-level fuel consumption in liters per 100 km from selected trip-level features.

## HDFS locations

Current model locations:

```text
/user/team15/project/models/model1
/user/team15/project/models/model2
/user/team15/project/models/model3
```

## Related outputs

| File | Description |
|---|---|
| `output/evaluation.csv` | RMSE, MAE, R2 comparison. |
| `output/model_training_log.csv` | Runtime, CV size, metrics, and best parameters. |
| `output/model_cv_results.csv` | Cross-validation RMSE for parameter combinations. |
| `output/best_model.csv` | Best model selected by minimum test RMSE. |
| `output/model1_predictions.csv` | Predictions from Linear Regression. |
| `output/model2_predictions.csv` | Predictions from Random Forest in the current pipeline. |
| `output/model3_predictions.csv` | Predictions from GBT in the current pipeline. |
| `output/specific_prediction.csv` | One selected prediction example from the best model. |

## How to regenerate

```bash
bash scripts/stage3_feature_selection.sh
bash scripts/stage3_train_models.sh
```

## Tracking policy

Recommended policy:

- commit this `README.MD`;
- do not commit full Spark model folders unless required;
- regenerate models from HDFS/Spark jobs when needed.
