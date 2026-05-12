#!/usr/bin/env python3
"""Train three Spark MLlib regression models on prepared train/test datasets."""

import csv
import os
import shutil
import subprocess
import time
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TRAIN_PATH = "/user/team15/project/ml_prepared/train"
TEST_PATH = "/user/team15/project/ml_prepared/test"

HDFS_MODEL_ROOT = "/user/team15/project/models"
HDFS_OUTPUT_ROOT = "/user/team15/project/output"

LOCAL_MODEL_ROOT = Path("models")
LOCAL_OUTPUT_ROOT = Path("output")

LABEL = "label"
SEED = 42
CV_FOLDS = 3
CV_PARALLELISM = 2


def run(command):
    print(command)
    subprocess.check_call(command, shell=True)


def local_reset(path):
    if path.exists():
        if path.is_dir():
            shutil.rmtree(str(path))
        else:
            path.unlink()


def get_numeric_and_categorical_columns(df):
    numeric_cols = []
    categorical_cols = []

    for name, dtype in df.dtypes:
        if name == LABEL:
            continue

        if dtype == "string":
            categorical_cols.append(name)
        else:
            numeric_cols.append(name)

    return numeric_cols, categorical_cols


def clean_numeric_nan(df, numeric_cols):
    result = df

    for col in numeric_cols:
        result = result.withColumn(
            col,
            F.when(F.isnan(F.col(col).cast("double")), None).otherwise(F.col(col)),
        )

    return result


def build_pipeline(model, numeric_cols, categorical_cols, use_scaler):
    stages = []

    imputed_cols = ["{}_imputed".format(col) for col in numeric_cols]

    if numeric_cols:
        stages.append(
            Imputer(
                inputCols=numeric_cols,
                outputCols=imputed_cols,
                strategy="median",
            )
        )

    encoded_cols = []

    for col in categorical_cols:
        idx_col = "{}_idx".format(col)
        oh_col = "{}_oh".format(col)

        stages.append(
            StringIndexer(
                inputCol=col,
                outputCol=idx_col,
                handleInvalid="keep",
            )
        )

        stages.append(
            OneHotEncoder(
                inputCol=idx_col,
                outputCol=oh_col,
            )
        )

        encoded_cols.append(oh_col)

    stages.append(
        VectorAssembler(
            inputCols=imputed_cols + encoded_cols,
            outputCol="raw_features",
            handleInvalid="keep",
        )
    )

    if use_scaler:
        stages.append(
            StandardScaler(
                inputCol="raw_features",
                outputCol="features",
                withMean=False,
                withStd=True,
            )
        )
    else:
        stages.append(
            VectorAssembler(
                inputCols=["raw_features"],
                outputCol="features",
                handleInvalid="keep",
            )
        )

    stages.append(model)

    return Pipeline(stages=stages)


def evaluate(predictions):
    rmse = RegressionEvaluator(
        labelCol=LABEL,
        predictionCol="prediction",
        metricName="rmse",
    ).evaluate(predictions)

    mae = RegressionEvaluator(
        labelCol=LABEL,
        predictionCol="prediction",
        metricName="mae",
    ).evaluate(predictions)

    r2 = RegressionEvaluator(
        labelCol=LABEL,
        predictionCol="prediction",
        metricName="r2",
    ).evaluate(predictions)

    return float(rmse), float(mae), float(r2)


def param_map_to_string(param_map):
    parts = []

    for param, value in param_map.items():
        parts.append("{}={}".format(param.name, value))

    return "; ".join(sorted(parts))


def best_params_to_string(best_model):
    model = best_model.stages[-1]

    params = []

    for param in model.extractParamMap():
        if param.parent == model.uid:
            try:
                value = model.getOrDefault(param)
                params.append("{}={}".format(param.name, value))
            except Exception:
                pass

    return "; ".join(sorted(params))


def save_predictions(predictions, hdfs_path, local_path):
    (
        predictions
        .select(
            F.col(LABEL).alias("actual_fuel_l_per_100km"),
            F.col("prediction").alias("predicted_fuel_l_per_100km"),
            F.abs(F.col(LABEL) - F.col("prediction")).alias("absolute_error"),
        )
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(hdfs_path)
    )

    local_reset(local_path)
    run("hdfs dfs -getmerge {}/*.csv {}".format(hdfs_path, local_path))


def train_one_model(
    model_index,
    model_name,
    model,
    pipeline,
    param_grid,
    train_df,
    test_df,
):
    evaluator = RegressionEvaluator(
        labelCol=LABEL,
        predictionCol="prediction",
        metricName="rmse",
    )

    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=CV_FOLDS,
        parallelism=CV_PARALLELISM,
        seed=SEED,
    )

    start = time.time()
    cv_model = cross_validator.fit(train_df)
    train_time = time.time() - start

    best_model = cv_model.bestModel

    pred_start = time.time()
    predictions = best_model.transform(test_df).cache()
    test_rows = predictions.count()
    prediction_time = time.time() - pred_start

    rmse, mae, r2 = evaluate(predictions)

    hdfs_model_path = "{}/model{}".format(HDFS_MODEL_ROOT, model_index)
    local_model_path = LOCAL_MODEL_ROOT / "model{}".format(model_index)

    run("hdfs dfs -rm -r -f {}".format(hdfs_model_path))
    best_model.write().overwrite().save(hdfs_model_path)

    local_reset(local_model_path)
    run("hdfs dfs -get {} {}".format(hdfs_model_path, local_model_path))

    hdfs_pred_path = "{}/model{}_predictions".format(HDFS_OUTPUT_ROOT, model_index)
    local_pred_path = LOCAL_OUTPUT_ROOT / "model{}_predictions.csv".format(model_index)
    save_predictions(predictions, hdfs_pred_path, local_pred_path)

    cv_rows = []

    for param_map, metric in zip(param_grid, cv_model.avgMetrics):
        cv_rows.append({
            "model": model_name,
            "params": param_map_to_string(param_map),
            "avg_cv_rmse": float(metric),
        })

    summary = {
        "model_index": model_index,
        "model": model_name,
        "cv_folds": CV_FOLDS,
        "param_grid_size": len(param_grid),
        "train_time_sec": round(train_time, 4),
        "prediction_time_sec": round(prediction_time, 4),
        "test_rows": int(test_rows),
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "best_params": best_params_to_string(best_model),
        "hdfs_model_path": hdfs_model_path,
        "local_model_path": str(local_model_path),
    }

    predictions.unpersist()

    return summary, cv_rows, predictions


def write_csv(path, rows, fieldnames):
    with Path(path).open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    LOCAL_MODEL_ROOT.mkdir(parents=True, exist_ok=True)
    LOCAL_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("team15_stage3_train_models")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )

    train_df = spark.read.parquet(TRAIN_PATH)
    test_df = spark.read.parquet(TEST_PATH)

    numeric_cols, categorical_cols = get_numeric_and_categorical_columns(train_df)

    train_df = clean_numeric_nan(train_df, numeric_cols).cache()
    test_df = clean_numeric_nan(test_df, numeric_cols).cache()

    train_rows = train_df.count()
    test_rows = test_df.count()

    print("Train rows:", train_rows)
    print("Test rows:", test_rows)
    print("Numeric columns:", numeric_cols)
    print("Categorical columns:", categorical_cols)

    run("hdfs dfs -mkdir -p {}".format(HDFS_MODEL_ROOT))
    run("hdfs dfs -mkdir -p {}".format(HDFS_OUTPUT_ROOT))

    # Model 1: Linear Regression
    lr = LinearRegression(
        featuresCol="features",
        labelCol=LABEL,
        maxIter=50,
    )

    lr_pipeline = build_pipeline(lr, numeric_cols, categorical_cols, use_scaler=True)

    lr_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.0, 0.01, 0.1])
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
        .addGrid(lr.aggregationDepth, [2, 3, 4])
        .build()
    )

    # Model 2: Random Forest
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=LABEL,
        seed=SEED,
    )

    rf_pipeline = build_pipeline(rf, numeric_cols, categorical_cols, use_scaler=False)

    rf_grid = (
        ParamGridBuilder()
        .addGrid(rf.maxDepth, [4, 6, 8])
        .addGrid(rf.numTrees, [20, 40, 60])
        .addGrid(rf.subsamplingRate, [0.7, 0.85, 1.0])
        .build()
    )

    # Model 3: GBT
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=LABEL,
        seed=SEED,
        maxIter=20,
    )

    gbt_pipeline = build_pipeline(gbt, numeric_cols, categorical_cols, use_scaler=False)

    gbt_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth, [3, 4, 5])
        .addGrid(gbt.maxBins, [32, 64, 128])
        .addGrid(gbt.stepSize, [0.03, 0.05, 0.1])
        .build()
    )

    model_specs = [
        (1, "LinearRegression", lr, lr_pipeline, lr_grid),
        (2, "RandomForestRegressor", rf, rf_pipeline, rf_grid),
        (3, "GBTRegressor", gbt, gbt_pipeline, gbt_grid),
    ]

    summaries = []
    cv_results = []

    for model_index, model_name, model, pipeline, param_grid in model_specs:
        print("Training model {}: {}".format(model_index, model_name))

        summary, cv_rows, _ = train_one_model(
            model_index=model_index,
            model_name=model_name,
            model=model,
            pipeline=pipeline,
            param_grid=param_grid,
            train_df=train_df,
            test_df=test_df,
        )

        summaries.append(summary)
        cv_results.extend(cv_rows)

    evaluation_rows = []

    for row in summaries:
        evaluation_rows.append({
            "model": row["model"],
            "RMSE": row["rmse"],
            "MAE": row["mae"],
            "R2": row["r2"],
        })

    write_csv(
        LOCAL_OUTPUT_ROOT / "evaluation.csv",
        evaluation_rows,
        ["model", "RMSE", "MAE", "R2"],
    )

    write_csv(
        LOCAL_OUTPUT_ROOT / "model_training_log.csv",
        summaries,
        [
            "model_index",
            "model",
            "cv_folds",
            "param_grid_size",
            "train_time_sec",
            "prediction_time_sec",
            "test_rows",
            "rmse",
            "mae",
            "r2",
            "best_params",
            "hdfs_model_path",
            "local_model_path",
        ],
    )

    write_csv(
        LOCAL_OUTPUT_ROOT / "model_cv_results.csv",
        cv_results,
        ["model", "params", "avg_cv_rmse"],
    )

    best = sorted(summaries, key=lambda row: row["rmse"])[0]

    write_csv(
        LOCAL_OUTPUT_ROOT / "best_model.csv",
        [best],
        [
            "model_index",
            "model",
            "cv_folds",
            "param_grid_size",
            "train_time_sec",
            "prediction_time_sec",
            "test_rows",
            "rmse",
            "mae",
            "r2",
            "best_params",
            "hdfs_model_path",
            "local_model_path",
        ],
    )

    best_pred_path = LOCAL_OUTPUT_ROOT / "model{}_predictions.csv".format(best["model_index"])

    with best_pred_path.open("r", encoding="utf-8") as source:
        lines = source.readlines()

    specific_path = LOCAL_OUTPUT_ROOT / "specific_prediction.csv"

    with specific_path.open("w", encoding="utf-8") as target:
        target.write("model,actual_fuel_l_per_100km,predicted_fuel_l_per_100km,absolute_error\n")
        if len(lines) > 1:
            target.write("{},{}".format(best["model"], lines[1]))

    # Copy local csv artifacts to HDFS.
    for filename in [
        "evaluation.csv",
        "model_training_log.csv",
        "model_cv_results.csv",
        "best_model.csv",
        "specific_prediction.csv",
    ]:
        run("hdfs dfs -put -f output/{} {}/{}".format(filename, HDFS_OUTPUT_ROOT, filename))

    train_df.unpersist()
    test_df.unpersist()
    spark.stop()

    print("Stage 3 training completed.")


if __name__ == "__main__":
    main()
