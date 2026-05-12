#!/usr/bin/env python3
"""Benchmark ML models with and without feature selection."""

import time

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_PATH = "/user/team15/project/analytics/trip_features"
RESULT_PATH = "/user/team15/project/benchmarks/results/ml_benchmark"

TARGET = "fuel_l_per_100km"
SEED = 42


SELECTED_NUMERIC = [
    "duration_min",
    "distance_km",
    "speed_mean",
    "speed_p50",
    "speed_p95",
    "rpm_mean",
    "maf_mean",
    "abs_load_mean",
    "oat_mean",
    "eng_displacement",
    "weight",
]

SELECTED_CATEGORICAL = [
    "vehtype",
    "vehclass",
    "transmission",
    "drive_wheels",
]


def existing(df, cols):
    return [c for c in cols if c in df.columns]


def build_pipeline(model, numeric_cols, categorical_cols):
    imputed_cols = [f"{c}_imputed" for c in numeric_cols]

    stages = []

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
        idx = f"{col}_idx"
        oh = f"{col}_oh"
        stages.append(StringIndexer(inputCol=col, outputCol=idx, handleInvalid="keep"))
        stages.append(OneHotEncoder(inputCol=idx, outputCol=oh))
        encoded_cols.append(oh)

    stages.append(
        VectorAssembler(
            inputCols=imputed_cols + encoded_cols,
            outputCol="features",
            handleInvalid="keep",
        )
    )

    stages.append(model)
    return Pipeline(stages=stages)


def evaluate(predictions):
    return {
        "rmse": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse").evaluate(predictions),
        "mae": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae").evaluate(predictions),
        "r2": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2").evaluate(predictions),
    }


def run_case(df, feature_set_name, numeric_cols, categorical_cols, model_name, model):
    train, test = df.randomSplit([0.7, 0.3], seed=SEED)

    pipeline = build_pipeline(model, numeric_cols, categorical_cols)

    start = time.time()
    fitted = pipeline.fit(train)
    train_time = time.time() - start

    start = time.time()
    pred = fitted.transform(test)
    pred_count = pred.count()
    pred_time = time.time() - start

    metrics = evaluate(pred)

    return (
        feature_set_name,
        model_name,
        len(numeric_cols) + len(categorical_cols),
        float(train_time),
        float(pred_time),
        int(pred_count),
        float(metrics["rmse"]),
        float(metrics["mae"]),
        float(metrics["r2"]),
    )


def main():
    spark = (
        SparkSession.builder
        .appName("team15_ml_benchmark")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )

    raw = spark.read.parquet(DATA_PATH)

    selected_numeric = existing(raw, SELECTED_NUMERIC)
    selected_categorical = existing(raw, SELECTED_CATEGORICAL)

    banned = {"vehid", "tripid", TARGET, "fuel_used_l", "fuel_rate_mean_lhr"}
    all_numeric = [
        name for name, dtype in raw.dtypes
        if dtype in ("int", "bigint", "double", "float") and name not in banned
    ]
    all_categorical = [
        name for name, dtype in raw.dtypes
        if dtype == "string" and name not in banned
    ]

    df = raw.where(F.col(TARGET).isNotNull()).where(F.col(TARGET) > 0).withColumnRenamed(TARGET, "label")

    cases = [
        ("all_features", all_numeric, all_categorical),
        ("selected_features", selected_numeric, selected_categorical),
    ]

    models = [
        ("LinearRegression", LinearRegression(featuresCol="features", labelCol="label", regParam=0.1, elasticNetParam=0.0)),
        ("RandomForestRegressor", RandomForestRegressor(featuresCol="features", labelCol="label", maxDepth=8, numTrees=50, seed=SEED)),
        ("GBTRegressor", GBTRegressor(featuresCol="features", labelCol="label", maxDepth=5, maxIter=30, seed=SEED)),
    ]

    rows = []
    for feature_set_name, numeric_cols, categorical_cols in cases:
        for model_name, model in models:
            print(f"Running {feature_set_name} + {model_name}")
            rows.append(run_case(df, feature_set_name, numeric_cols, categorical_cols, model_name, model))

    result_df = spark.createDataFrame(
        rows,
        [
            "feature_set",
            "model",
            "num_features_before_encoding",
            "train_time_sec",
            "prediction_time_sec",
            "test_rows",
            "rmse",
            "mae",
            "r2",
        ],
    )

    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
