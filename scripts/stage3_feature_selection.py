#!/usr/bin/env python3
"""Preprocess trip-level dataset and run SelectKBest feature selection."""

import csv
import os
from pathlib import Path

import pandas as pd
from sklearn.feature_selection import SelectKBest, mutual_info_regression

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/user/team15/project/analytics/trip_features"
OUTPUT_HDFS = "/user/team15/project/ml_prepared"

LOCAL_OUTPUT_DIR = Path("output")
FEATURE_SELECTION_FILE = LOCAL_OUTPUT_DIR / "feature_selection.csv"
TRAIN_TEST_METADATA_FILE = LOCAL_OUTPUT_DIR / "train_test_metadata.csv"

TARGET = "fuel_l_per_100km"
LABEL = "label"
RANDOM_SEED = 42
TRAIN_RATIO = 0.7
SELECT_K = 10

NUMERIC_CANDIDATES = [
    "duration_min",
    "observed_seconds",
    "distance_km",
    "speed_mean",
    "speed_median",
    "speed_p95",
    "stop_go_ratio",
    "idle_time_min",
    "maf_mean",
    "maf_p95",
    "rpm_mean",
    "rpm_p95",
    "abs_load_mean",
    "oat_mean",
    "hv_current_mean",
    "hv_soc_mean",
    "hv_voltage_mean",
    "gen_weight",
    "eng_dis",
]

CATEGORICAL_KEEP = [
    "vehtype",
    "vehclass",
    "transmission",
    "drive_wheels",
    "eng_type",
    "eng_conf",
]


def existing_columns(df, columns):
    return [col for col in columns if col in df.columns]


def clean_nan_values(df, columns):
    result = df
    for col in columns:
        result = result.withColumn(
            col,
            F.when(F.isnan(F.col(col).cast("double")), None).otherwise(F.col(col)),
        )
    return result


def write_feature_selection(rows):
    LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with FEATURE_SELECTION_FILE.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "feature",
                "feature_type",
                "score",
                "rank",
                "selected",
                "selection_method",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def write_metadata(rows):
    LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with TRAIN_TEST_METADATA_FILE.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["dataset", "rows", "columns", "hdfs_path"],
        )
        writer.writeheader()
        writer.writerows(rows)


def main():
    spark = (
        SparkSession.builder
        .appName("team15_stage3_feature_selection")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )

    df = spark.read.parquet(INPUT_PATH)

    numeric_candidates = existing_columns(df, NUMERIC_CANDIDATES)
    categorical_features = existing_columns(df, CATEGORICAL_KEEP)

    df = clean_nan_values(df, numeric_candidates + [TARGET])

    df = (
        df
        .where(F.col(TARGET).isNotNull())
        .where(F.col(TARGET) > 0)
    )

    # Basic noise/outlier filtering on target and key trip fields.
    target_bounds = df.approxQuantile(TARGET, [0.01, 0.99], 0.01)
    if len(target_bounds) == 2:
        lower, upper = target_bounds
        df = df.where((F.col(TARGET) >= lower) & (F.col(TARGET) <= upper))

    if "distance_km" in df.columns:
        df = df.where(F.col("distance_km") >= 0.5)

    if "duration_min" in df.columns:
        df = df.where(F.col("duration_min") > 0)

    if "speed_mean" in df.columns:
        df = df.where((F.col("speed_mean") >= 0) & (F.col("speed_mean") <= 160))

    # sklearn feature selection is run only on the aggregated trip-level dataset.
    pandas_df = df.select(numeric_candidates + [TARGET]).toPandas()

    x = pandas_df[numeric_candidates].copy()
    y = pandas_df[TARGET].copy()

    x = x.fillna(x.median(numeric_only=True))

    k = min(SELECT_K, len(numeric_candidates))
    selector = SelectKBest(score_func=mutual_info_regression, k=k)
    selector.fit(x, y)

    scores = selector.scores_
    selected_mask = selector.get_support()

    feature_rows = []
    scored = []

    for feature, score, selected in zip(numeric_candidates, scores, selected_mask):
        scored.append((feature, float(score), bool(selected)))

    scored_sorted = sorted(scored, key=lambda item: item[1], reverse=True)

    rank_by_feature = {
        feature: rank + 1
        for rank, (feature, _, _) in enumerate(scored_sorted)
    }

    selected_numeric = []

    for feature, score, selected in scored_sorted:
        if selected:
            selected_numeric.append(feature)

        feature_rows.append({
            "feature": feature,
            "feature_type": "numeric",
            "score": score,
            "rank": rank_by_feature[feature],
            "selected": int(selected),
            "selection_method": "SelectKBest_mutual_info_regression",
        })

    for feature in categorical_features:
        feature_rows.append({
            "feature": feature,
            "feature_type": "categorical",
            "score": "",
            "rank": "",
            "selected": 1,
            "selection_method": "kept_for_spark_string_indexing",
        })

    write_feature_selection(feature_rows)

    selected_columns = selected_numeric + categorical_features + [TARGET]

    prepared = (
        df.select(selected_columns)
        .withColumnRenamed(TARGET, LABEL)
        .cache()
    )

    train_df, test_df = prepared.randomSplit(
        [TRAIN_RATIO, 1.0 - TRAIN_RATIO],
        seed=RANDOM_SEED,
    )

    train_path = "{}/train".format(OUTPUT_HDFS)
    test_path = "{}/test".format(OUTPUT_HDFS)

    train_df.write.mode("overwrite").option("compression", "snappy").parquet(train_path)
    test_df.write.mode("overwrite").option("compression", "snappy").parquet(test_path)

    metadata = [
        {
            "dataset": "train",
            "rows": train_df.count(),
            "columns": len(train_df.columns),
            "hdfs_path": train_path,
        },
        {
            "dataset": "test",
            "rows": test_df.count(),
            "columns": len(test_df.columns),
            "hdfs_path": test_path,
        },
    ]

    write_metadata(metadata)

    print("Selected numeric features:", selected_numeric)
    print("Kept categorical features:", categorical_features)
    print("Saved:", FEATURE_SELECTION_FILE)
    print("Saved:", TRAIN_TEST_METADATA_FILE)

    prepared.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
