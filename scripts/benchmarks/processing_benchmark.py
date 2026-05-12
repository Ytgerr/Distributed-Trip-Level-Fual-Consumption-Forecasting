#!/usr/bin/env python3
"""Benchmark main Spark processing operations."""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


WAREHOUSE = "/user/team15/project/warehouse"
ANALYTICS = "/user/team15/project/analytics"
RESULT_PATH = "/user/team15/project/benchmarks/results/processing_benchmark"


def measure(name, action):
    start = time.time()
    df = action()
    rows = df.count()
    elapsed = time.time() - start
    print(f"{name}: rows={rows}, time={elapsed:.3f}s")
    return name, rows, float(elapsed)


def main():
    spark = (
        SparkSession.builder
        .appName("team15_processing_benchmark")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )

    results = []

    trips = spark.read.parquet(f"{WAREHOUSE}/trips")
    vehicles = spark.read.parquet(f"{WAREHOUSE}/vehicles")

    results.append(measure("read_trips", lambda: trips))
    results.append(measure("filter_valid_speed", lambda: trips.where(F.col("speed").isNotNull())))
    results.append(measure("read_vehicles", lambda: vehicles))

    results.append(
        measure(
            "join_trips_vehicles",
            lambda: trips.join(F.broadcast(vehicles), on="vehid", how="left"),
        )
    )

    results.append(
        measure(
            "trip_level_aggregation",
            lambda: trips.groupBy("vehid", "tripid").agg(
                F.count("*").alias("records"),
                F.avg("speed").alias("avg_speed"),
                F.max("speed").alias("max_speed"),
                F.avg("rpm").alias("avg_rpm"),
                F.avg("maf").alias("avg_maf"),
            ),
        )
    )

    trip_features = spark.read.parquet(f"{ANALYTICS}/trip_features")
    results.append(measure("read_trip_features", lambda: trip_features))

    result_df = spark.createDataFrame(results, ["stage_name", "output_rows", "runtime_sec"])
    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
