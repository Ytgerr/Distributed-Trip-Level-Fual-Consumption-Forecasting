#!/usr/bin/env python3
"""Benchmark storage formats and compression codecs with PySpark."""

import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


APP_NAME = "team15_storage_benchmark"

SOURCE_PATH = "/user/team15/project/analytics/trip_features"
BENCH_ROOT = "/user/team15/project/benchmarks/storage"
LOCAL_OUTPUT = "output/benchmarks/storage_benchmark.csv"

VARIANTS = [
    ("parquet", "snappy"),
    ("parquet", "gzip"),
    ("avro", "snappy"),
    ("avro", "deflate"),
    ("csv", "gzip"),
]


def timed(action):
    start = time.time()
    result = action()
    return result, time.time() - start


def hdfs_du_mb(path):
    # Uses Spark JVM Hadoop API indirectly is painful here;
    # this value is filled later by shell script with hdfs dfs -du.
    return None


def main():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )

    df = spark.read.parquet(SOURCE_PATH).cache()
    row_count = df.count()

    rows = []

    for fmt, codec in VARIANTS:
        out_path = f"{BENCH_ROOT}/{fmt}_{codec}"
        print(f"\nBenchmarking {fmt} + {codec}: {out_path}")

        spark._jvm.org.apache.hadoop.fs.FileSystem \
            .get(spark._jsc.hadoopConfiguration()) \
            .delete(spark._jvm.org.apache.hadoop.fs.Path(out_path), True)

        def write_action():
            writer = df.repartition(8).write.mode("overwrite").option("compression", codec)
            if fmt == "csv":
                writer.option("header", "true").csv(out_path)
            else:
                writer.format(fmt).save(out_path)

        _, write_time = timed(write_action)

        def read_action():
            if fmt == "csv":
                read_df = (
                    spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(out_path)
                )
            else:
                read_df = spark.read.format(fmt).load(out_path)

            return read_df.select(F.count("*").alias("cnt")).collect()[0]["cnt"]

        read_rows, read_time = timed(read_action)

        rows.append((fmt, codec, float(write_time), float(read_time), int(read_rows), int(row_count), out_path))

    result_df = spark.createDataFrame(
        rows,
        ["format", "compression", "write_time_sec", "read_time_sec", "read_rows", "source_rows", "hdfs_path"],
    )

    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        "/user/team15/project/benchmarks/results/storage_benchmark"
    )

    spark.stop()


if __name__ == "__main__":
    main()
