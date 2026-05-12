#!/usr/bin/env python3
"""Benchmark several small-sample ingestion/read methods."""

import csv
import subprocess
import time
import re
from pathlib import Path

from pyspark.sql import SparkSession


SAMPLE_CSV = Path("data/sample/trips_sample_100k.csv")
OUTPUT_DIR = Path("output/benchmarks")
OUTPUT_FILE = OUTPUT_DIR / "ingest_benchmark.csv"

HDFS_ROOT = "/user/team15/project/benchmarks/ingest_sample"


def run_command(command):
    start = time.time()
    subprocess.check_call(command, shell=True)
    return time.time() - start


def write_result(rows):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["method", "operation", "runtime_sec", "rows", "details"],
        )
        writer.writeheader()
        writer.writerows(rows)



def clean_column_name(name):
    """Convert raw CSV column name to Spark/Parquet-safe name."""
    clean = re.sub(r"[^A-Za-z0-9_]+", "_", name.strip())
    clean = re.sub(r"_+", "_", clean).strip("_").lower()
    return clean or "column"


def make_unique_columns(columns):
    """Make column names safe and unique."""
    result = []
    seen = {}

    for col in columns:
        base = clean_column_name(col)
        count = seen.get(base, 0)

        if count == 0:
            result.append(base)
        else:
            result.append("{}_{}".format(base, count))

        seen[base] = count + 1

    return result


def main():
    if not SAMPLE_CSV.exists():
        raise FileNotFoundError("Create data/sample/trips_sample_100k.csv first.")

    rows = []

    spark = (
        SparkSession.builder
        .appName("team15_ingest_benchmark")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    # 1. Local Python CSV scan
    start = time.time()
    with SAMPLE_CSV.open("r", encoding="utf-8", errors="ignore") as file:
        reader = csv.reader(file)
        next(reader, None)
        count = sum(1 for _ in reader)
    rows.append({
        "method": "python_csv",
        "operation": "local_scan",
        "runtime_sec": round(time.time() - start, 4),
        "rows": count,
        "details": "Python csv.reader local scan",
    })

    # 2. HDFS put raw CSV
    run_command("hdfs dfs -rm -r -f {}".format(HDFS_ROOT))
    rows.append({
        "method": "hdfs_put_csv",
        "operation": "write_to_hdfs",
        "runtime_sec": round(run_command("hdfs dfs -mkdir -p {}".format(HDFS_ROOT)), 4),
        "rows": "",
        "details": "mkdir benchmark root",
    })

    runtime = run_command("hdfs dfs -put {} {}/raw_csv".format(SAMPLE_CSV, HDFS_ROOT))
    rows.append({
        "method": "hdfs_put_csv",
        "operation": "write_to_hdfs",
        "runtime_sec": round(runtime, 4),
        "rows": count,
        "details": "hdfs dfs -put sample CSV",
    })

    # 3. Spark read CSV from HDFS
    start = time.time()
    df_hdfs = spark.read.option("header", "true").option("inferSchema", "true").csv("{}/raw_csv".format(HDFS_ROOT))
    hdfs_rows = df_hdfs.count()
    rows.append({
        "method": "spark_csv_hdfs",
        "operation": "read_count",
        "runtime_sec": round(time.time() - start, 4),
        "rows": hdfs_rows,
        "details": "Spark read HDFS CSV with inferSchema",
    })

    # Rename raw CSV columns before writing to Parquet.
    df_hdfs = df_hdfs.toDF(*make_unique_columns(df_hdfs.columns))

    # 5. Write Parquet Snappy
    parquet_path = "{}/parquet_snappy".format(HDFS_ROOT)
    run_command("hdfs dfs -rm -r -f {}".format(parquet_path))
    start = time.time()
    df_hdfs.write.mode("overwrite").option("compression", "snappy").parquet(parquet_path)
    rows.append({
        "method": "spark_parquet_snappy",
        "operation": "write",
        "runtime_sec": round(time.time() - start, 4),
        "rows": hdfs_rows,
        "details": "Spark write Parquet Snappy",
    })

    # 6. Read Parquet Snappy
    start = time.time()
    parquet_rows = spark.read.parquet(parquet_path).count()
    rows.append({
        "method": "spark_parquet_snappy",
        "operation": "read_count",
        "runtime_sec": round(time.time() - start, 4),
        "rows": parquet_rows,
        "details": "Spark read Parquet Snappy",
    })

    write_result(rows)
    spark.stop()

    print("Saved benchmark to {}".format(OUTPUT_FILE))


if __name__ == "__main__":
    main()
