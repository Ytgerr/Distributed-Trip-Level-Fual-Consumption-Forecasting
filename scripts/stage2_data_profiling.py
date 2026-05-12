#!/usr/bin/env python3
"""Profile warehouse tables with PySpark."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


WAREHOUSE = "/user/team15/project/warehouse"
PROFILING_HDFS = "/user/team15/project/profiling"

TABLES = {
    "trips": "{}/trips".format(WAREHOUSE),
    "vehicles": "{}/vehicles".format(WAREHOUSE),
}

NUMERIC_TYPES = {
    "byte", "short", "int", "bigint", "long", "float", "double", "decimal"
}


def is_numeric_type(dtype):
    return any(dtype.startswith(t) for t in NUMERIC_TYPES)


def write_csv(df, name):
    path = "{}/{}".format(PROFILING_HDFS, name)
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )


def table_overview(spark, loaded_tables):
    rows = []

    for table_name, df in loaded_tables.items():
        rows.append((
            table_name,
            int(df.count()),
            len(df.columns),
            TABLES[table_name],
        ))

    schema = T.StructType([
        T.StructField("table_name", T.StringType(), False),
        T.StructField("row_count", T.LongType(), False),
        T.StructField("column_count", T.IntegerType(), False),
        T.StructField("hdfs_path", T.StringType(), False),
    ])

    return spark.createDataFrame(rows, schema)


def column_types(spark, loaded_tables):
    rows = []

    for table_name, df in loaded_tables.items():
        for column_name, dtype in df.dtypes:
            rows.append((table_name, column_name, dtype))

    schema = T.StructType([
        T.StructField("table_name", T.StringType(), False),
        T.StructField("column_name", T.StringType(), False),
        T.StructField("data_type", T.StringType(), False),
    ])

    return spark.createDataFrame(rows, schema)


def missingness(spark, loaded_tables):
    rows = []

    for table_name, df in loaded_tables.items():
        total_rows = df.count()
        exprs = []

        for column_name, dtype in df.dtypes:
            col = F.col(column_name)
            condition = col.isNull()

            if dtype in ("float", "double"):
                condition = condition | F.isnan(col)

            exprs.append(
                F.sum(F.when(condition, 1).otherwise(0)).alias(column_name)
            )

        result = df.agg(*exprs).collect()[0].asDict()

        for column_name, null_count in result.items():
            null_count = int(null_count or 0)
            null_pct = round(100.0 * null_count / total_rows, 4) if total_rows else 0.0
            rows.append((table_name, column_name, total_rows, null_count, null_pct))

    schema = T.StructType([
        T.StructField("table_name", T.StringType(), False),
        T.StructField("column_name", T.StringType(), False),
        T.StructField("total_rows", T.LongType(), False),
        T.StructField("null_count", T.LongType(), False),
        T.StructField("null_pct", T.DoubleType(), False),
    ])

    return spark.createDataFrame(rows, schema)


def numeric_summary(spark, loaded_tables):
    rows = []

    for table_name, df in loaded_tables.items():
        numeric_columns = [
            column_name
            for column_name, dtype in df.dtypes
            if is_numeric_type(dtype)
        ]

        for column_name in numeric_columns:
            clean_df = (
                df.select(F.col(column_name).cast("double").alias("x"))
                .where(F.col("x").isNotNull())
                .where(~F.isnan(F.col("x")))
            )

            stats = (
                clean_df
                .agg(
                    F.count("x").alias("non_null_count"),
                    F.mean("x").alias("mean"),
                    F.stddev("x").alias("stddev"),
                    F.variance("x").alias("variance"),
                    F.min("x").alias("min"),
                    F.max("x").alias("max"),
                )
                .collect()[0]
            )

            rows.append((
                table_name,
                column_name,
                int(stats["non_null_count"] or 0),
                float(stats["mean"]) if stats["mean"] is not None else None,
                float(stats["stddev"]) if stats["stddev"] is not None else None,
                float(stats["variance"]) if stats["variance"] is not None else None,
                float(stats["min"]) if stats["min"] is not None else None,
                float(stats["max"]) if stats["max"] is not None else None,
            ))

    schema = T.StructType([
        T.StructField("table_name", T.StringType(), False),
        T.StructField("column_name", T.StringType(), False),
        T.StructField("non_null_count", T.LongType(), False),
        T.StructField("mean", T.DoubleType(), True),
        T.StructField("stddev", T.DoubleType(), True),
        T.StructField("variance", T.DoubleType(), True),
        T.StructField("min", T.DoubleType(), True),
        T.StructField("max", T.DoubleType(), True),
    ])

    return spark.createDataFrame(rows, schema)


def numeric_quantiles(spark, loaded_tables):
    rows = []
    probabilities = [0.01, 0.25, 0.5, 0.75, 0.99]

    for table_name, df in loaded_tables.items():
        numeric_columns = [
            column_name
            for column_name, dtype in df.dtypes
            if is_numeric_type(dtype)
        ]

        for column_name in numeric_columns:
            quantiles = (
                df.select(F.col(column_name).cast("double").alias("x"))
                .where(F.col("x").isNotNull())
                .where(~F.isnan(F.col("x")))
                .approxQuantile("x", probabilities, 0.01)
            )

            if len(quantiles) == 5:
                rows.append((
                    table_name,
                    column_name,
                    float(quantiles[0]),
                    float(quantiles[1]),
                    float(quantiles[2]),
                    float(quantiles[3]),
                    float(quantiles[4]),
                ))

    schema = T.StructType([
        T.StructField("table_name", T.StringType(), False),
        T.StructField("column_name", T.StringType(), False),
        T.StructField("p01", T.DoubleType(), True),
        T.StructField("p25", T.DoubleType(), True),
        T.StructField("p50", T.DoubleType(), True),
        T.StructField("p75", T.DoubleType(), True),
        T.StructField("p99", T.DoubleType(), True),
    ])

    return spark.createDataFrame(rows, schema)


def categorical_summary(spark, loaded_tables):
    rows = []

    for table_name, df in loaded_tables.items():
        string_columns = [
            column_name
            for column_name, dtype in df.dtypes
            if dtype == "string"
        ]

        for column_name in string_columns:
            stats = (
                df.agg(
                    F.approx_count_distinct(F.col(column_name)).alias("approx_distinct_count")
                )
                .collect()[0]
            )

            rows.append((
                table_name,
                column_name,
                int(stats["approx_distinct_count"] or 0),
            ))

    schema = T.StructType([
        T.StructField("table_name", T.StringType(), False),
        T.StructField("column_name", T.StringType(), False),
        T.StructField("approx_distinct_count", T.LongType(), False),
    ])

    return spark.createDataFrame(rows, schema)


def main():
    spark = (
        SparkSession.builder
        .appName("team15_stage2_data_profiling")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )

    loaded_tables = {}

    for table_name, path in TABLES.items():
        print("Reading {} from {}".format(table_name, path))
        loaded_tables[table_name] = spark.read.parquet(path)

    write_csv(table_overview(spark, loaded_tables), "table_overview")
    write_csv(column_types(spark, loaded_tables), "column_types")
    write_csv(missingness(spark, loaded_tables), "missingness")
    write_csv(numeric_summary(spark, loaded_tables), "numeric_summary")
    write_csv(numeric_quantiles(spark, loaded_tables), "numeric_quantiles")
    write_csv(categorical_summary(spark, loaded_tables), "categorical_summary")

    spark.stop()


if __name__ == "__main__":
    main()
