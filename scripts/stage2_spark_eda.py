#!/usr/bin/env python3

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


WAREHOUSE = "/user/team15/project/warehouse" # hdfs path with raw parquet data from stage 1; change if ingestion path changes
ANALYTICS_HDFS = "/user/team15/project/analytics" # hdfs path for analytical parquet outputs; change if another warehouse layer is required
ANALYTICS_CSV_HDFS = "/user/team15/project/analytics_csv" # hdfs path for small csv outputs; change if report/dashboard exports should go elsewhere
SPARK_APP_NAME = "stage2-data-science-eda" # spark application name; change only for clearer yarn tracking
SPARK_LOG_LEVEL = "WARN"

GASOLINE_DENSITY_G_PER_L = 745.0 # average gasoline density in g/L; tune if fuel type assumptions change
STOICH_AFR = 14.7 # gasoline stoichiometric air-fuel ratio; use 14.7 for standard gasoline approximation

MAX_VALID_DT_S = 3600.0 # max valid time gap inside one trip in seconds; lower value removes long telemetry breaks
STOP_GO_SPEED_KMH = 5.0 # speed below this threshold is treated as stop-go traffic
IDLE_SPEED_KMH = 0.1 # speed below this threshold is treated as idling

MIN_VALID_SPEED_KMH = 0.0 # lower bound for speed-bin EDA; change if reverse/invalid speeds should be analyzed
MAX_VALID_SPEED_KMH = 160.0 # upper bound for speed-bin EDA; change if high-speed outliers should be kept
SPEED_BIN_SIZE_KMH = 10.0 # speed bin width for speed-based EDA charts
CITY_SPEED_LIMIT_KMH = 30.0 # upper speed threshold for city driving mode
NORMAL_SPEED_LIMIT_KMH = 80.0 # upper speed threshold for normal driving mode

OAT_BIN_SIZE_C = 5.0 # temperature bin width for outside-air-temperature EDA
WATTS_PER_KW = 1000.0 # converts watts to kilowatts for hvac power fields

GEO_ROUND_DIGITS = 2 # coordinate rounding precision for coarse geo activity grid
MAX_GEO_ROWS = 5000 # max number of geo grid cells exported to csv

MIN_TRIP_DISTANCE_KM = 0.5 # minimum trip distance kept in trip-level features; increase to remove short noisy trips

TRIP_FEATURE_SAMPLE_SIZE = 5000 # number of longest trips exported as a local csv sample

NO_DATA_LABEL = "NO DATA" # fallback label for missing vehicle type


def clean_nan_values(df):
    """convert floating-point nan values to null values."""
    for col_name, col_type in df.dtypes:
        if col_type in ("double", "float"):
            df = df.withColumn(
                col_name,
                F.when(F.isnan(F.col(col_name)), F.lit(None)).otherwise(F.col(col_name)),
            )

    return df


def write_parquet(df, name):
    path = "{}/{}".format(ANALYTICS_HDFS, name)
    print("Writing parquet:", path)

    df.write.mode("overwrite").parquet(path)


def write_small_csv(df, name):
    path = "{}/{}".format(ANALYTICS_CSV_HDFS, name)
    print("Writing csv:", path)
    
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )


def write_output(df, name, write_csv=True):
    write_parquet(df, name)

    if write_csv:
        write_small_csv(df, name)


spark = (SparkSession.builder
    .appName(SPARK_APP_NAME)
    .getOrCreate()
    )

spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

vehicles_raw = spark.read.parquet("{}/vehicles".format(WAREHOUSE))
trips_raw = spark.read.parquet("{}/trips".format(WAREHOUSE))

raw_trip_columns_count = len(trips_raw.columns)
raw_vehicle_columns_count = len(vehicles_raw.columns)

vehicles = (clean_nan_values(vehicles_raw)
    .withColumn("vehid", F.col("vehid").cast("string"))
    )

trips = (clean_nan_values(trips_raw)
    .withColumn("vehid", F.col("vehid").cast("string"))
    .withColumn("tripid", F.col("tripid").cast("string"))
    .withColumn("time_raw_ms", F.col("time").cast("double"))
    .withColumn("time_seconds", F.col("time_raw_ms") / F.lit(1000))
    )

# enrich telemetry rows with static vehicle metadata and a unified fuel-rate signal
trips_enriched = (
    trips.alias("t")
    .join(vehicles.alias("v"), on="vehid", how="left")
    .withColumn(
        "fuel_rate_est_lhr",
        F.when(F.col("fuel_rate").isNotNull(), F.col("fuel_rate").cast("double"))
        .when(F.col("maf").isNotNull(),
            F.col("maf")
            / F.lit(STOICH_AFR)
            / F.lit(GASOLINE_DENSITY_G_PER_L)
            * F.lit(3600),
        )
        .otherwise(F.lit(None).cast("double")),
        )
    .withColumn("fuel_rate_est_lhr",
        F.when(F.isnan(F.col("fuel_rate_est_lhr")), F.lit(None))
        .otherwise(F.col("fuel_rate_est_lhr")),
    )
    .withColumn("is_stop_go",
        F.when(F.col("speed") < STOP_GO_SPEED_KMH, F.lit(1)).otherwise(F.lit(0)),
    )
    .withColumn("is_idle",
        F.when(F.col("speed") < IDLE_SPEED_KMH, F.lit(1)).otherwise(F.lit(0)),
    )
)

write_output(vehicles, "vehicles_raw", write_csv=True)
write_output(trips, "trips_raw", write_csv=False)
write_output(trips_enriched, "trips_enriched", write_csv=False)

trips_rows = trips.count()
vehicles_rows = vehicles.count()
unique_vehicles = trips.select("vehid").distinct().count()
unique_trips = trips.select("vehid", "tripid").distinct().count()

eda_data_characteristics = spark.createDataFrame(
    [("trips_rows", str(trips_rows)),
        ("vehicles_rows", str(vehicles_rows)),
        ("unique_vehicles_in_trips", str(unique_vehicles)),
        ("unique_trips", str(unique_trips)),
        ("trip_columns_count", str(raw_trip_columns_count)),
        ("vehicle_columns_count", str(raw_vehicle_columns_count)),
    ],
    ["metric", "value"],
)

write_output(eda_data_characteristics, "eda_data_characteristics", write_csv=True)

missing_features = [
    "speed",
    "maf",
    "rpm",
    "abs_load",
    "oat",
    "fuel_rate",
    "air_cp_kw",
    "air_cp_watts",
    "heater_power",
    "hv_battery_current",
    "hv_battery_soc",
    "hv_battery_vol",
    "stfb_1",
    "stfb_2",
    "ltfb_1",
    "ltfb_2",
]

existing_missing_features = [
    feature for feature in missing_features
    if feature in trips.columns
]

missing_aggs = [
    F.sum(F.when(F.col(feature).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(feature)
    for feature in existing_missing_features
]

missing_row = trips.agg(*missing_aggs).collect()[0]

missing_rows = []

for feature in existing_missing_features:
    null_count = int(missing_row[feature])
    null_share = float(null_count) / float(trips_rows) if trips_rows > 0 else 0.0
    missing_rows.append((feature, int(trips_rows), null_count, null_share))

eda_missingness = spark.createDataFrame(
    missing_rows,
    ["feature", "rows_total", "null_count", "null_share"],
)

write_output(eda_missingness, "eda_missingness", write_csv=True)

insight_01 = (
    trips_enriched
    .groupBy(F.coalesce(F.col("vehtype"), F.lit(NO_DATA_LABEL)).alias("vehicle_type"))
    .agg(F.count("*").alias("sensor_rows"),
        F.countDistinct("vehid").alias("vehicles_count"),
        F.countDistinct("vehid", "tripid").alias("trips_count"),
    )
    .orderBy(F.desc("sensor_rows"))
)

write_output(insight_01, "insight_01_vehicle_type_distribution", write_csv=True)

insight_02 = (
    trips_enriched
    .where(F.col("fuel_rate_est_lhr").isNotNull())
    .groupBy(F.coalesce(F.col("vehtype"), F.lit(NO_DATA_LABEL)).alias("vehicle_type"))
    .agg(F.count("*").alias("rows_count"),
        F.avg("fuel_rate_est_lhr").alias("avg_fuel_rate_lhr"),
        F.expr("percentile_approx(fuel_rate_est_lhr, {})".format(0.5)).alias("median_fuel_rate_lhr"),
        F.expr("percentile_approx(fuel_rate_est_lhr, {})".format(0.9)).alias("p90_fuel_rate_lhr"),
    )
    .orderBy(F.desc("rows_count"))
)

write_output(insight_02, "insight_02_fuel_by_vehicle_type", write_csv=True)

insight_03 = (
    trips_enriched
    .where((F.col("speed").isNotNull())
        & (F.col("fuel_rate_est_lhr").isNotNull())
        & (F.col("speed").between(MIN_VALID_SPEED_KMH, MAX_VALID_SPEED_KMH))
    )
    .withColumn("speed_bin_kmh",
        (F.floor(F.col("speed") / SPEED_BIN_SIZE_KMH) * SPEED_BIN_SIZE_KMH).cast("int"),
    )
    .groupBy("speed_bin_kmh")
    .agg(F.count("*").alias("rows_count"),
        F.avg("speed").alias("avg_speed_kmh"),
        F.avg("fuel_rate_est_lhr").alias("avg_fuel_rate_lhr"),
        F.avg("rpm").alias("avg_rpm"),
        F.avg("abs_load").alias("avg_engine_load"),
    )
    .orderBy("speed_bin_kmh")
)

write_output(insight_03, "insight_03_fuel_by_speed_bin", write_csv=True)

# define interpretable driving modes from speed thresholds
driving_mode = (F.when(F.col("speed") < STOP_GO_SPEED_KMH, F.lit("stop_go_under_5_kmh"))
    .when(F.col("speed") < CITY_SPEED_LIMIT_KMH, F.lit("city_5_30_kmh"))
    .when(F.col("speed") < NORMAL_SPEED_LIMIT_KMH, F.lit("normal_30_80_kmh"))
    .otherwise(F.lit("highway_80_plus_kmh"))
)

insight_04 = (
    trips_enriched
    .where((F.col("speed").isNotNull()) & (F.col("fuel_rate_est_lhr").isNotNull()))
    .withColumn("driving_mode", driving_mode)
    .groupBy("driving_mode")
    .agg(
        F.count("*").alias("rows_count"),F.avg("fuel_rate_est_lhr").alias("avg_fuel_rate_lhr"),
        F.avg("rpm").alias("avg_rpm"),
        F.avg("abs_load").alias("avg_engine_load"),
        F.avg("speed").alias("avg_speed_kmh"),
    )
    .orderBy("driving_mode")
)

write_output(insight_04, "insight_04_stop_go_vs_fuel", write_csv=True)

insight_05 = (trips_enriched
    .where(F.col("oat").isNotNull())
    .withColumn(
        "oat_bin_c",
        (F.floor(F.col("oat") / OAT_BIN_SIZE_C) * OAT_BIN_SIZE_C).cast("int"),
    )
    .groupBy("oat_bin_c")
    .agg(F.count("*").alias("rows_count"),
        F.avg("fuel_rate_est_lhr").alias("avg_fuel_rate_lhr"),
        F.avg(F.coalesce(F.col("air_cp_kw"), F.col("air_cp_watts") / WATTS_PER_KW)).alias("avg_ac_kw"),
        F.avg(F.col("heater_power") / WATTS_PER_KW).alias("avg_heater_kw"),
    )
    .orderBy("oat_bin_c")
)

write_output(insight_05, "insight_05_temperature_hvac_fuel", write_csv=True)

insight_06 = (trips_enriched
    .where((F.col("eng_dis").isNotNull()) & (F.col("fuel_rate_est_lhr").isNotNull()))
    .groupBy("eng_dis")
    .agg(F.count("*").alias("rows_count"),
        F.countDistinct("vehid").alias("vehicles_count"),
        F.avg("fuel_rate_est_lhr").alias("avg_fuel_rate_lhr"),
        F.avg("speed").alias("avg_speed_kmh"),
        F.avg("rpm").alias("avg_rpm"),
    )
    .orderBy("eng_dis")
)

write_output(insight_06, "insight_06_engine_displacement_fuel", write_csv=True)

insight_07 = (trips_enriched
    .where((F.col("lat").isNotNull()) & (F.col("_long").isNotNull()))
    .withColumn("lat_bin", F.round(F.col("lat"), GEO_ROUND_DIGITS))
    .withColumn("long_bin", F.round(F.col("_long"), GEO_ROUND_DIGITS))
    .groupBy("lat_bin", "long_bin")
    .agg(F.count("*").alias("rows_count"),
        F.countDistinct("vehid", "tripid").alias("trips_count"),
        F.avg("speed").alias("avg_speed_kmh"),
        F.avg("fuel_rate_est_lhr").alias("avg_fuel_rate_lhr"),
    )
    .orderBy(F.desc("rows_count"))
    .limit(MAX_GEO_ROWS)
)

write_output(insight_07, "insight_07_geo_activity", write_csv=True)

# integrate sensor rows into trip-level features
window_spec = Window.partitionBy("vehid", "tripid").orderBy(F.col("time_seconds"), F.col("time"))

trips_for_features = (trips_enriched
    .withColumn("next_time_seconds", F.lead("time_seconds").over(window_spec))
    .withColumn("next_speed", F.lead("speed").over(window_spec))
    .withColumn("dt_s", F.col("next_time_seconds") - F.col("time_seconds"))
    .withColumn("dt_s",
        F.when((F.col("dt_s") > 0) & (F.col("dt_s") < MAX_VALID_DT_S), F.col("dt_s"))
        .otherwise(F.lit(0.0)),
    )
    .withColumn("speed_clean", F.coalesce(F.col("speed"), F.lit(0.0)))
    .withColumn("distance_km_segment", F.col("speed_clean") * F.col("dt_s") / F.lit(3600))
    .withColumn("fuel_l_segment", 
            F.when(
            F.col("fuel_rate_est_lhr").isNotNull(),
            F.col("fuel_rate_est_lhr") * F.col("dt_s") / F.lit(3600),
        ),
    )
    .withColumn("stop_go_s", F.when(F.col("speed_clean") < STOP_GO_SPEED_KMH, F.col("dt_s")).otherwise(F.lit(0.0)))
    .withColumn("idle_s", F.when(F.col("speed_clean") < IDLE_SPEED_KMH, F.col("dt_s")).otherwise(F.lit(0.0)))
)

trip_features = (
    trips_for_features
    .groupBy("vehid", "tripid")
    .agg(
        F.min("daynum").alias("daynum"),
        F.count("*").alias("n_sensor_points"),
        ((F.max("time_seconds") - F.min("time_seconds")) / F.lit(60)).alias("duration_min"),
        F.sum("dt_s").alias("observed_seconds"),
        F.sum("distance_km_segment").alias("distance_km"),
        F.sum("fuel_l_segment").alias("fuel_used_l"),
        F.sum(F.when(F.col("fuel_rate_est_lhr").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("fuel_valid_points"),
        F.sum("stop_go_s").alias("stop_go_s"),
        F.sum("idle_s").alias("idle_s"),
        F.avg("speed").alias("speed_mean"),
        F.expr("percentile_approx(speed, {})".format(0.5)).alias("speed_median"),
        F.expr("percentile_approx(speed, {})".format(0.95)).alias("speed_p95"),
        F.avg("maf").alias("maf_mean"),
        F.expr("percentile_approx(maf, {})".format(0.95)).alias("maf_p95"),
        F.avg("rpm").alias("rpm_mean"),
        F.expr("percentile_approx(rpm, {})".format(0.95)).alias("rpm_p95"),
        F.avg("abs_load").alias("abs_load_mean"),
        F.avg("oat").alias("oat_mean"),
        F.avg("hv_battery_current").alias("hv_current_mean"),
        F.avg("hv_battery_soc").alias("hv_soc_mean"),
        F.avg("hv_battery_vol").alias("hv_voltage_mean"),
        F.avg("fuel_rate_est_lhr").alias("fuel_rate_mean_lhr"),
        F.first("vehtype", ignorenulls=True).alias("vehtype"),
        F.first("vehclass", ignorenulls=True).alias("vehclass"),
        F.first("transmission", ignorenulls=True).alias("transmission"),
        F.first("drive_wheels", ignorenulls=True).alias("drive_wheels"),
        F.first("gen_weight", ignorenulls=True).alias("gen_weight"),
        F.first("eng_type", ignorenulls=True).alias("eng_type"),
        F.first("eng_dis", ignorenulls=True).alias("eng_dis"),
        F.first("eng_conf", ignorenulls=True).alias("eng_conf"),
    )
    .withColumn("trip_id", F.concat_ws("_", F.col("vehid"), F.col("tripid")))
    .withColumn("stop_go_ratio", F.when(F.col("observed_seconds") > 0, F.col("stop_go_s") / F.col("observed_seconds")))
    .withColumn("idle_time_min", F.col("idle_s") / F.lit(60))
    .withColumn(
        "fuel_l_per_100km",
        F.when(F.col("distance_km") > 0, F.lit(100) * F.col("fuel_used_l") / F.col("distance_km")),
    )
    .drop("stop_go_s", "idle_s")
    .where(F.col("distance_km") >= MIN_TRIP_DISTANCE_KM)
)

write_output(trip_features, "trip_features", write_csv=False)

trip_features_sample = trip_features.orderBy(F.desc("distance_km")).limit(TRIP_FEATURE_SAMPLE_SIZE)
write_small_csv(trip_features_sample, "trip_features_sample")

print("Stage 2 completed.")
print("Outputs:")
print("Parquet:", ANALYTICS_HDFS)
print("CSV:", ANALYTICS_CSV_HDFS)

spark.stop()
