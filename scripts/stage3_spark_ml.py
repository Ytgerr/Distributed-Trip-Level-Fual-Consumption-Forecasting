#!/usr/bin/env python3
"""Stage 3 – Spark ML pipeline for trip-level fuel consumption forecasting.

Target  : fuel_l_per_100km
Models  : 1) LinearRegression  (model1)
          2) GBTRegressor       (model2)
"""

import os

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TEAM = "team15"
WAREHOUSE = "project/hive/warehouse"

TRIP_FEATURES_HDFS = "/user/team15/project/analytics/trip_features"

HDFS_DATA_TRAIN = "project/data/train"
HDFS_DATA_TEST = "project/data/test"
HDFS_MODEL1 = "project/models/model1"
HDFS_MODEL2 = "project/models/model2"
HDFS_PRED1 = "project/output/model1_predictions"
HDFS_PRED2 = "project/output/model2_predictions"
HDFS_EVAL = "project/output/evaluation"

LOCAL_DATA_DIR = "data"
LOCAL_OUTPUT_DIR = "output"
LOCAL_MODEL1_DIR = "models/model1"
LOCAL_MODEL2_DIR = "models/model2"

RANDOM_SEED = 42
TRAIN_RATIO = 0.7
TEST_RATIO = 0.3
CV_FOLDS = 3
CV_PARALLELISM = 4

TARGET = "fuel_l_per_100km"

NUMERIC_FEATURES = [
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

CATEGORICAL_FEATURES = [
    "vehtype",
    "vehclass",
    "transmission",
    "drive_wheels",
    "eng_type",
    "eng_conf",
]


def run(command):
    "Short-cut function for programm open-reading"
    return os.popen(command).read()


spark = (
    SparkSession.builder.appName(f"{TEAM} - spark ML")
    .master("yarn")
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
    .config("spark.sql.warehouse.dir", WAREHOUSE)
    .config("spark.sql.avro.compression.codec", "snappy")
    .config("spark.sql.shuffle.partitions", "32")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Stage 3 – Spark ML: fuel consumption forecasting")
print("=" * 60)

df = spark.read.parquet(TRIP_FEATURES_HDFS)
df.printSchema()
print("Total rows:", df.count())

existing_numeric = [c for c in NUMERIC_FEATURES if c in df.columns]
existing_categorical = [c for c in CATEGORICAL_FEATURES if c in df.columns]

print("Numeric features    :", existing_numeric)
print("Categorical features:", existing_categorical)

df_ml = (
    df.select(existing_numeric + existing_categorical + [TARGET])
    .where(F.col(TARGET).isNotNull())
    .where(F.col(TARGET) > 0)
    .withColumnRenamed(TARGET, "label")
)

print("Rows after target filter:", df_ml.count())
df_ml.show(5, truncate=False)

imputer = Imputer(
    inputCols=existing_numeric,
    outputCols=[f"{c}_imp" for c in existing_numeric],
    strategy="median",
)
imputed_numeric_cols = [f"{c}_imp" for c in existing_numeric]

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in existing_categorical
]
encoders = [
    OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe", dropLast=True)
    for c in existing_categorical
]
ohe_cols = [f"{c}_ohe" for c in existing_categorical]

assembler = VectorAssembler(
    inputCols=imputed_numeric_cols + ohe_cols,
    outputCol="features_raw",
    handleInvalid="keep",
)
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True,
)

prep_pipeline = Pipeline(stages=[imputer] + indexers + encoders + [assembler, scaler])

train_raw, test_raw = df_ml.randomSplit([TRAIN_RATIO, TEST_RATIO], seed=RANDOM_SEED)
print("Train rows:", train_raw.count(), "| Test rows:", test_raw.count())

prep_model = prep_pipeline.fit(train_raw)
train_data = prep_model.transform(train_raw).select("features", "label")
test_data = prep_model.transform(test_raw).select("features", "label")

train_data.cache()
test_data.cache()

train_data.coalesce(1).write.mode("overwrite").format("json").save(HDFS_DATA_TRAIN)
test_data.coalesce(1).write.mode("overwrite").format("json").save(HDFS_DATA_TEST)

os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
run(f"hdfs dfs -getmerge {HDFS_DATA_TRAIN}//*.json {LOCAL_DATA_DIR}/train.json")
run(f"hdfs dfs -getmerge {HDFS_DATA_TEST}//*.json {LOCAL_DATA_DIR}/test.json")

evaluator_rmse = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse"
)
evaluator_mae = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="mae"
)
evaluator_r2 = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="r2"
)

print("\n[Model 1] LinearRegression")

lr = LinearRegression(featuresCol="features", labelCol="label", maxIter=100)

param_grid_lr = (
    ParamGridBuilder()
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    .addGrid(lr.regParam, [0.01, 0.1, 1.0])
    .build()
)

cv_lr = CrossValidator(
    estimator=lr,
    estimatorParamMaps=param_grid_lr,
    evaluator=evaluator_rmse,
    numFolds=CV_FOLDS,
    parallelism=CV_PARALLELISM,
    seed=RANDOM_SEED,
)

cv_model_lr = cv_lr.fit(train_data)
model1 = cv_model_lr.bestModel

print("  elasticNetParam =", model1.getElasticNetParam())
print("  regParam        =", model1.getRegParam())

predictions1 = model1.transform(test_data)
rmse1 = evaluator_rmse.evaluate(predictions1)
mae1 = evaluator_mae.evaluate(predictions1)
r2_1 = evaluator_r2.evaluate(predictions1)

print(f"  RMSE={rmse1:.4f}  MAE={mae1:.4f}  R2={r2_1:.4f}")

model1.write().overwrite().save(HDFS_MODEL1)
run(f"hdfs dfs -get {HDFS_MODEL1} {LOCAL_MODEL1_DIR}")

os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
(
    predictions1.select("label", "prediction")
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .save(HDFS_PRED1)
)
run(f"hdfs dfs -getmerge {HDFS_PRED1}//*.csv {LOCAL_OUTPUT_DIR}/model1_predictions.csv")

print("\n[Model 2] GBTRegressor")

gbt = GBTRegressor(
    featuresCol="features", labelCol="label", maxIter=50, seed=RANDOM_SEED
)

param_grid_gbt = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [3, 5])
    .addGrid(gbt.stepSize, [0.05, 0.1])
    .build()
)

cv_gbt = CrossValidator(
    estimator=gbt,
    estimatorParamMaps=param_grid_gbt,
    evaluator=evaluator_rmse,
    numFolds=CV_FOLDS,
    parallelism=CV_PARALLELISM,
    seed=RANDOM_SEED,
)

cv_model_gbt = cv_gbt.fit(train_data)
model2 = cv_model_gbt.bestModel

print("  maxDepth =", model2.getMaxDepth())
print("  stepSize =", model2.getStepSize())

predictions2 = model2.transform(test_data)
rmse2 = evaluator_rmse.evaluate(predictions2)
mae2 = evaluator_mae.evaluate(predictions2)
r2_2 = evaluator_r2.evaluate(predictions2)

print(f"  RMSE={rmse2:.4f}  MAE={mae2:.4f}  R2={r2_2:.4f}")

model2.write().overwrite().save(HDFS_MODEL2)
run(f"hdfs dfs -get {HDFS_MODEL2} {LOCAL_MODEL2_DIR}")

(
    predictions2.select("label", "prediction")
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .save(HDFS_PRED2)
)
run(f"hdfs dfs -getmerge {HDFS_PRED2}//*.csv {LOCAL_OUTPUT_DIR}/model2_predictions.csv")

print("\n[Comparison]")

eval_df = spark.createDataFrame(
    [
        (str(model1), float(rmse1), float(mae1), float(r2_1)),
        (str(model2), float(rmse2), float(mae2), float(r2_2)),
    ],
    ["model", "RMSE", "MAE", "R2"],
)
eval_df.show(truncate=False)

(
    eval_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .save(HDFS_EVAL)
)
run(f"hdfs dfs -getmerge {HDFS_EVAL}//*.csv {LOCAL_OUTPUT_DIR}/evaluation.csv")

print("\n" + "=" * 60)
print("Stage 3 completed.")
print("=" * 60)

spark.stop()
