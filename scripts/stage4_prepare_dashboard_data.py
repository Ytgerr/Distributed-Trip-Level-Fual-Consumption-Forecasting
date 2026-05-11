#!/usr/bin/env python3
"""prepare stage4 dashboard tables"""

import os
import sys

import pandas as pd
import psycopg2 as psql


PSQL_HOST = "hadoop-04.uni.innopolis.ru"
PSQL_PORT = 5432
PSQL_USER = "team15"
PSQL_DB = "team15_projectdb"
PSQL_PASS_FILE = os.path.join("secrets", ".psql.pass")

OUTPUT_DIR = os.path.join("output", "stage4")
EVALUATION_FILE = os.path.join("output", "evaluation.csv")
MODEL1_PREDICTIONS_FILE = os.path.join("output", "model1_predictions.csv")
MODEL2_PREDICTIONS_FILE = os.path.join("output", "model2_predictions.csv")
PREDICTION_SAMPLE_LIMIT = 5000

FEATURE_ROWS = [
    ("duration_min", "trip_time", "numeric", "trip-level duration", "feature"),
    ("observed_seconds", "trip_time", "numeric", "valid observed trip time", "feature"),
    ("distance_km", "trip_distance", "numeric", "speed-time integration", "feature"),
    ("speed_mean", "speed_profile", "numeric", "trip-level average speed", "feature"),
    ("speed_median", "speed_profile", "numeric", "trip-level median speed", "feature"),
    ("speed_p95", "speed_profile", "numeric", "95th speed percentile", "feature"),
    ("stop_go_ratio", "driving_behavior", "numeric", "share of stop-go time", "feature"),
    ("idle_time_min", "driving_behavior", "numeric", "idle time in minutes", "feature"),
    ("maf_mean", "engine_signal", "numeric", "average mass air flow", "feature"),
    ("maf_p95", "engine_signal", "numeric", "95th mass air flow percentile", "feature"),
    ("rpm_mean", "engine_signal", "numeric", "average engine rpm", "feature"),
    ("rpm_p95", "engine_signal", "numeric", "95th engine rpm percentile", "feature"),
    ("abs_load_mean", "engine_signal", "numeric", "average engine load", "feature"),
    ("oat_mean", "environment", "numeric", "average outside air temperature", "feature"),
    ("hv_current_mean", "hybrid_battery", "numeric", "average HV battery current", "feature"),
    ("hv_soc_mean", "hybrid_battery", "numeric", "average HV battery state of charge", "feature"),
    ("hv_voltage_mean", "hybrid_battery", "numeric", "average HV battery voltage", "feature"),
    ("vehtype", "vehicle_metadata", "categorical", "static vehicle type", "feature"),
    ("vehclass", "vehicle_metadata", "categorical", "static vehicle class", "feature"),
    ("transmission", "vehicle_metadata", "categorical", "static transmission type", "feature"),
    ("drive_wheels", "vehicle_metadata", "categorical", "static drivetrain type", "feature"),
    ("gen_weight", "vehicle_metadata", "numeric", "static vehicle weight", "feature"),
    ("eng_type", "vehicle_metadata", "categorical", "static engine type", "feature"),
    ("eng_dis", "vehicle_metadata", "numeric", "engine displacement", "feature"),
    ("eng_conf", "vehicle_metadata", "categorical", "engine configuration", "feature"),
    ("fuel_l_per_100km", "target", "numeric", "trip fuel consumption", "target"),
]

HYPERPARAMETER_ROWS = [
    ("Linear Regression", "elasticNetParam", "0.0, 0.5, 1.0", "selected by CrossValidator", "3", "RMSE"),
    ("Linear Regression", "regParam", "0.01, 0.1, 1.0", "selected by CrossValidator", "3", "RMSE"),
    ("Gradient-Boosted Trees", "maxDepth", "3, 5", "selected by CrossValidator", "3", "RMSE"),
    ("Gradient-Boosted Trees", "stepSize", "0.05, 0.1", "selected by CrossValidator", "3", "RMSE"),
]

CLEANING_ROWS = [
    (1, "source separation", "Raw data was stored as vehicles metadata and trips telemetry tables."),
    (2, "engine displacement extraction", "Engine displacement was extracted into the eng_dis column."),
    (3, "time conversion", "Trip time was converted from milliseconds to seconds during Stage 2."),
    (4, "missing values", "Floating-point NaN values were converted to NULL before analytics."),
    (5, "fuel-rate approximation", "Missing fuel_rate values were approximated from MAF when possible."),
    (6, "trip-level aggregation", "Sensor-level telemetry was aggregated into one row per trip."),
]


def read_password():
    with open(PSQL_PASS_FILE, "r", encoding="utf-8") as file:
        return file.read().strip()


def get_psql_connection():
    return psql.connect(
        host=PSQL_HOST,
        port=PSQL_PORT,
        user=PSQL_USER,
        dbname=PSQL_DB,
        password=read_password(),
    )


def clean_text_values(data):
    result = data.copy()

    for col_name in result.columns:
        if result[col_name].dtype == object:
            result[col_name] = (
                result[col_name]
                .astype(str)
                .str.replace("\t", " ", regex=False)
                .str.replace("\n", " ", regex=False)
                .str.replace("\r", " ", regex=False)
            )
            result[col_name] = result[col_name].replace("nan", "")

    return result.where(pd.notnull(result), "")


def write_tsv(data, name):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    path = os.path.join(OUTPUT_DIR, f"{name}.tsv")
    clean_text_values(data).to_csv(path, sep="\t", index=False)
    print("saved:", path)


def read_sql(query):
    with get_psql_connection() as conn:
        return pd.read_sql_query(query, conn)


def normalize_model_name(value):
    value = str(value)

    if "LinearRegression" in value:
        return "Linear Regression"

    if "GBTRegression" in value or "GBTRegressor" in value:
        return "Gradient-Boosted Trees"

    return value


def build_psql_table_counts():
    query = """
        SELECT 'vehicles' AS table_name, COUNT(*) AS records_count
        FROM vehicles
        UNION ALL
        SELECT 'trips' AS table_name, COUNT(*) AS records_count
        FROM trips
    """
    return read_sql(query)


def build_psql_column_datatypes():
    query = """
        SELECT
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ('vehicles', 'trips')
        ORDER BY table_name, ordinal_position
    """
    return read_sql(query)


def build_vehicles_sample():
    query = """
        SELECT
            vehid,
            vehtype,
            vehclass,
            transmission,
            drive_wheels,
            gen_weight,
            eng_type,
            eng_dis,
            eng_conf
        FROM vehicles
        LIMIT 20
    """
    return read_sql(query)


def build_trips_sample():
    query = """
        SELECT
            daynum,
            vehid,
            tripid,
            time AS time_raw_ms,
            speed,
            maf,
            rpm,
            fuel_rate,
            oat
        FROM trips
        LIMIT 20
    """
    return read_sql(query)


def build_data_cleaning_summary():
    return pd.DataFrame(
        CLEANING_ROWS,
        columns=["step_order", "step_name", "description"],
    )


def build_feature_extraction_characteristics():
    return pd.DataFrame(
        FEATURE_ROWS,
        columns=[
            "feature_name",
            "feature_group",
            "feature_type",
            "transformation",
            "model_usage",
        ],
    )


def build_feature_group_summary(features):
    return (
        features
        .groupby("feature_group", as_index=False)
        .agg(features_count=("feature_name", "count"))
        .sort_values("feature_group")
    )


def build_hyperparameter_optimization():
    return pd.DataFrame(
        HYPERPARAMETER_ROWS,
        columns=[
            "model_name",
            "param_name",
            "tested_values",
            "best_value",
            "cv_folds",
            "optimization_metric",
        ],
    )


def build_model_evaluation():
    data = pd.read_csv(EVALUATION_FILE)

    result = pd.DataFrame()
    result["model_name"] = data["model"].apply(normalize_model_name)
    result["rmse"] = pd.to_numeric(data["RMSE"], errors="coerce")
    result["mae"] = pd.to_numeric(data["MAE"], errors="coerce")
    result["r2"] = pd.to_numeric(data["R2"], errors="coerce")

    return result


def build_model_comparison(evaluation):
    rows = []

    for _, row in evaluation.iterrows():
        rows.append((row["model_name"], "RMSE", row["rmse"]))
        rows.append((row["model_name"], "MAE", row["mae"]))
        rows.append((row["model_name"], "R2", row["r2"]))

    return pd.DataFrame(rows, columns=["model_name", "metric", "metric_value"])


def build_best_model(evaluation):
    best = evaluation.sort_values("rmse", ascending=True).iloc[0]

    return pd.DataFrame(
        [
            (
                best["model_name"],
                "RMSE",
                best["rmse"],
                best["rmse"],
                best["mae"],
                best["r2"],
            )
        ],
        columns=[
            "model_name",
            "selection_metric",
            "selection_value",
            "rmse",
            "mae",
            "r2",
        ],
    )


def read_predictions(path, model_name):
    data = pd.read_csv(path)
    data.columns = [col_name.strip() for col_name in data.columns]

    if data.empty:
        raise RuntimeError(f"prediction file has no rows: {path}")

    for col_name in ["label", "prediction"]:
        if col_name not in data.columns:
            raise RuntimeError(f"missing column {col_name} in {path}")

    result = pd.DataFrame()
    result["model_name"] = model_name
    result["row_id"] = range(1, len(data) + 1)
    result["label"] = pd.to_numeric(data["label"], errors="coerce")
    result["prediction"] = pd.to_numeric(data["prediction"], errors="coerce")
    result["error"] = result["prediction"] - result["label"]
    result["abs_error"] = result["error"].abs()
    result = result.dropna(subset=["label", "prediction"])

    if result.empty:
        raise RuntimeError(f"prediction values cannot be parsed: {path}")

    print("prediction source:", path, "raw rows:", len(data), "usable rows:", len(result))
    return result.head(PREDICTION_SAMPLE_LIMIT)


def build_prediction_samples():
    model1 = read_predictions(MODEL1_PREDICTIONS_FILE, "Linear Regression")
    model2 = read_predictions(MODEL2_PREDICTIONS_FILE, "Gradient-Boosted Trees")

    return pd.concat([model1, model2], ignore_index=True), model1, model2


def check_inputs():
    for path in [
        EVALUATION_FILE,
        MODEL1_PREDICTIONS_FILE,
        MODEL2_PREDICTIONS_FILE,
        PSQL_PASS_FILE,
    ]:
        if not os.path.exists(path):
            raise RuntimeError(f"missing required file: {path}")


def main():
    check_inputs()

    print("building psql source tables")
    write_tsv(build_psql_table_counts(), "stage4_psql_table_counts")
    write_tsv(build_psql_column_datatypes(), "stage4_psql_column_datatypes")
    write_tsv(build_vehicles_sample(), "stage4_psql_vehicles_sample")
    write_tsv(build_trips_sample(), "stage4_psql_trips_sample")
    write_tsv(build_data_cleaning_summary(), "stage4_data_cleaning_summary")

    print("building ml tables")
    features = build_feature_extraction_characteristics()
    evaluation = build_model_evaluation()
    comparison = build_model_comparison(evaluation)
    best_model = build_best_model(evaluation)
    predictions, model1_predictions, model2_predictions = build_prediction_samples()

    write_tsv(features, "stage4_feature_extraction_characteristics")
    write_tsv(build_feature_group_summary(features), "stage4_feature_group_summary")
    write_tsv(build_hyperparameter_optimization(), "stage4_hyperparameter_optimization")
    write_tsv(evaluation, "stage4_model_evaluation")
    write_tsv(comparison, "stage4_model_comparison")
    write_tsv(best_model, "stage4_best_model")
    write_tsv(predictions, "stage4_prediction_samples")
    write_tsv(model1_predictions, "stage4_model1_predictions")
    write_tsv(model2_predictions, "stage4_model2_predictions")

    print("stage4 local files are ready:", OUTPUT_DIR)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        print("ERROR:", error, file=sys.stderr)
        sys.exit(1)
