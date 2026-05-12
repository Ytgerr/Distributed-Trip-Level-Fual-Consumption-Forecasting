#!/bin/bash
set -euo pipefail

mkdir -p output models

spark-submit --master yarn scripts/stage3_train_models.py \
  2>&1 | tee output/stage3_train_models_run.log

echo "Training outputs:"
ls -lh output/evaluation.csv \
       output/model_training_log.csv \
       output/model_cv_results.csv \
       output/best_model.csv \
       output/specific_prediction.csv \
       output/model1_predictions.csv \
       output/model2_predictions.csv \
       output/model3_predictions.csv

echo "Models:"
ls -lh models

echo "Refreshing specific prediction sample from best model..."
python3 - <<'PY'
import csv

best_model_file = "output/best_model.csv"

with open(best_model_file, "r", encoding="utf-8") as f:
    best = next(csv.DictReader(f))

model_index = best["model_index"]
model_name = best["model"]

input_file = "output/model{}_predictions.csv".format(model_index)
output_file = "output/specific_prediction.csv"

with open(input_file, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

best_row = min(rows, key=lambda r: float(r["absolute_error"]))

with open(output_file, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "model",
            "actual_fuel_l_per_100km",
            "predicted_fuel_l_per_100km",
            "absolute_error",
        ],
    )
    writer.writeheader()
    writer.writerow({
        "model": model_name,
        "actual_fuel_l_per_100km": best_row["actual_fuel_l_per_100km"],
        "predicted_fuel_l_per_100km": best_row["predicted_fuel_l_per_100km"],
        "absolute_error": best_row["absolute_error"],
    })
PY

hdfs dfs -put -f output/specific_prediction.csv \
  /user/team15/project/output/specific_prediction.csv
