# Output Directory

This directory contains local artifacts exported by the pipeline for checking, reporting, and dashboard preparation.

## Stage 1 outputs

| Path | Description |
|---|---|
| `output/warehouse/` | Local copy of Sqoop-imported HDFS warehouse tables. Created by `scripts/stage1.sh`. |

## Stage 2 outputs

| Path | Description |
|---|---|
| `output/eda/` | Small CSV files with EDA tables and trip-feature samples. |
| `output/figures/` | Generated EDA figures for documentation and dashboard storytelling. |

Expected Stage 2 CSV files include:

- `eda_data_characteristics.csv`
- `eda_missingness.csv`
- `insight_01_vehicle_type_distribution.csv`
- `insight_02_fuel_by_vehicle_type.csv`
- `insight_03_fuel_by_speed_bin.csv`
- `insight_04_stop_go_vs_fuel.csv`
- `insight_05_temperature_hvac_fuel.csv`
- `insight_06_engine_displacement_fuel.csv`
- `insight_07_geo_activity.csv`
- `trip_features_sample.csv`
- `vehicles_raw.csv`

## Stage 3 outputs

| File | Description |
|---|---|
| `model1_predictions.csv` | Test-set predictions from Linear Regression. Columns: `label`, `prediction`. |
| `model2_predictions.csv` | Test-set predictions from GBT Regressor. Columns: `label`, `prediction`. |
| `evaluation.csv` | Model comparison table with RMSE, MAE, and R2. |

## How to regenerate

```bash
bash scripts/stage1.sh   # regenerates output/warehouse
bash scripts/stage2.sh   # regenerates output/eda and output/figures
bash scripts/stage3.sh   # regenerates prediction/evaluation CSV files
```

## Notes

- Most files in this directory are generated artifacts, not source code.
- If outputs look outdated, delete the relevant subfolder/file and rerun the corresponding stage.
- Superset dashboard charts should be checked against the latest files/tables after rerunning the pipeline.
