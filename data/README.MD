# Data Directory

This directory stores local raw data files and generated vehicle metadata used by Stage 1.

## Main files

| File | Created by | Description |
|---|---|---|
| `VED_Static_Data_ICE&HEV.xlsx` | Dataset download | Static vehicle metadata for ICE/HEV vehicles. |
| `VED_Static_Data_PHEV&EV.xlsx` | Dataset download | Static vehicle metadata for PHEV/EV vehicles. |
| `vehicles.csv` | `scripts/preprocess_dataset.py` | Cleaned vehicle metadata used for the PostgreSQL `vehicles` table. |
| `*_week.csv` | `scripts/data_collection.sh` | Raw VED telemetry files. These files may be large and are usually not committed. |

## Role in the pipeline

Stage 1 reads this directory to load PostgreSQL tables:

- `vehicles.csv` is copied into the `vehicles` table;
- telemetry week CSV files are copied into the `trips` table.

Stage 3 train/test splits are not stored here in the current pipeline. They are written as Parquet datasets to HDFS:

```text
/user/team15/project/ml_prepared/train
/user/team15/project/ml_prepared/test
```

Small metadata about the split is exported locally to:

```text
output/train_test_metadata.csv
```

## Important notes

If `vehicles.csv` is missing, run:

```bash
python scripts/preprocess_dataset.py
```

If raw VED files are missing, run:

```bash
bash scripts/data_collection.sh
```

Large raw data files are usually not committed to the repository. They should be downloaded or regenerated in the cluster environment.
