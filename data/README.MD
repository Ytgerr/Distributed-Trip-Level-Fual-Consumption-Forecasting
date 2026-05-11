# Data Directory

This directory stores local data files used by the pipeline and small exported ML split files.

## Main files

| File | Created by | Description |
|---|---|---|
| `VED_Static_Data_ICE&HEV.xlsx` | Dataset download | Static vehicle metadata for ICE/HEV vehicles. |
| `VED_Static_Data_PHEV&EV.xlsx` | Dataset download | Static vehicle metadata for PHEV/EV vehicles. |
| `vehicles.csv` | `scripts/preprocess_dataset.py` | Cleaned vehicle metadata used for the PostgreSQL `vehicles` table. |
| `*_week.csv` | `scripts/data_collection.sh` | Raw VED telemetry files. These files may be large and are usually not committed. |
| `train.json` | `scripts/stage3.sh` | Local copy of the Stage 3 training split. |
| `test.json` | `scripts/stage3.sh` | Local copy of the Stage 3 test split. |

## Role in the pipeline

Stage 1 reads this directory to load PostgreSQL tables:

- `vehicles.csv` is copied into the `vehicles` table.
- telemetry week CSV files are copied into the `trips` table.

Stage 3 writes local train/test splits here after generating them in HDFS:

- HDFS train split: `project/data/train`
- HDFS test split: `project/data/test`
- Local copies: `data/train.json`, `data/test.json`

## Important notes

- If `vehicles.csv` is missing, run:

```bash
python scripts/preprocess_dataset.py
```

- If raw VED files are missing, run:

```bash
bash scripts/data_collection.sh
```
