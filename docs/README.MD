# Documentation Directory

This directory contains project documentation, data stories, benchmark notes, dashboard notes, and report-ready figures.

## Files

| File | Description |
|---|---|
| `stage1_data_ingestion.md` | Stage 1 documentation: data download, raw-data audit, PostgreSQL schema, Sqoop import, and validation. |
| `stage2_data_story.md` | Stage 2 EDA story with dashboard-ready insights and figures. |
| `stage3_ml_modeling.md` | Stage 3 documentation: feature selection, train/test split, Spark ML models, outputs, and evaluation. |
| `stage4_dashboard.md` | Stage 4 documentation: dashboard-ready CSV uploads, Hive external tables, and Superset usage. |
| `benchmarking.md` | Benchmark documentation for ingest, storage, processing, and ML experiments. |
| `repository_structure_review.md` | Review of repository structure, inconsistencies, and recommended cleanup actions. |

## Figures

Stage 2 report figures are stored in:

```text
docs/figures/stage2/
```

These images mirror generated EDA figures and are used by `stage2_data_story.md`.

## Dashboard link

Published Superset dashboard:

<http://hadoop-03.uni.innopolis.ru:8808/superset/dashboard/221/>
