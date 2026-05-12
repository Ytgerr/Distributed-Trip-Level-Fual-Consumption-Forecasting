# Repository Structure Review

## TL;DR

The repository structure is mostly clear and stage-oriented. The main issue was documentation drift: `main.sh` already describes a newer pipeline with profiling, benchmarks, feature selection, three models, and Stage 4 Hive tables, while several Markdown files still described the older two-model Stage 3 flow. This documentation has been updated.

## Current structure verdict

| Area | Status | Comment |
|---|---|---|
| Top-level layout | Good | Standard project folders exist: `data`, `docs`, `models`, `notebooks`, `output`, `scripts`, `sql`. |
| Pipeline entrypoint | Good | `main.sh` gives a clear full-pipeline order. |
| Stage scripts | Mostly good | Scripts are grouped by stage; benchmark scripts are isolated under `scripts/benchmarks`. |
| Documentation | Improved | Markdown files now match the current `main.sh` flow. |
| Generated artifacts | Needs decision | `data/`, `output/`, and `models/` are ignored by `.gitignore`, but the archive contains generated files and README files inside them. |
| Stage 3 structure | Needs cleanup | There are two Stage 3 implementations: legacy `stage3.sh`/`stage3_spark_ml.py` and current `stage3_feature_selection.sh`/`stage3_train_models.sh`. |
| Environment files | Needs cleanup | `.python-version`, `pyproject.toml`, and `requirements.txt` should be aligned to the real cluster Python/Spark runtime. |

## Main structural findings

### 1. Documentation drift was the biggest problem

Before this update, the root README and Stage 3 documentation described a two-model pipeline:

```text
Linear Regression + GBT
```

But `main.sh` runs the newer flow:

```text
feature selection + Linear Regression + Random Forest + GBT
```

The Markdown files now document the current flow and explicitly mark the older Stage 3 path as legacy.

### 2. There are two Stage 3 paths

Current path used by `main.sh`:

```text
scripts/stage3_feature_selection.sh
scripts/stage3_train_models.sh
```

Legacy path still present:

```text
scripts/stage3.sh
scripts/stage3_spark_ml.py
```

Recommendation: keep the legacy path only if it is needed for comparison. Otherwise remove it or move it to:

```text
scripts/legacy/
```

### 3. Generated artifact policy is inconsistent

`.gitignore` ignores:

```text
data/
output/
models/
```

But the archive contains files under these folders. This is not fatal, but the team should choose one policy.

Recommended policy:

```text
commit:
  data/README.MD
  output/README.MD
  models/README.MD
  docs/figures/stage2/*.png

do not commit:
  raw data files
  full model folders
  full output folders
  large generated CSVs
```

If small sample outputs are required for grading, keep them intentionally and document that they are snapshots.

### 4. Environment metadata should be aligned

The repository has three environment sources:

```text
.python-version
pyproject.toml
requirements.txt
```

This can confuse reproducibility if they disagree. The Spark cluster should be the source of truth.

Recommendation:

- keep `requirements.txt` as the cluster dependency file;
- update `pyproject.toml` only if it is actually used;
- set `.python-version` to the same Python version used on the cluster;
- avoid declaring a Python version that is not compatible with the pinned Spark/Pandas stack.

### 5. Stage 4 depends on generated files

`stage4.sh` expects outputs from Stage 1, profiling, benchmarks, feature selection, and model training. Running Stage 4 directly from a fresh clone may fail or skip tables unless earlier stages have already produced local CSV artifacts.

Recommendation: keep Stage 4 documented as a final aggregation/publishing stage, not as an independent standalone stage.

## Recommended target tree

```text
.
├── README.MD
├── requirements.txt
├── main.sh
├── data/
│   └── README.MD
├── docs/
│   ├── README.MD
│   ├── stage1_data_ingestion.md
│   ├── stage2_data_story.md
│   ├── stage3_ml_modeling.md
│   ├── stage4_dashboard.md
│   ├── benchmarking.md
│   ├── repository_structure_review.md
│   └── figures/stage2/*.png
├── models/
│   └── README.MD
├── notebooks/
│   ├── README.MD
│   └── *.ipynb
├── output/
│   └── README.MD
├── scripts/
│   ├── README.MD
│   ├── stage1*.sh / stage1*.py
│   ├── stage2*.sh / stage2*.py
│   ├── stage3*.sh / stage3*.py
│   ├── stage4.sh
│   └── benchmarks/
└── sql/
    ├── README.MD
    ├── create_tables.sql
    ├── stage2_superset_tables.hql
    └── stage4_superset_tables.hql
```

## Recommended cleanup commits

### Commit 1: documentation alignment

```text
docs: align README files with current pipeline structure
```

Include only Markdown changes.

### Commit 2: generated artifact policy

```text
chore: clarify generated artifact tracking policy
```

Decide whether large generated outputs should stay in the repository.

### Commit 3: legacy Stage 3 cleanup

```text
refactor: separate legacy stage3 training entrypoint
```

Move old Stage 3 scripts to `scripts/legacy/` or remove them if not needed.

### Commit 4: environment cleanup

```text
chore: align python environment metadata
```

Make `.python-version`, `pyproject.toml`, and `requirements.txt` consistent.
