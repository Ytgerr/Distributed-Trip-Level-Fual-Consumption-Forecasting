This repository is a template for the final project of big data course. It contains the following directories:

- `data/` contains the dataset files.
- `models/` contains the Spark ML models.
- `notebooks/` has the Jupyter or Zeppelin notebooks of your project and used just for learning purposes.
- `output/` represents the output directory for storing the results of the project. It can contain `csv` files, text files. images and any other materials you returned as an ouput of the pipeline.
- `scripts/` is a place for storing `.sh` scripts and `.py` scripts of the pipeline.
- `sql/` is a folder for keeping all `.sql` and `.hql` files.

`requirements.txt` lists the Python packages needed for running your Python scripts. Feel free to add more packages when necessary.

`main.sh` is the main script that will run all scripts of the pipeline stages which will execute the full pipeline and store the results in `output/` folder. During checking your project repo, the grader will run only the main script and check the results in `output/` folder.

**Important Note:** You cannot change the content of the script `main.sh` since it will be used for assessment purposes.

**Another Note:** The notebooks in `notebooks/` folder are used only for learning purposes since you need to put all Python scripts of the pipeline in `scripts/` folder. During the assessment, the grader can delete the folder `notebooks/` to check that your pipeline does not depend on its content.

## Stage 2: Data Science EDA

Stage 2 covers data understanding, EDA, feature analysis, chart generation, and trip-level feature engineering.

Main files:

- `scripts/stage2.sh` — runs Stage 2 Spark EDA pipeline.
- `scripts/stage2_spark_eda.py` — Spark EDA and trip-level feature engineering.
- `scripts/build_stage2_charts.py` — generates EDA figures from Stage 2 CSV outputs.
- `docs/stage2_data_story.md` — data story with EDA figures.
- `docs/stage2_dashboard.md` — static dashboard with generated figures.

Outputs after running Stage 2:

- HDFS Parquet: `/user/team15/project/analytics`
- HDFS CSV: `/user/team15/project/analytics_csv`
- Local CSV: `output/eda`
- Local figures: `output/figures`
