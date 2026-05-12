USE team15_projectdb;
DROP TABLE IF EXISTS stage4_data_audit;
CREATE EXTERNAL TABLE stage4_data_audit (
  file_path STRING,
  file_name STRING,
  extension STRING,
  size_mb DOUBLE,
  row_count BIGINT,
  column_count STRING,
  excel_sheets STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/data_audit'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_table_overview;
CREATE EXTERNAL TABLE stage4_table_overview (
  table_name STRING,
  row_count BIGINT,
  column_count INT,
  hdfs_path STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/table_overview'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_column_types;
CREATE EXTERNAL TABLE stage4_column_types (
  table_name STRING,
  column_name STRING,
  data_type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/column_types'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_missingness;
CREATE EXTERNAL TABLE stage4_missingness (
  table_name STRING,
  column_name STRING,
  total_rows BIGINT,
  null_count BIGINT,
  null_pct DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/missingness'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_numeric_summary;
CREATE EXTERNAL TABLE stage4_numeric_summary (
  table_name STRING,
  column_name STRING,
  non_null_count BIGINT,
  mean DOUBLE,
  stddev DOUBLE,
  variance DOUBLE,
  min_value DOUBLE,
  max_value DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/numeric_summary'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_numeric_quantiles;
CREATE EXTERNAL TABLE stage4_numeric_quantiles (
  table_name STRING,
  column_name STRING,
  p01 DOUBLE,
  p25 DOUBLE,
  p50 DOUBLE,
  p75 DOUBLE,
  p99 DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/numeric_quantiles'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_categorical_summary;
CREATE EXTERNAL TABLE stage4_categorical_summary (
  table_name STRING,
  column_name STRING,
  approx_distinct_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/categorical_summary'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_feature_selection;
CREATE EXTERNAL TABLE stage4_feature_selection (
  feature STRING,
  feature_type STRING,
  score STRING,
  feature_rank STRING,
  selected INT,
  selection_method STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/feature_selection'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_train_test_metadata;
CREATE EXTERNAL TABLE stage4_train_test_metadata (
  dataset STRING,
  row_count BIGINT,
  columns_count INT,
  hdfs_path STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/train_test_metadata'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_model_evaluation;
CREATE EXTERNAL TABLE stage4_model_evaluation (
  model STRING,
  rmse DOUBLE,
  mae DOUBLE,
  r2 DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/model_evaluation'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_best_model;
CREATE EXTERNAL TABLE stage4_best_model (
  model_index INT,
  model STRING,
  cv_folds INT,
  param_grid_size INT,
  train_time_sec DOUBLE,
  prediction_time_sec DOUBLE,
  test_rows BIGINT,
  rmse DOUBLE,
  mae DOUBLE,
  r2 DOUBLE,
  best_params STRING,
  hdfs_model_path STRING,
  local_model_path STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/best_model'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_model_training_log;
CREATE EXTERNAL TABLE stage4_model_training_log (
  model_index INT,
  model STRING,
  cv_folds INT,
  param_grid_size INT,
  train_time_sec DOUBLE,
  prediction_time_sec DOUBLE,
  test_rows BIGINT,
  rmse DOUBLE,
  mae DOUBLE,
  r2 DOUBLE,
  best_params STRING,
  hdfs_model_path STRING,
  local_model_path STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/model_training_log'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_model_cv_results;
CREATE EXTERNAL TABLE stage4_model_cv_results (
  model STRING,
  params STRING,
  avg_cv_rmse DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/model_cv_results'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_specific_prediction;
CREATE EXTERNAL TABLE stage4_specific_prediction (
  model STRING,
  actual_fuel_l_per_100km DOUBLE,
  predicted_fuel_l_per_100km DOUBLE,
  absolute_error DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/specific_prediction'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_model_prediction_samples;
CREATE EXTERNAL TABLE stage4_model_prediction_samples (
  actual_fuel_l_per_100km DOUBLE,
  predicted_fuel_l_per_100km DOUBLE,
  absolute_error DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/model_prediction_samples'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_ingest_benchmark;
CREATE EXTERNAL TABLE stage4_ingest_benchmark (
  method STRING,
  operation STRING,
  runtime_sec DOUBLE,
  row_count STRING,
  details STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/ingest_benchmark'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_storage_benchmark;
CREATE EXTERNAL TABLE stage4_storage_benchmark (
  storage_format STRING,
  compression STRING,
  write_time_sec DOUBLE,
  read_time_sec DOUBLE,
  read_rows BIGINT,
  source_rows BIGINT,
  hdfs_path STRING,
  hdfs_size_mb DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/storage_benchmark'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_processing_benchmark;
CREATE EXTERNAL TABLE stage4_processing_benchmark (
  stage_name STRING,
  output_rows BIGINT,
  runtime_sec DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/processing_benchmark'
TBLPROPERTIES ("skip.header.line.count"="1");
