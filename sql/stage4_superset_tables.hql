USE team15_projectdb;

DROP TABLE IF EXISTS stage4_psql_table_counts;
CREATE EXTERNAL TABLE stage4_psql_table_counts (
  table_name STRING,
  records_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_psql_table_counts'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_psql_column_datatypes;
CREATE EXTERNAL TABLE stage4_psql_column_datatypes (
  table_name STRING,
  column_name STRING,
  data_type STRING,
  is_nullable STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_psql_column_datatypes'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_psql_vehicles_sample;
CREATE EXTERNAL TABLE stage4_psql_vehicles_sample (
  vehid DOUBLE,
  vehtype STRING,
  vehclass STRING,
  transmission STRING,
  drive_wheels STRING,
  gen_weight DOUBLE,
  eng_type STRING,
  eng_dis DOUBLE,
  eng_conf STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_psql_vehicles_sample'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_psql_trips_sample;
CREATE EXTERNAL TABLE stage4_psql_trips_sample (
  daynum DOUBLE,
  vehid DOUBLE,
  tripid DOUBLE,
  time_raw_ms DOUBLE,
  speed DOUBLE,
  maf DOUBLE,
  rpm DOUBLE,
  fuel_rate DOUBLE,
  oat DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_psql_trips_sample'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_data_cleaning_summary;
CREATE EXTERNAL TABLE stage4_data_cleaning_summary (
  step_order INT,
  step_name STRING,
  description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_data_cleaning_summary'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_feature_extraction_characteristics;
CREATE EXTERNAL TABLE stage4_feature_extraction_characteristics (
  feature_name STRING,
  feature_group STRING,
  feature_type STRING,
  transformation STRING,
  model_usage STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_feature_extraction_characteristics'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_feature_group_summary;
CREATE EXTERNAL TABLE stage4_feature_group_summary (
  feature_group STRING,
  features_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_feature_group_summary'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_hyperparameter_optimization;
CREATE EXTERNAL TABLE stage4_hyperparameter_optimization (
  model_name STRING,
  param_name STRING,
  tested_values STRING,
  best_value STRING,
  cv_folds STRING,
  optimization_metric STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_hyperparameter_optimization'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_model_evaluation;
CREATE EXTERNAL TABLE stage4_model_evaluation (
  model_name STRING,
  rmse DOUBLE,
  mae DOUBLE,
  r2 DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_model_evaluation'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_model_comparison;
CREATE EXTERNAL TABLE stage4_model_comparison (
  model_name STRING,
  metric STRING,
  metric_value DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_model_comparison'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_best_model;
CREATE EXTERNAL TABLE stage4_best_model (
  model_name STRING,
  selection_metric STRING,
  selection_value DOUBLE,
  rmse DOUBLE,
  mae DOUBLE,
  r2 DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_best_model'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_prediction_samples;
CREATE EXTERNAL TABLE stage4_prediction_samples (
  model_name STRING,
  row_id BIGINT,
  label DOUBLE,
  prediction DOUBLE,
  error DOUBLE,
  abs_error DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_prediction_samples'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_model1_predictions;
CREATE EXTERNAL TABLE stage4_model1_predictions (
  model_name STRING,
  row_id BIGINT,
  label DOUBLE,
  prediction DOUBLE,
  error DOUBLE,
  abs_error DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_model1_predictions'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS stage4_model2_predictions;
CREATE EXTERNAL TABLE stage4_model2_predictions (
  model_name STRING,
  row_id BIGINT,
  label DOUBLE,
  prediction DOUBLE,
  error DOUBLE,
  abs_error DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/team15/project/stage4/stage4_model2_predictions'
TBLPROPERTIES ("skip.header.line.count"="1");
