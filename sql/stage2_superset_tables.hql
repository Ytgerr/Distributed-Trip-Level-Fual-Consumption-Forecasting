USE team15_projectdb;

DROP TABLE IF EXISTS stage2_eda_data_characteristics;
CREATE EXTERNAL TABLE stage2_eda_data_characteristics (
    metric STRING,
    value STRING
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/eda_data_characteristics';

DROP TABLE IF EXISTS stage2_eda_missingness;
CREATE EXTERNAL TABLE stage2_eda_missingness (
    feature STRING,
    rows_total BIGINT,
    null_count BIGINT,
    null_share DOUBLE
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/eda_missingness';

DROP TABLE IF EXISTS stage2_vehicle_type_distribution;
CREATE EXTERNAL TABLE stage2_vehicle_type_distribution (
    vehicle_type STRING,
    sensor_rows BIGINT,
    vehicles_count BIGINT,
    trips_count BIGINT
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/insight_01_vehicle_type_distribution';

DROP TABLE IF EXISTS stage2_fuel_by_vehicle_type;
CREATE EXTERNAL TABLE stage2_fuel_by_vehicle_type (
    vehicle_type STRING,
    rows_count BIGINT,
    avg_fuel_rate_lhr DOUBLE,
    median_fuel_rate_lhr DOUBLE,
    p90_fuel_rate_lhr DOUBLE
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/insight_02_fuel_by_vehicle_type';

DROP TABLE IF EXISTS stage2_fuel_by_speed_bin;
CREATE EXTERNAL TABLE stage2_fuel_by_speed_bin (
    speed_bin_kmh INT,
    rows_count BIGINT,
    avg_speed_kmh DOUBLE,
    avg_fuel_rate_lhr DOUBLE,
    avg_rpm DOUBLE,
    avg_engine_load DOUBLE
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/insight_03_fuel_by_speed_bin';

DROP TABLE IF EXISTS stage2_stop_go_vs_fuel;
CREATE EXTERNAL TABLE stage2_stop_go_vs_fuel (
    driving_mode STRING,
    rows_count BIGINT,
    avg_fuel_rate_lhr DOUBLE,
    avg_rpm DOUBLE,
    avg_engine_load DOUBLE,
    avg_speed_kmh DOUBLE
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/insight_04_stop_go_vs_fuel';

DROP TABLE IF EXISTS stage2_temperature_hvac_fuel;
CREATE EXTERNAL TABLE stage2_temperature_hvac_fuel (
    oat_bin_c INT,
    rows_count BIGINT,
    avg_fuel_rate_lhr DOUBLE,
    avg_ac_kw DOUBLE,
    avg_heater_kw DOUBLE
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/insight_05_temperature_hvac_fuel';

DROP TABLE IF EXISTS stage2_engine_displacement_fuel;
CREATE EXTERNAL TABLE stage2_engine_displacement_fuel (
    eng_dis DOUBLE,
    rows_count BIGINT,
    vehicles_count BIGINT,
    avg_fuel_rate_lhr DOUBLE,
    avg_speed_kmh DOUBLE,
    avg_rpm DOUBLE
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/insight_06_engine_displacement_fuel';

DROP TABLE IF EXISTS stage2_geo_activity;
CREATE EXTERNAL TABLE stage2_geo_activity (
    lat_bin DOUBLE,
    long_bin DOUBLE,
    rows_count BIGINT,
    trips_count BIGINT,
    avg_speed_kmh DOUBLE,
    avg_fuel_rate_lhr DOUBLE
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/insight_07_geo_activity';

DROP TABLE IF EXISTS stage2_trip_features;
CREATE EXTERNAL TABLE stage2_trip_features (
    vehid STRING,
    tripid STRING,
    daynum DOUBLE,
    n_sensor_points BIGINT,
    duration_min DOUBLE,
    observed_seconds DOUBLE,
    distance_km DOUBLE,
    fuel_used_l DOUBLE,
    fuel_valid_points BIGINT,
    speed_mean DOUBLE,
    speed_median DOUBLE,
    speed_p95 DOUBLE,
    maf_mean DOUBLE,
    maf_p95 DOUBLE,
    rpm_mean DOUBLE,
    rpm_p95 DOUBLE,
    abs_load_mean DOUBLE,
    oat_mean DOUBLE,
    hv_current_mean DOUBLE,
    hv_soc_mean DOUBLE,
    hv_voltage_mean DOUBLE,
    fuel_rate_mean_lhr DOUBLE,
    vehtype STRING,
    vehclass STRING,
    transmission STRING,
    drive_wheels STRING,
    gen_weight DOUBLE,
    eng_type STRING,
    eng_dis DOUBLE,
    eng_conf STRING,
    trip_id STRING,
    stop_go_ratio DOUBLE,
    idle_time_min DOUBLE,
    fuel_l_per_100km DOUBLE
)
STORED AS PARQUET
LOCATION '/user/team15/project/analytics/trip_features';
