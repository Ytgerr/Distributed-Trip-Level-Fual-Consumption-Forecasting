# Stage 2: Data Understanding, EDA and Feature Engineering

## TL;DR

Stage 2 processes raw vehicle and trip telemetry data with Spark on YARN. The pipeline cleans numerical missing values, joins trips with vehicle metadata, creates EDA insight tables, and builds trip-level features for downstream analytics and modeling.

The dataset contains:

- 22,436,808 telemetry rows
- 384 vehicles
- 32,552 unique trips
- 22 trip columns
- 9 vehicle columns

## Data quality

The main data-quality issue is sparsity in fuel and energy-related fields.

Raw `fuel_rate` is missing in approximately 96% of telemetry rows. MAF is missing in approximately 18.6% of rows, so we use raw `fuel_rate` when available and estimate fuel rate from MAF otherwise.

HV battery fields are missing in approximately 83% of rows because these fields are relevant only for specific vehicle types and powertrain configurations.

## Fuel-rate approximation

When raw `fuel_rate` is unavailable, fuel rate is estimated from MAF:

`fuel_rate_lhr = MAF / 14.7 / 745 * 3600`

where:

- `14.7` is the gasoline stoichiometric air-fuel ratio.
- `745 g/L` is the average gasoline density used in the approximation.
- `3600` converts seconds to hours.

## Insight 1: Missingness by feature

The missingness chart shows that fuel and energy-related fields are sparse. This explains why the pipeline needs fuel-rate approximation and why feature availability should be considered during modeling.

![Missingness by feature](figures/stage2/01_missingness_by_feature.png)

Direct fuel_rate is missing for most records, while speed and rpm are almost fully available, so the model should rely on stable driving-behavior features and use MAF-based fuel approximation where needed.

Source file:

`output/eda/eda_missingness.csv`

## Insight 2: Vehicle type distribution

The dataset contains ICE, HEV, and records with missing vehicle type. Vehicle type is important because fuel and energy behavior differs across powertrain technologies.

![Vehicle type distribution](figures/stage2/02_vehicle_type_distribution.png)

ICE trips dominate the dataset, so model results may be biased toward conventional vehicles, while HEV and missing-type records should be handled carefully as smaller groups.

Source file:

`output/eda/insight_01_vehicle_type_distribution.csv`

## Insight 3: Fuel rate by vehicle type

ICE vehicles have a higher average fuel-rate profile than HEV vehicles. This supports using vehicle metadata as model features.

![Fuel rate by vehicle type](figures/stage2/03_fuel_by_vehicle_type.png)

ICE vehicles have the highest average fuel rate, HEV vehicles are lower, and missing-type records are the lowest, so vehicle type is a strong feature for explaining fuel behavior.

Source file:

`output/eda/insight_02_fuel_by_vehicle_type.csv`

## Insight 4: Fuel rate by speed bin

Fuel rate changes across speed regimes. Distance alone is not enough to explain consumption; speed profile should also be included.

![Fuel rate by speed bin](figures/stage2/04_fuel_by_speed_bin.png)

Fuel rate stays relatively stable at low and medium speeds, but rises sharply in high-speed bins, so speed profile is an important predictor of fuel consumption behavior.

Source file:

`output/eda/insight_03_fuel_by_speed_bin.csv`

## Insight 5: Driving mode vs fuel

Stop-go, city, normal, and highway regimes have different fuel-rate profiles. This motivates trip-level features such as `stop_go_ratio`, `idle_time_min`, `speed_mean`, and `speed_p95`.

![Driving mode vs fuel](figures/stage2/05_driving_mode_vs_fuel.png)

Highway driving has the highest fuel-rate intensity, while stop-go mode has the lowest L/h value; therefore, driving mode should be included as a key behavioral feature, but L/h should not be confused with L/100km efficiency.

Source file:

`output/eda/insight_04_stop_go_vs_fuel.csv`

## Insight 6: Temperature, HVAC and fuel

Outside air temperature is related to AC and heater usage. Environmental context can influence fuel and energy consumption.

![Temperature, HVAC and fuel](figures/stage2/06_temperature_hvac_fuel.png)

Heater usage is highest at low temperatures, AC usage increases at high temperatures, and fuel rate rises at temperature extremes, so outside temperature and HVAC load should be included as environmental features.

Source file:

`output/eda/insight_05_temperature_hvac_fuel.csv`

## Insight 7: Engine displacement and fuel rate

Engine displacement is associated with fuel-rate differences. It should be interpreted together with vehicle type and driving conditions.

![Engine displacement and fuel rate](figures/stage2/07_engine_displacement_fuel.png)

Fuel rate generally increases with engine displacement, which suggests that engine size is an important static vehicle feature, although outliers show that driving conditions and vehicle type also matter.

Source file:

`output/eda/insight_06_engine_displacement_fuel.csv`

## Insight 8: Trip distance vs fuel consumption

The trip-level sample shows how fuel consumption varies across trip distance. This helps validate trip-level aggregation and detect outliers.

![Trip distance vs fuel consumption](figures/stage2/08_trip_distance_vs_consumption.png)

Short trips show much higher variability in fuel consumption, while longer trips are more stable and mostly stay in a lower L/100km range.

Source file:

`output/eda/trip_features_sample.csv`

## Trip-level feature engineering

The raw trips table is sensor-level: each row corresponds to one timestamp inside a trip. Stage 2 aggregates it into a trip-level table where each row corresponds to one `(vehid, tripid)` pair.

The resulting features include:

- trip duration
- observed seconds
- distance in km
- fuel used in liters
- fuel consumption in L/100km
- mean, median, and p95 speed
- stop-go ratio
- idle time
- MAF, RPM, and engine-load statistics
- outside air temperature
- HV battery statistics
- vehicle metadata

## Outputs

HDFS Parquet outputs:

`/user/team15/project/analytics`

HDFS CSV outputs:

`/user/team15/project/analytics_csv`

Local CSV outputs:

`output/eda`

Local generated figures:

`output/figures`

Committed report figures:

`docs/figures/stage2`
