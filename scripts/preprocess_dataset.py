import pandas as pd

from pathlib import Path
import re
import os

data_folder = Path("data")

if (data_folder / "vehicles.csv").exists():
    print("Preprocessed data detected, removing it...")
    # os.remove(data_folder / "trips.csv")
    os.remove(data_folder / "vehicles.csv")
else:
    print("Preprocessed data not detected")

# trips_df = None

# trips_mapper = {
#     "DayNum": "daynum",
#     "Trip": "tripid",
#     "VehId": "vehid",
#     "Timestamp(ms)": "time",
#     "Latitude[deg]": "lat",
#     "Longitude[deg]": "long",
#     "Vehicle Speed[km/h]": "speed",
#     "MAF[g/sec]": "maf",
#     "Engine RPM[RPM]": "rpm",
#     "Absolute Load[%]": "abs_load",
#     "OAT[DegC]": "oat",
#     "Fuel Rate[L/hr]": "fuel_rate",
#     "Air Conditioning Power[kW]": "air_cp_kw",
#     "Air Conditioning Power[Watts]": "air_cp_watts",
#     "Heater Power[Watts]": "heater_power",
#     "HV Battery Current[A]": "hv_battery_current",
#     "HV Battery SOC[%]": "hv_battery_soc",
#     "HV Battery Voltage[V]": "hv_battery_vol",
#     "Short Term Fuel Trim Bank 1[%]": "stfb_1",
#     "Short Term Fuel Trim Bank 2[%]": "stfb_2",
#     "Long Term Fuel Trim Bank 1[%]": "ltfb_1",
#     "Long Term Fuel Trim Bank 2[%]": "ltfb_2",
# }

print("Preprocessing data...")

# for weekly_trips_file in tqdm(data_folder.glob("*.csv")):
#     try:
#         df = pd.read_csv(weekly_trips_file)

#         trips = df.rename(columns=trips_mapper)

#         if trips_df is None:
#             trips_df = trips
#         else:
#             trips_df = pd.concat([trips_df, trips])

#     except Exception:
#         print("Skipping {}".format(str(weekly_trips_file)))

static_data = pd.concat(
    [
        pd.read_excel(data_folder / "VED_Static_Data_ICE&HEV.xlsx"),
        pd.read_excel(data_folder / "VED_Static_Data_PHEV&EV.xlsx"),
    ]
)


def convert_l(string: str):
    dis = re.findall(r"(\d\.\d)L", string)
    conf = re.sub(r"\d\.\dL", "", string).strip()
    if len(dis) == 0:
        return None, conf
    if conf == "":
        conf = None
    return float(dis[0]), conf


def process_static_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe["eng_dis"] = dataframe["Engine Configuration & Displacement"].apply(
        lambda x: convert_l(x)[0]
    )
    dataframe["eng_conf"] = dataframe["Engine Configuration & Displacement"].apply(
        lambda x: convert_l(x)[1]
    )
    dataframe.drop(["Engine Configuration & Displacement"], axis=1, inplace=True)
    dataframe.rename(
        columns={
            "VehId": "vehid",
            "Vehicle Type": "vehtype",
            "Vehicle Class": "vehclass",
            "EngineType": "eng_type",
            "Transmission": "transmission",
            "Drive Wheels": "drive_wheels",
            "Generalized_Weight": "gen_weight",
        },
        inplace=True,
    )
    dataframe.replace("NO DATA", None, inplace=True)
    return dataframe


vehicles_df = process_static_data(static_data)

print("Loading data into csv files..")

# trips_df.to_csv(data_folder / "trips.csv", index=False, na_rep="null")
vehicles_df.to_csv(data_folder / "vehicles.csv", index=False, na_rep="null")

print("Done with {} vehicles detected in dataset!".format(len(vehicles_df)))
