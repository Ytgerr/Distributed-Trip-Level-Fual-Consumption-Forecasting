#!/usr/bin/env python3

import os
import pandas as pd
import matplotlib.pyplot as plt


EDA_DIR = "output/eda"
FIG_DIR = "output/figures"


def save_figure(name):
    os.makedirs(FIG_DIR, exist_ok=True)
    path = os.path.join(FIG_DIR, name)

    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()

    print("saved:", path)


def plot_missingness():
    df = pd.read_csv(os.path.join(EDA_DIR, "eda_missingness.csv"))
    df = df.sort_values("null_share", ascending=False)

    plt.figure(figsize=(10, 5))
    plt.bar(df["feature"], df["null_share"])
    plt.xticks(rotation=70, ha="right")
    plt.ylabel("null share")
    plt.title("Missingness by feature")

    save_figure("01_missingness_by_feature.png")


def plot_vehicle_type_distribution():
    df = pd.read_csv(os.path.join(EDA_DIR, "insight_01_vehicle_type_distribution.csv"))

    plt.figure(figsize=(7, 4))
    plt.bar(df["vehicle_type"], df["trips_count"])
    plt.xlabel("vehicle type")
    plt.ylabel("trips count")
    plt.title("Trip count by vehicle type")

    save_figure("02_vehicle_type_distribution.png")


def plot_fuel_by_vehicle_type():
    df = pd.read_csv(os.path.join(EDA_DIR, "insight_02_fuel_by_vehicle_type.csv"))

    plt.figure(figsize=(7, 4))
    plt.bar(df["vehicle_type"], df["avg_fuel_rate_lhr"])
    plt.xlabel("vehicle type")
    plt.ylabel("avg fuel rate, L/h")
    plt.title("Average fuel rate by vehicle type")

    save_figure("03_fuel_by_vehicle_type.png")


def plot_fuel_by_speed_bin():

    df = pd.read_csv(os.path.join(EDA_DIR, "insight_03_fuel_by_speed_bin.csv"))
    df = df.sort_values("speed_bin_kmh")

    plt.figure(figsize=(8, 4))
    plt.plot(df["speed_bin_kmh"], df["avg_fuel_rate_lhr"], marker="o")
    plt.xlabel("speed bin, km/h")
    plt.ylabel("avg fuel rate, L/h")
    plt.title("Average fuel rate by speed bin")

    save_figure("04_fuel_by_speed_bin.png")


def plot_driving_mode():
    df = pd.read_csv(os.path.join(EDA_DIR, "insight_04_stop_go_vs_fuel.csv"))

    plt.figure(figsize=(8, 4))
    plt.bar(df["driving_mode"], df["avg_fuel_rate_lhr"])
    plt.xticks(rotation=25, ha="right")
    plt.ylabel("avg fuel rate, L/h")
    plt.title("Fuel rate by driving mode")

    save_figure("05_driving_mode_vs_fuel.png")


def plot_temperature_hvac():

    df = pd.read_csv(os.path.join(EDA_DIR, "insight_05_temperature_hvac_fuel.csv"))
    df = df.sort_values("oat_bin_c")

    plt.figure(figsize=(8, 4))
    plt.plot(df["oat_bin_c"], df["avg_fuel_rate_lhr"], marker="o", label="fuel rate, L/h")
    plt.plot(df["oat_bin_c"], df["avg_ac_kw"], marker="o", label="AC, kW")
    plt.plot(df["oat_bin_c"], df["avg_heater_kw"], marker="o", label="heater, kW")
    plt.xlabel("outside air temperature bin, C")
    plt.title("Temperature, HVAC and fuel rate")
    plt.legend()

    save_figure("06_temperature_hvac_fuel.png")


def plot_engine_displacement():
    df = pd.read_csv(os.path.join(EDA_DIR, "insight_06_engine_displacement_fuel.csv"))
    df = df.sort_values("eng_dis")

    plt.figure(figsize=(8, 4))
    plt.scatter(df["eng_dis"], df["avg_fuel_rate_lhr"])
    plt.xlabel("engine displacement, L")
    plt.ylabel("avg fuel rate, L/h")
    plt.title("Fuel rate by engine displacement")

    save_figure("07_engine_displacement_fuel.png")


def plot_trip_distance_vs_consumption():
    df = pd.read_csv(os.path.join(EDA_DIR, "trip_features_sample.csv"))
    df = df.dropna(subset=["distance_km", "fuel_l_per_100km"])

    plt.figure(figsize=(8, 4))
    plt.scatter(df["distance_km"], df["fuel_l_per_100km"], s=8)
    plt.xlabel("trip distance, km")
    plt.ylabel("fuel consumption, L/100km")
    plt.title("Trip distance vs fuel consumption")

    save_figure("08_trip_distance_vs_consumption.png")


def main():
    # just plots everything
    plot_missingness()
    plot_vehicle_type_distribution()
    plot_fuel_by_vehicle_type()
    plot_fuel_by_speed_bin()
    plot_driving_mode()
    plot_temperature_hvac()
    plot_engine_displacement()
    plot_trip_distance_vs_consumption()


if __name__ == "__main__":
    main()
