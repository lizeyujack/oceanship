import folium
import os

import numpy as np
import pandas as pd
import pyarrow.feather as feather

from tqdm import tqdm

INCLUSION_RADIUS = 2000
EXCLUSION_RADIUS = 2000 + INCLUSION_RADIUS
SCENARIO_ROOT_PATH = f"/workspaces/underwater/dataset/07_classified_wav_files/inclusion_{INCLUSION_RADIUS}_exclusion_{EXCLUSION_RADIUS}"
INFO_FILE = f"/workspaces/underwater/dataset/testes/audio/inclusion_{INCLUSION_RADIUS}_exclusion_{EXCLUSION_RADIUS}_info.csv"

DEVICE = "ICLISTENAF2523"
DEPLOYMENT_FILE = f"/workspaces/underwater/dataset/00_hydrophone_deployments/{DEVICE}.csv"

INTERVAL_FILE = "/workspaces/underwater/dataset/06a_scenario_intervals/ICLISTENAF2523_20170624T000000.000Z_20171104T000000.000Z_unique_vessel_intervals.csv"
SCENARIO_AIS_DIR = "/workspaces/underwater/dataset/06b_interval_ais_data"

MAPS_DIR = "/workspaces/underwater/dataset/testes/maps"

class map_colors:
    exclusion_zone = "#ad2727"
    inclusion_zone = "#27ad27"
    device = "#3388ff"
    inclusion_vessel = "#bb33ff"
    exclusion_vessel = "#ffaa33"

def read_data_frame_from_feather_file(_file):
    return feather.read_feather(_file)

def gen_hex_colors(num_of_colors):
    color_max = 16777215
    step = int(16777215/num_of_colors)

    return [hex(num).replace('0x', "#") for num in range(0, color_max, step)]

def plot_map(
        deployment, device, interval_data, exclusion_radius=0, inclusion_radius=0, file_name="map"
    ):
    
    # Create a map object.
    folium_map = folium.Map(
        location=(deployment.latitude, deployment.longitude),
        tiles="CartoDB positron",
        zoom_start=12,
    )

    # Adds the exclusion radius.
    folium.Circle(
        location=(deployment.latitude, deployment.longitude),
        radius=float(exclusion_radius),
        dash_array="10,20",
        color=map_colors.exclusion_zone,
        fill_color=map_colors.exclusion_zone,
        fill_opacity=0.2,
        popup=f"Exclusion radius of {exclusion_radius} meters",
        tooltip=f"Exclusion radius of {exclusion_radius} meters",
    ).add_to(folium_map)

    # Adds the inclusion radius.
    folium.Circle(
        location=(deployment.latitude, deployment.longitude),
        radius=float(inclusion_radius),
        dash_array="10,20",
        color=map_colors.inclusion_zone,
        fill_color=map_colors.inclusion_zone,
        fill_opacity=0.2,
        popup=f"Inclusion radius of {inclusion_radius} meters",
        tooltip=f"Inclusion radius of {inclusion_radius} meters",
    ).add_to(folium_map)

    # Adds the device into the map.
    folium.Circle(
        location=(deployment.latitude, deployment.longitude),
        radius=1.0,
        color=map_colors.device,
        popup=f"{device}",
        tooltip=f"{device}",
    ).add_to(folium_map)
    
    # Gets all the records for this MMSI
    grouped_by_mmsi = interval_data.groupby("mmsi")
    colors = gen_hex_colors(interval_data.mmsi.nunique())

    for idx, (mmsi, data) in enumerate(grouped_by_mmsi):
        data["x"] = data["x"].interpolate()
        data["y"] = data["y"].interpolate()
        
        data=data.dropna(subset=['x'])
        data=data.dropna(subset=['y'])

        vessel_location = list(zip(data.y, data.x))

        for latitude, longitude in vessel_location:
            folium.Circle(
                location=(latitude, longitude),
                radius=1.0,
                color=colors[idx],
                popup=f"{mmsi}",
                tooltip=f"{mmsi}",
            ).add_to(folium_map)

    folium_map.save(file_name)

if __name__ == "__main__":

    intervals_file = os.path.join(SCENARIO_ROOT_PATH, "vessel", "intervals.csv")
    df_vessel = pd.read_csv(intervals_file)
    df_deployment = pd.read_csv(DEPLOYMENT_FILE)

    df_info = pd.read_csv(INFO_FILE)
    wav_files = list(df_info["original_wav"])

    filtered_df_vessel = df_vessel[df_vessel["wav_file"].isin(wav_files)]

    for _, row in tqdm(filtered_df_vessel.iterrows(), total=filtered_df_vessel.shape[0]):
        begin_time = row["begin"].replace("-","").replace(":","")
        end_time = row["end"].replace("-","").replace(":","")
        wav_file = row["wav_file"]

        interval_file_name = "_".join([begin_time, end_time, "interval_data.feather"])
        interval_file = read_data_frame_from_feather_file(os.path.join(SCENARIO_AIS_DIR, interval_file_name))

        map_file_name = f"{wav_file}.html"
        plot_map(
            df_deployment,
            DEVICE,
            interval_file,
            exclusion_radius=EXCLUSION_RADIUS,
            inclusion_radius=INCLUSION_RADIUS,
            file_name=os.path.join(MAPS_DIR, map_file_name)
        )
