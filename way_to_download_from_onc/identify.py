import os
import folium
import sys
import numpy as np
import pandas as pd

from tqdm import tqdm
from utils import (
    create_dir,
    get_hydrophone_deployments,
    pandas_timestamp_to_zulu_format,
    dump_data_frame_to_feather_file,
    read_data_frame_from_feather_file,
)


class map_colors:
    exclusion_zone = "#ad2727"
    inclusion_zone = "#27ad27"


def plot_map(
    deployment,
    device,
    map_data_directory,
    exclusion_radius=0,
    inclusion_radius=0,
    file_name="map"):

    folium_map = folium.Map(
        location=(
            deployment.latitude,
            deployment.longitude,
        ),
        tiles="CartoDB positron",
        zoom_start=12,
    )

    if exclusion_radius != 0:
        folium.Circle(
            location=(
                deployment.latitude,
                deployment.longitude,
            ),
            radius=float(exclusion_radius),
            dash_array="10,20",
            color=map_colors.exclusion_zone,
            fill_color=map_colors.exclusion_zone,
            fill_opacity=0.2,
            popup=f"Exclusion radius of {exclusion_radius} metres",
            tooltip=f"Exclusion radius of {exclusion_radius} metres",
        ).add_to(folium_map)

    if inclusion_radius != 0:
        folium.Circle(
            location=(
                deployment.latitude,
                deployment.longitude,
            ),
            radius=float(inclusion_radius),
            dash_array="10,20",
            color=map_colors.inclusion_zone,
            fill_color=map_colors.inclusion_zone,
            fill_opacity=0.2,
            popup=f"Inclusion radius of {inclusion_radius} metres",
            tooltip=f"Inclusion radius of {inclusion_radius} metres",
        ).add_to(folium_map)

    folium.Circle(
        location=(
            deployment.latitude,
            deployment.longitude,
        ),
        radius=1.0,
        color="#3388ff",
        popup=f"{device}",
        tooltip=f"{device}",
    ).add_to(folium_map)

    folium_map.save(
        os.path.join(
            map_data_directory,
            file_name,
        )
    )

def generate_csv(
    interval_dicts,
    device,
    deployment_begin,
    deployment_end,
    scenario_intervals_directory,
    interval_ais_data_directory,
    data_frame,
    minimum_consecutive_minutes=30,
    is_background=True,
    csv_name="file.csv"
    ):

    list_of_data_to_fetch = {}

    # We need to incrementally enforce uniqueness so that each sample is statistically isolated.
    # That is to say, if an interval is selected at 10000 meters, it needs to be removed from all closer ranges.
    descending_keys = list(interval_dicts.keys())
    descending_keys.sort(reverse=True)

    print(f"Identifying consecutively time intervals and saving to feather file...")
    for distance_index, key in tqdm(enumerate(descending_keys)):

        # Create a temporary DataFrame with a single column.
        temporary_data_frame = pd.DataFrame(
            interval_dicts[key],
            columns=["closed_left"],
        )

        # Calculate the difference between each row and the next.
        temporary_data_frame["difference"] = (
            temporary_data_frame["closed_left"]
            .diff()
            .dt.total_seconds()
            .div(60, fill_value=0.0)
        )

        # This logic ignore the first occurency of consecutive diference (the value will be 1 for the first element).
        # This behaviour is compensated at the feather file dump by incrementing one to the interval.
        # Tally the number of consecutive differences (e.g. where delta_time == 1 consistently).
        temporary_data_frame["consecutive"] = (
            temporary_data_frame["difference"]
            .groupby(
                (
                    temporary_data_frame["difference"]
                    != temporary_data_frame["difference"].shift()
                ).cumsum()
            )
            .transform("size")
        )

        # Select only the intervals where the consecutive number (e.g. total time) meets the criteria above.
        temporary_data_frame = temporary_data_frame[
            temporary_data_frame["consecutive"] >= minimum_consecutive_minutes
        ]

        start_index = -1
        end_index = -1
        start_timestamp = None
        end_timestamp = None

        # There is most certainly a more pandas way of doing this whole process, but bugger it, this is fast enough.
        # Get start and end timestamps from the consecutive periods.
        while end_index != (temporary_data_frame.shape[0] - 1):
            start_index = end_index + 1
            end_index = (
                start_index
                + temporary_data_frame.iloc[start_index]["consecutive"]
                - 1
            )
            start_timestamp = temporary_data_frame.iloc[start_index][
                "closed_left"
            ]
            end_timestamp = temporary_data_frame.iloc[end_index][
                "closed_left"
            ] + pd.DateOffset(minutes=1)

            if key not in list_of_data_to_fetch:
                list_of_data_to_fetch[key] = []

            list_of_data_to_fetch[key].append(
                [start_timestamp, end_timestamp]
            )


        # Having the reference list as a set makes the look-up an O(1) operation instead of an O(N) for a list.
        # Utterly hilarious difference in performance between those two with large lists.
        reference_list = set(temporary_data_frame["closed_left"])

        for modify_index in range(distance_index + 1, len(descending_keys)):
            interval_dicts[descending_keys[modify_index]] = [
                value
                for value in interval_dicts[
                    descending_keys[modify_index]
                ]
                if value not in reference_list
            ]

    file_name = "_".join(
        [
            device,
            pandas_timestamp_to_zulu_format(deployment_begin),
            pandas_timestamp_to_zulu_format(deployment_end),
            csv_name,
        ]
    )

    output_file = open(
        os.path.join(scenario_intervals_directory, file_name), "w"
    )

    if is_background:
        output_file.write("exclusion_radius,begin,end\n")
    else:
        output_file.write("inclusion_radius,exclusion_radius,begin,end\n")

    for key, intervals in list_of_data_to_fetch.items():
        for interval in intervals:
            if is_background:
                output_file.write(
                    ",".join(
                        [
                            f"{key:05d}",
                            pandas_timestamp_to_zulu_format(interval[0]),
                            pandas_timestamp_to_zulu_format(interval[1]),
                        ]
                    )
                    + "\n"
                )
            else:
                scenario_parts = key.split("_")
                output_file.write(
                        ",".join(
                            [
                                f"{scenario_parts[1]}",
                                f"{scenario_parts[3]}",
                                pandas_timestamp_to_zulu_format(interval[0]),
                                pandas_timestamp_to_zulu_format(interval[1]),
                            ]
                        )
                        + "\n"
                    )

            # Dump the AIS data for these scenarios to individual feather files.
            feather_file_name = "_".join(
                [
                    pandas_timestamp_to_zulu_format(interval[0]),
                    pandas_timestamp_to_zulu_format(interval[1]),
                    "interval_data.feather",
                ]
            )

            dump_data_frame_to_feather_file(
                os.path.join(interval_ais_data_directory, feather_file_name),
                data_frame[
                    (
                        data_frame["pd_timestamp"]
                        >= (interval[0] - pd.DateOffset(minutes=1))
                    )
                    & (
                        data_frame["pd_timestamp"]
                        <= (interval[1] + pd.DateOffset(minutes=1))
                    )
                ],
            )

    output_file.close()


def identify_scenarios(
    working_directory,
    deployment_directory,
    scenario_intervals_directory,
    interval_ais_data_directory,
    combined_deployment_directory,
):

    # Read in the hydrophone deployments as we will treat each deployment as an individual dataset.
    print('deployment_directory',deployment_directory)
    hydrophone_deployments = get_hydrophone_deployments(deployment_directory)

    # 1: Find all of the cleaned AIS files for each deployment.
    devices = hydrophone_deployments.keys()
    # print('devices:', devices)
    # sys.exit()
    for device in devices:
        for deployment in hydrophone_deployments[device].itertuples(index=False):
            deployment_begin = pd.Timestamp(deployment.begin).normalize()
            deployment_end = pd.Timestamp(deployment.end).normalize() + pd.DateOffset(
                days=1
            )

            print(
                "\nWorking on device {0} for deployment from {1} to {2}...".format(
                    device, deployment_begin, deployment_end
                )
            )

            deployment_file_name = "_".join(
                [
                    device,
                    pandas_timestamp_to_zulu_format(deployment_begin),
                    pandas_timestamp_to_zulu_format(deployment_end),
                    "clean_interpolated_ais_data.feather",
                ]
            )

            data_frame = read_data_frame_from_feather_file(
                os.path.join(combined_deployment_directory, deployment_file_name)
            )
            # print(data_frame.head())
            # sys.exit()
            minimum_distance = 1000
            maximum_distance = 10000
            distance_step = 1000
            inclusion_radii = np.arange(
                minimum_distance, maximum_distance + distance_step, distance_step
            )
            exclusion_radius_offset = 2000

            data_frame = data_frame.sort_values(by=["pd_timestamp"])
            # 2: Find the time intervals where only one vessel is within range.

            map_data_directory = create_dir(
                working_directory, "99_inclusion_exclusion_zone_maps"
            )

            # Sanity checking for one-time map generation per entry.
            exclusion_radii_mapped = []
            inclusion_radii_mapped = []

            # Breaking the data off into dictionaries of DataFrames to make organising it easier, compared to n-columns being added to all entries.
            inclusion_exclusion_interval_dicts = {}
            background_noise_interval_dicts = {}

            # data_frame.set_index("pd_timestamp")
            data_frame.set_index(data_frame['pd_timestamp'], inplace=True)
            grouped_by_time_intervals = data_frame.groupby(
                pd.Grouper(freq="1Min", offset="0Min", label="left")
            )
            # grouped_by_time_intervals = data_frame.groupby(['distance_to_hydrophone'])

            reporting_day = (
                grouped_by_time_intervals.first().iloc[0]["pd_timestamp"].normalize()
            )
            print(f"Processing day {reporting_day} now...")

            # Due to the size of the primary DataFrame, we only really want to iterate through the entire thing once, so the interval is top.
            for interval_left, interval_data in tqdm(grouped_by_time_intervals):
                for inclusion_radius in inclusion_radii:
                    exclusion_radius = inclusion_radius + exclusion_radius_offset

                    # No entries within exclusion range means that we can use this interval for background noise estimation.
                    # print('sum\t',sum(interval_data["distance_to_hydrophone"]))
                    # if sum(interval_data["distance_to_hydrophone"]) != 0:
                    #     print(sum(interval_data["distance_to_hydrophone"]))
                    #     sys.exit()
                    if (sum(interval_data["distance_to_hydrophone"] <= exclusion_radius) == 0):
                        if exclusion_radius not in background_noise_interval_dicts:
                            background_noise_interval_dicts[exclusion_radius] = []

                        background_noise_interval_dicts[exclusion_radius].append(interval_left)

                        # Create the map for sanity checking.
                        if exclusion_radius not in exclusion_radii_mapped:
                            exclusion_radii_mapped.append(exclusion_radius)
                            plot_map(
                                deployment,
                                device,
                                map_data_directory,
                                exclusion_radius=exclusion_radius,
                                file_name=f"exclusion_radius_{exclusion_radius:05d}_metres.html"
                            )

                    # Else there is something in the exclusion range.
                    else:
                        # print('there is something in the exclusion range.')
                        only_one_vessel_within_exclusion_radius = (
                            interval_data[
                                interval_data["distance_to_hydrophone"]
                                <= exclusion_radius
                            ]["mmsi"]
                            .unique()
                            .shape[0]
                        ) == 1

                        number_of_messages_in_inclusion_radius = sum(
                            interval_data["distance_to_hydrophone"] <= inclusion_radius
                        )
                        number_of_messages_in_exclusion_radius = sum(
                            interval_data["distance_to_hydrophone"] <= exclusion_radius
                        )
                        all_messages_are_within_inclusion_radius = (
                            number_of_messages_in_inclusion_radius
                            == number_of_messages_in_exclusion_radius
                        )

                        # If there is only a single vessel within the exclusion range and that vessel is within the inclusion range.
                        if (
                            only_one_vessel_within_exclusion_radius
                            and all_messages_are_within_inclusion_radius
                        ):
                            scenario = (
                                f"in_{inclusion_radius:05d}_out_{exclusion_radius:05d}"
                            )

                            if scenario not in inclusion_exclusion_interval_dicts:
                                inclusion_exclusion_interval_dicts[scenario] = []

                            inclusion_exclusion_interval_dicts[scenario].append(
                                interval_left
                            )

                            if inclusion_radius not in inclusion_radii_mapped:
                                inclusion_radii_mapped.append(inclusion_radius)

                                plot_map(
                                    deployment,
                                    device,
                                    map_data_directory,
                                    exclusion_radius=exclusion_radius,
                                    inclusion_radius=inclusion_radius,
                                    file_name=f"inclusion_radius_{inclusion_radius:05d}_metres_exclusion_radius_{exclusion_radius:05d}_metres.html"
                                )

            print(f"  Finished identifying all scenarios...")

            # Find unique background intervals.
            generate_csv(
                background_noise_interval_dicts,
                device,
                deployment_begin,
                deployment_end,
                scenario_intervals_directory,
                interval_ais_data_directory,
                data_frame,
                minimum_consecutive_minutes=30,
                is_background=True,
                csv_name="background_intervals.csv"
            )

            # Find unique inclusion intervals.
            generate_csv(
                inclusion_exclusion_interval_dicts,
                device,
                deployment_begin,
                deployment_end,
                scenario_intervals_directory,
                interval_ais_data_directory,
                data_frame,
                minimum_consecutive_minutes=5,
                is_background=False,
                csv_name="unique_vessel_intervals.csv"
            )

