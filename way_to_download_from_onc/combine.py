import os
import time
import multiprocessing
import sys
import numpy as np
import pandas as pd

from tqdm import tqdm

from utils import (
    get_num_of_threads,
    get_hydrophone_deployments,
    dump_data_frame_to_feather_file,
    pandas_timestamp_to_zulu_format,
    read_data_frame_from_feather_file,
)


def generate_time_steps(_row_pd_timestamp, _row_time_difference, _minimum_delta):

    end_timestamp = _row_pd_timestamp
    time_difference = _row_time_difference

    steps_required = np.ceil(time_difference / _minimum_delta)

    time_steps = pd.date_range(
        start=(end_timestamp - time_difference),
        end=end_timestamp - (time_difference / steps_required),
        periods=steps_required,
    )

    return time_steps.to_list()


def interpolation_for_chunks(_chunk):
    '''
    Interpolate the location data to generate new entries with more regularity.
    The new interpolated data will be generated if two values are separated for
    a time greater than minimum_delta and smaller than maximum_delta.
    '''

    # Seconds between timestamps.
    minimum_delta = np.timedelta64(20, "s")
    maximum_delta = np.timedelta64(1200, "s")

    _chunk["time_difference"] = _chunk["pd_timestamp"] - _chunk["pd_timestamp"].shift()
    _chunk["to_interpolate"] = (_chunk["time_difference"] > minimum_delta) & (
        _chunk["time_difference"] <= maximum_delta
    )

    if _chunk["to_interpolate"].any():
        time_steps_to_add = np.vectorize(generate_time_steps)(
            _chunk[_chunk["to_interpolate"]]["pd_timestamp"],
            _chunk[_chunk["to_interpolate"]]["time_difference"],
            minimum_delta,
        )

        time_steps_to_add = [
            subvalue for value in time_steps_to_add for subvalue in value
        ]
        new_timesteps = pd.DataFrame(data=time_steps_to_add, columns=["pd_timestamp"])

        _chunk = pd.concat([_chunk, new_timesteps], ignore_index=True)
        _chunk = _chunk.sort_values(by="pd_timestamp", ignore_index=True)
        _chunk = _chunk.drop(labels=["time_difference", "to_interpolate"], axis=1)

        _chunk["x"] = _chunk["x"].interpolate()
        _chunk["y"] = _chunk["y"].interpolate()
        _chunk["sog"] = _chunk["sog"].interpolate()
        _chunk["cog"] = _chunk["cog"].interpolate()
        _chunk["true_heading"] = _chunk["true_heading"].interpolate()
        _chunk["distance_to_hydrophone"] = _chunk[
            "distance_to_hydrophone"
        ].interpolate()

        _chunk["id"] = _chunk["id"].fillna(method="ffill")
        _chunk["mmsi"] = _chunk["mmsi"].fillna(method="ffill")
        _chunk["type_and_cargo"] = _chunk["type_and_cargo"].fillna(method="ffill")

        return _chunk[_chunk["ais_timestamp"].isna()]

    else:
        _chunk = _chunk.drop(labels=["time_difference", "to_interpolate"], axis=1)

        return pd.DataFrame(columns=_chunk.columns)


def combine_deployment_ais_data(
    deployment_directory,
    clean_ais_directory,
    combined_deployment_directory,
    _run_shortest=False,
    _inclusion_radius=15000.0,
    use_all_threads=False,
):
    '''
    This function combines the feather files from the same deployment into one
    unique cleaned file. It also generate a new interpolated file, with values
    for the location with more granularity with values generated from the linear
    interpolation of the real ais messages from the original feather files.
    '''

    # Threading differences between systems.
    # number_of_threads = get_num_of_threads(use_all_threads)
    number_of_threads = 2

    # Find all of the cleaned AIS files for each deployment.
    cleaned_ais_files = [file for file in os.listdir(clean_ais_directory)]
    # print('cleaned_ais_files: ', cleaned_ais_files)
    # Read in the hydrophone deployments as we will treat each deployment as an individual dataset.
    hydrophone_deployments = get_hydrophone_deployments(deployment_directory)
    # print(hydrophone_deployments)
    # sys.exit()
    # Process each device and deployment individually.
    # TIP: You probably don't want to parallelise this level, unless you have a _lot_ of system memory.
    for device in hydrophone_deployments.keys():
        for deployment in hydrophone_deployments[device].itertuples(index=False):

            # Get deployment begin and end information to evaluate duration.
            deployment_begin = pd.Timestamp(deployment.begin).normalize()
            deployment_end = pd.Timestamp(deployment.end).normalize() + pd.DateOffset(
                days=1
            )

            print(deployment_begin,deployment_end)
            deployment_duration = (deployment_end - deployment_begin) / np.timedelta64(
                24, "h"
            )

            if _run_shortest and (deployment_duration > 60.0):
                continue

            print(
                "Working on deviceID {0} for deployment {1} to {2}...".format(
                    device, deployment_begin, deployment_end
                )
            )

            deployment_ais_data_files = []

            for file in cleaned_ais_files:
                file_timestamp = pd.Timestamp(file.split("_")[1])
                if deployment_begin <= file_timestamp <= deployment_end:
                    deployment_ais_data_files.append(file)

            print("Reading cleaned AIS file into a pandas DataFrame...")

            start_time = time.time()

            files = [
                read_data_frame_from_feather_file(
                    os.path.join(clean_ais_directory, file)
                )
                for file in deployment_ais_data_files
            ]
            if not len(files):
                continue

            data_frame = pd.concat(files)

            print(
                "  There are {0} MMSI's across {1} entries".format(
                    data_frame.mmsi.unique().shape[0], data_frame.shape[0]
                )
            )
            print(
                "  This took {0:.3f} seconds to process".format(
                    time.time() - start_time
                )
            )

            print("Removing all MMSI's that have a single message...")

            start_time = time.time()
            data_frame = data_frame.groupby("mmsi").filter(
                lambda mmsi_entries: len(mmsi_entries) > 1
            )

            print(
                "  There are now {0} MMSI's across {1} entries".format(
                    data_frame.mmsi.unique().shape[0], data_frame.shape[0]
                )
            )
            print(
                "  This took {0:.3f} seconds to process".format(
                    time.time() - start_time
                )
            )

            print("Populating pd_timestamp column...")

            start_time = time.time()
            data_frame["pd_timestamp"] = pd.to_datetime(data_frame["ais_timestamp"])
            data_frame.sort_values(by="pd_timestamp", inplace=True, ignore_index=True)
            data_frame.reset_index(inplace=True, drop=True)

            print(
                "  There are now {0} MMSI's across {1} entries".format(
                    data_frame.mmsi.unique().shape[0], data_frame.shape[0]
                )
            )
            print(
                "  This took {0:.3f} seconds to process".format(
                    time.time() - start_time
                )
            )

            print("Dumping deployment AIS data to a monolithic FEATHER file...")

            start_time = time.time()
            output_file_name = "_".join(
                [
                    device,
                    pandas_timestamp_to_zulu_format(deployment_begin),
                    pandas_timestamp_to_zulu_format(deployment_end),
                    "clean_ais_data.feather",
                ]
            )
            output_file_name = os.path.join(
                combined_deployment_directory, output_file_name
            )
            dump_data_frame_to_feather_file(output_file_name, data_frame)

            print(
                "  This took {0:.3f} seconds to process".format(
                    time.time() - start_time
                )
            )

            # Interpolate the position information for each vessel and save to a separate file.
            print("Performing temporal interpolation on data...")
            print(
                f"  There are {data_frame.shape[0]} AIS entries across {data_frame.mmsi.unique().shape[0]} MMSI's"
            )
            start_time = time.time()
            grouped_by_mmsi = data_frame.groupby("mmsi")
            grouped_data = [data for mmsi, data in grouped_by_mmsi]
            grouped_data.sort(key=lambda x: x.shape[0], reverse=True)

            threading_pool = multiprocessing.Pool(processes=number_of_threads)
            outputs = list(
                tqdm(
                    threading_pool.imap_unordered(
                        interpolation_for_chunks, grouped_data, chunksize=1
                    ),
                    total=len(grouped_data)
                )
            )
            threading_pool.close()
            threading_pool.join()
            interpolated_data_frame = pd.concat(outputs)

            print(
                f"  There are {interpolated_data_frame.shape[0]} interpolated entries across {interpolated_data_frame.mmsi.unique().shape[0]} MMSI's"
            )
            print(
                "  This took {0:.3f} seconds to process".format(
                    time.time() - start_time
                )
            )

            # Combine the raw and interpolated data frames.
            print("Combining the raw and interpolated data frames...")

            start_time = time.time()
            data_frame = pd.concat(
                [data_frame, interpolated_data_frame], ignore_index=True
            )
            data_frame.sort_values(by="pd_timestamp", inplace=True, ignore_index=True)

            print(
                f"  There are now {data_frame.shape[0]} combined entries across {data_frame.mmsi.unique().shape[0]} MMSI's"
            )
            print(
                "  This took {0:.3f} seconds to process".format(
                    time.time() - start_time
                )
            )

            print(
                "Dumping interpolated deployment AIS data to a monolithic FEATHER file..."
            )

            start_time = time.time()
            output_file_name = "_".join(
                [
                    device,
                    pandas_timestamp_to_zulu_format(deployment_begin),
                    pandas_timestamp_to_zulu_format(deployment_end),
                    "clean_interpolated_ais_data.feather",
                ]
            )
            output_file_name = os.path.join(
                combined_deployment_directory, output_file_name
            )

            dump_data_frame_to_feather_file(output_file_name, data_frame)
            print(
                "  This took {0:.3f} seconds to process...".format(
                    time.time() - start_time
                )
            )
