import os
import ujson
import multiprocessing

import pandas as pd
import pyarrow.feather as feather

from datetime import datetime


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


class ais_params:
    X = "x"
    Y = "y"
    SOG = "sog"
    COG = "cog"
    TRUE_HEADING = "true_heading"
    POSITION_ACCURACY = "position_accuracy"
    TYPE_AND_CARGO = "type_and_cargo"
    FIX_TYPE = "fix_type"
    IMO = "imo"


def create_dir(path, dir_name):
    dir = os.path.join(path, dir_name)
    try:
        os.makedirs(dir)
    except FileExistsError:
        pass

    return dir


def read_messages_from_json_file(_file):
    with open(_file, "r") as input_file:
        return ujson.load(input_file)


def dump_data_frame_to_feather_file(_file, _data_frame):
    feather.write_feather(_data_frame, _file)


def read_data_frame_from_feather_file(_file):
    return feather.read_feather(_file)


def get_num_of_threads(use_all_threads=False):
    # Threading differences between systems.
    number_of_threads = multiprocessing.cpu_count()
    if not use_all_threads:
        number_of_threads = int(number_of_threads / 2)


def get_hydrophone_deployments(deployments_directory):
    # Read in the hydrophone deployments as we need the deployment details for distance calculations.
    hydrophones = os.listdir(deployments_directory)
    hydrophone_deployments = {}

    for hydrophone in hydrophones:
        hydrophone_deployments[hydrophone.split(".")[0]] = pd.read_csv(
            os.path.join(deployments_directory, hydrophone)
        )
    
    return hydrophone_deployments


def timestamp_iso_to_zulu(_timestamp):
    return _timestamp.replace('+00:00', 'Z')


def timestamp_zulu_to_iso(_timestamp):
    return _timestamp.replace('Z', '+00:00')


def pandas_timestamp_to_onc_format(_timestamp):
    return _timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def pandas_timestamp_to_zulu_format(_timestamp):
    return _timestamp.strftime('%Y%m%dT%H%M%S.%f')[:-3] + 'Z'


def zulu_string_to_datetime(_timestamp):
    return datetime.strptime(_timestamp, '%Y%m%dT%H%M%S.%f'+'Z')


def get_exclusion_radius(inclusion_radius):
    return inclusion_radius+2000


def get_min_max_normalization(input_value, min_value, max_value):
    return (input_value - min_value) / (max_value - min_value)


def get_min_max_values_from_df(df, columns):
    return {column: (df[column].astype(float).min(), df[column].astype(float).max()) for column in columns}