import os
import time
import os.path
import multiprocessing


import pandas as pd

from tqdm import tqdm
from onc.onc import ONC
from functools import partial

from utils import bcolors


def get_deployment_filters(deployment_directory, filter_type="WAV"):
    filters = []

    for hydrophone_file in os.listdir(deployment_directory):
        hydrophone_name = hydrophone_file.split(".")[0]
        hydrophone_file_path = os.path.join(deployment_directory, hydrophone_file)
        hydrophone_deployment = pd.read_csv(hydrophone_file_path)
        for index, row in hydrophone_deployment.iterrows():
            if filter_type.lower() == "wav":
                filters.append(
                    {
                        "deviceCode": hydrophone_name,
                        "dateFrom": row["begin"],
                        "dateTo": row["end"],
                        "extension": "wav",
                    }
                )
            elif filter_type.lower() == "ais":
                filters.append(
                    {
                        "deviceCode": "DIGITALYACHTAISNET1302-0097-01",
                        "dateFrom": row["begin"],
                        "dateTo": row["end"],
                        "extension": "txt",
                    }
                )
            elif filter_type.lower() == "ctd":
                filters.append(
                    {
                        "deviceCode": "SBECTD19p6935",
                        "dateFrom": row["begin"],
                        "dateTo": row["end"],
                        "extension": "txt",
                    }
                )
    return filters


def download_onc_file(_filename, _token, _path):
        onc_api = ONC(_token, outPath=_path, timeout=600)
        onc_api.getFile(_filename, overwrite=False)


def download_file_list(output_directory, token, files_to_download):
    start_time = time.time()
    thread_pool = multiprocessing.Pool(20)
    arguments = partial(download_onc_file, _token=token, _path=output_directory)
    for _ in tqdm(thread_pool.imap(arguments, files_to_download), total=len(files_to_download)):
        pass
    thread_pool.close()
    thread_pool.join()
    print(
        "  This download took {0:.3f} seconds to complete.\n".format(
            time.time() - start_time
        )
    )

    return


def query_onc_deployments(deployment_directory, token):
    # Instantiate ONC object.
    onc_api = ONC(token, outPath=deployment_directory, timeout=600)

    # Find the deployment windows for the hydrophones.
    filters = [{"deviceCode": "ICLISTENAF2523"}, {"deviceCode": "ICLISTENAF2556"}]
    results = []

    for new_filter in filters:
        results.extend(onc_api.getDeployments(new_filter))

    data_to_fetch = {}

    for result in sorted(results, key=lambda kv: kv["begin"]):
        if not result["begin"] or not result["end"]:
            continue

        else:
            if result["deviceCode"] not in data_to_fetch:
                data_to_fetch[result["deviceCode"]] = []

            data_to_fetch[result["deviceCode"]].append(
                (
                    result["begin"],
                    result["end"],
                    result["lat"],
                    result["lon"],
                    result["depth"],
                    result["locationCode"],
                )
            )

    filters = []

    for device_code, intervals in data_to_fetch.items():
        output_file = open(
            os.path.join(deployment_directory, f"{device_code}.csv"), "w"
        )
        output_file.write("begin,end,latitude,longitude,depth,location\n")

        for interval in intervals:
            output_file.write(",".join(str(entry) for entry in interval) + "\n")

        output_file.close()
    return


def download_files(output_directory, deployment_directory, token, file_type="WAV"):
    # Instantiate ONC object.
    onc_api = ONC(token, timeout=600)

    # Get the desired filter object to query for files at ONC servers.
    filters = get_deployment_filters(deployment_directory, filter_type=file_type)

    print(f"Finding available {file_type} files to download...")
    available_files = [
        files
        for new_filter in filters
        for files in onc_api.getListByDevice(new_filter, allPages=True)["files"]
    ]
    available_files.sort()
    print(
        f"  Found {bcolors.BOLD}{len(available_files)}{bcolors.ENDC} available {file_type} files.\n"
    )

    print(f"Checking existing {file_type} file directory...")
    existing_files = os.listdir(output_directory)
    existing_files.sort()
    print(
        f"  Found {bcolors.BOLD}{len(existing_files)}{bcolors.ENDC} existing {file_type} files.\n"
    )

    print(f"Working out what files need downloading...")
    existing_files = set(existing_files)
    files_to_download = [file for file in available_files if file not in existing_files]
    print(
        f"  There are {bcolors.BOLD}{len(files_to_download)}{bcolors.ENDC} {file_type} files to download.\n"
    )

    if files_to_download:
        print(f"Commencing download of {file_type} files now...")
        # TODO: There are 306345 files to be downloaded. I just downloaded the first 10 files.
        #download_file_list(output_directory, token, files_to_download[:10])
        download_file_list(output_directory, token, files_to_download)
    else:
        print(f"{bcolors.WARNING}No {file_type} files to download.{bcolors.ENDC}\n")

    return