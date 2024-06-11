import argparse
import os

from config import *
from utils import bcolors, create_dir, get_exclusion_radius
from download import query_onc_deployments, download_files
from parse import parse_ais_to_json
from clean import clean_ais_data, clean_ctd_data
from combine import combine_deployment_ais_data
from identify import identify_scenarios
from format import group_wav_from_range, wav_all, wav_all_processed
from generate_metadata import generate_full_metadata, generate_balanced_metadata, get_metadata_for_small_times, split_dataset


def create_parser():
    # Create the parser
    parser = argparse.ArgumentParser(description="Dataset Preparation Script")

    # Add the arguments
    parser.add_argument(
        "--onc_token",
        type=str,
        default=ONC_TOKEN,
        help="The Ocean Networks Canada Token to download the files."
        " It can be obtained at https://wiki.oceannetworks.ca/display/O2KB/Get+your+API+token.",
    )

    parser.add_argument(
        "--work_dir",
        "-w",
        type=str,
        default=WORK_DIR,
        help="The path to the workspace directory.",
    )

    parser.add_argument(
        "--steps",
        "-s",
        action='store',
        type=int,
        nargs="+",
        default=STEPS,
        help="The numbers related to the steps that you want to execute. "
        "By default, all stages are executed."
        "0 - Query ONC deployments; "
        "1 - Download AIS files; "
        "2 - Download WAV files; "
        "3 - Parse AIS to JSON; "
        "4 - Clean AIS data; "
        "5 - Combine deployment AIS data; "
        "6 - Identify scenarios; "
        "7 - Classify WAV files from range; "
        "8 - Download CTD files; "
        "9 - Clean CTD files; "
        "10 - Generate the metadata for the full dataset; "
        "11 - Generate a balanced version of the full dataset; "
        "12 - Generate metadata for small periods of duration; "
        "13 - Split dataset into Train, Test and Validation.",
    )

    parser.add_argument(
        "--max_inclusion_radius",
        "-m",
        type=float,
        default=MAX_INCLUSION_RADIUS,
        help="The maximum distance (metres) that a vessel can be from the hydrophone. Used for the whole dataset.",
    )
    
    parser.add_argument(
        "--inclusion_radius",
        "-i",
        type=int,
        default=INCLUSION_RADIUS,
        help="The maximum distance (metres) that a vessel can be from the hydrophone. Used on the subset",
    )

    parser.add_argument(
        "--seconds",
        "-t",
        type=int,
        default=METADATA_SECONDS,
        help="The size of each audio sample in metadata.",
    )

    parser.add_argument(
        "--metadata_file",
        "-f",
        type=str,
        default=METADATA_FILE,
        help="The name of the metadata file without the extension. Assumed to be .csv",
    )

    parser.add_argument(
        "--validation_split",
        "-vs",
        type=float,
        default=METADATA_VAL_SPLIT,
        help="The proportion reserved from metadata to the validation split"
    )

    parser.add_argument(
        "--test_split",
        "-ts",
        type=float,
        default=METADATA_TEST_SPLIT,
        help="The proportion reserved from metadata to the test split"
    )

    return parser


def _main():
    # Execute the parse_args() method
    parser = create_parser()
    args = parser.parse_args()

    print(f"{bcolors.HEADER}Dataset Preparation Script{bcolors.ENDC}\n")

    working_directory = args.work_dir

    deployment_directory = create_dir(working_directory, "00_hydrophone_deployments")
    raw_ais_directory = create_dir(working_directory, "01_raw_ais_files")
    raw_wav_directory = create_dir(working_directory, "02_raw_wav_files")
    parsed_ais_directory = create_dir(working_directory, "03_parsed_ais_files")
    clean_ais_directory = create_dir(working_directory, "04_clean_and_inrange_ais_data")
    combined_deployment_directory = create_dir(working_directory, "05_combined_deployment_ais_data")
    scenario_intervals_directory = create_dir(working_directory, "06a_scenario_intervals")
    interval_ais_data_directory = create_dir(working_directory, "06b_interval_ais_data")
    classified_wav_directory = create_dir(working_directory, "07_classified_wav_files")
    raw_ctd_directory = create_dir(working_directory, "08_raw_ctd_files")
    clean_ctd_directory = create_dir(working_directory, "09_cleaned_ctd_files")
    making_wav_classification = create_dir(working_directory, "10_making_wav_classification")
    makinggoodmakeer_wav_classification = create_dir(working_directory, "11_making_wav_classification")

    token = args.onc_token

    # The maximum distance (metres) that a vessel can be from the hydrophone before we start caring about it.
    max_inclusion_radius = args.max_inclusion_radius
    inclusion_radius = args.inclusion_radius

    # The size of each audio sample in metadata.
    seconds = args.seconds

    # The matadata file name.
    metadata_file = args.metadata_file
    metadata_val_split=args.validation_split
    metadata_test_split=args.test_split

    root_path = os.path.join(classified_wav_directory, f"inclusion_{inclusion_radius}_exclusion_{get_exclusion_radius(inclusion_radius)}")

    if 0 in args.steps:
        print(f"\n{bcolors.HEADER}Querying Ocean Natworks Canada for Deployments{bcolors.ENDC}")
        query_onc_deployments(
            deployment_directory,
            token,
        )

    if 1 in args.steps:
        print(f"\n{bcolors.HEADER}Downloading AIS Files{bcolors.ENDC}")
        download_files(
            raw_ais_directory,
            deployment_directory,
            token,
            file_type="AIS",
        )

    if 2 in args.steps:
        print(f"\n{bcolors.HEADER}Downloading Raw WAV Files{bcolors.ENDC}")
        download_files(
            raw_wav_directory,
            deployment_directory,
            token,
            file_type="WAV",
        )

    if 3 in args.steps:
        print(f"\n{bcolors.HEADER}Parsing AIS files to JSON files{bcolors.ENDC}")
        parse_ais_to_json(
            raw_ais_directory,
            parsed_ais_directory,
            single_threaded_processing=True,
        )

    if 4 in args.steps:
        print(f"\n{bcolors.HEADER}Cleaning AIS data{bcolors.ENDC}")
        clean_ais_data(
            deployment_directory,
            parsed_ais_directory,
            clean_ais_directory,
            _inclusion_radius=max_inclusion_radius,
            use_all_threads=False,
        )

    if 5 in args.steps:
        print(f"\n{bcolors.HEADER}Combining Deployment AIS data{bcolors.ENDC}")
        # This will run the shortest hydrophone deployment to speed up development.
        run_shortest = False
        combine_deployment_ais_data(
            deployment_directory,
            clean_ais_directory,
            combined_deployment_directory,
            run_shortest,
            max_inclusion_radius,
            use_all_threads=False,
        )

    if 6 in args.steps:
        print(f"\n{bcolors.HEADER}Identifying scenarios{bcolors.ENDC}")
        identify_scenarios(
            working_directory,
            deployment_directory,
            scenario_intervals_directory,
            interval_ais_data_directory,
            combined_deployment_directory,
        )

    if 7 in args.steps:
        print(f"\n{bcolors.HEADER}Classifying the dataset into the chosen range{bcolors.ENDC}")
        group_wav_from_range(
            classified_wav_directory,
            scenario_intervals_directory,
            interval_ais_data_directory,
            raw_wav_directory,
            inclusion_radius,
        )

    if 8 in args.steps:
        print(f"\n{bcolors.HEADER}Downloading Conductivity Temperature Depth Files{bcolors.ENDC}")
        download_files(
            raw_ctd_directory,
            deployment_directory,
            token,
            file_type="CTD",
        )

    if 9 in args.steps:
        print(f"\n{bcolors.HEADER}Cleaning CTD data{bcolors.ENDC}")
        clean_ctd_data(
            deployment_directory,
            raw_ctd_directory,
            clean_ctd_directory,
            use_all_threads=False,
        )

    if 10 in args.steps:
        print(f"\n{bcolors.HEADER}Generating the metadata for the full dataset{bcolors.ENDC}")
        generate_full_metadata(
            root_path,
            clean_ctd_directory,
            interval_ais_data_directory,
            inclusion_radius,
        )

    if 11 in args.steps:
        print(f"\n{bcolors.HEADER}Splitting dataset into small periods of time{bcolors.ENDC}")
        get_metadata_for_small_times(
            root_path,
            f"{metadata_file}.csv",
            seconds,
        )

    if 12 in args.steps:
        print(f"\n{bcolors.HEADER}Splitting dataset into train, test and validation datasets{bcolors.ENDC}")
        split_dataset(
            root_path,
            f"{metadata_file}_{seconds}s.csv",
            validation_split=metadata_val_split,
            test_split=metadata_test_split
        )

    if 13 in args.steps:
        print(f"\n{bcolors.HEADER}Generating the balanced metadata version{bcolors.ENDC}")
        generate_balanced_metadata(
            f"{metadata_file}_{seconds}s_train.csv",
            root_path,
        )
    if 14 in args.steps:
        print(f"\n{bcolors.HEADER}making clean the dataset{bcolors.ENDC}")
        wav_all(
            making_wav_classification,
            scenario_intervals_directory,
            interval_ais_data_directory,
            raw_wav_directory,
            inclusion_radius,
        )
    # cutting and preprocessing these raw WAV files.
    if 15 in args.steps:
        wav_all(
            makinggoodmakeer_wav_classification,
            scenario_intervals_directory,
            interval_ais_data_directory,
            raw_wav_directory,
            inclusion_radius,
        )

if __name__ == "__main__":
    _main()
