import os
import re
import time
from tqdm import tqdm
import ujson

import numpy as np
import lpais.ais as ais

from functools import partial
import multiprocessing

from utils import bcolors, ais_params


def _get_parameters_from_message(_message, _parameters):

    response = {}
    value = np.nan
    for parameter in _parameters:
        try:
            if parameter == ais_params.X:
                value = (
                    _message[ais_params.X] if _message[ais_params.X] < 181 else np.nan
                )

            elif parameter == ais_params.Y:
                value = (
                    _message[ais_params.Y] if _message[ais_params.Y] < 91 else np.nan
                )

            elif parameter == ais_params.SOG:
                value = (
                    _message[ais_params.SOG]
                    if _message[ais_params.SOG] < 1023
                    else np.nan
                )

            elif parameter == ais_params.COG:
                value = (
                    _message[ais_params.COG]
                    if _message[ais_params.COG] < 3600
                    else np.nan
                )

            elif parameter == ais_params.TRUE_HEADING:
                value = (
                    _message[ais_params.TRUE_HEADING]
                    if _message[ais_params.TRUE_HEADING] < 511
                    else np.nan
                )

            elif parameter == ais_params.POSITION_ACCURACY:
                value = _message[ais_params.POSITION_ACCURACY]

            elif parameter == ais_params.TYPE_AND_CARGO:
                value = (
                    _message[ais_params.TYPE_AND_CARGO]
                    if _message[ais_params.TYPE_AND_CARGO] != 0
                    else np.nan
                )

            elif parameter == ais_params.FIX_TYPE:
                value = _message[ais_params.FIX_TYPE]

            elif parameter == ais_params.IMO:
                value = (
                    _message[ais_params.IMO]
                    if _message[ais_params.IMO] != 0
                    else np.nan
                )

        except KeyError as e:
            print(f"{bcolors.FAIL}{_message}{bcolors.ENDC}")
            raise KeyError(e)

        if not np.isnan(value):
            response[parameter] = value

    return response


def dump_data_to_json_file(_file, _data):

    # print("Dumping valid messages to JSON file now...")
    start_time = time.time()
    output_file_name = _file

    with open(output_file_name, "w") as output_file:
        ujson.dump(_data, output_file)

    # print("  Dumped valid messages to JSON file in {0:.3f} seconds".format(time.time() - start_time))


def parse_all_valid_messages(
    _raw_file_path, _raw_data_directory, _parsed_data_directory
):

    # Check if the file already exist.
    if os.path.exists(
        os.path.join(
            _parsed_data_directory, _raw_file_path.strip(".txt") + "_parsed.json"
        )
    ):
        # Pull the data in from all of the JSON files.
        print(f"  The JSON file for this data file already exists, passing through")
        return

    # Checksums appear to be quite useless.
    # Read comments here: https://math.stackexchange.com/questions/2841295/how-many-possible-invalid-ais-message-body-combinations-are-there-for-a-specific
    decoder = ais.decoder(
        allow_unknown=True,
        allow_missing_timestamps=True,
        pass_invalid_checksums=True,
        handle_err=None,
    )

    lines_read = 0
    messages_decoded = 0
    messages_accepted = 0
    messages_rejected = 0

    message_ids_in_file = []
    valid_messages = []

    # Read input ais files.
    input_file = open(os.path.join(_raw_data_directory, _raw_file_path), "r")
    file_contents = input_file.readlines()
    input_file.close()

    # Declare formating message.
    correct_formatting_regex = re.compile("^\w{15}\.\w{4}\ !")

    for line in file_contents:
        if not correct_formatting_regex.match(line):
            messages_rejected += 1
            continue

        lines_read += 1

        # On the off chance that there is an errant space in the message, split by the zulu indicator and space.
        line_contents = line.split("Z ")

        # Timestamp appears as YYYYMMDDT000000.000Z.
        # Translate that into a more standard ISO 8601 representation as that is what the ONC API requires.
        # 'YYYY-MM-DDThh:mm:ss.sssZ'
        ais_timestamp = line_contents[0] + "Z"
        data = line_contents[1].strip("\n")

        # Start decoding the message.
        message = None

        try:
            message = decoder(data)

        except:
            messages_rejected += 1
            continue

        # Corrupted, non-compliant, and multi-line messages return None, so ignore it.
        if message:
            messages_decoded += 1

            # Does the message have a correct, 9-digit MMSI?
            if len(str(message["mmsi"])) != 9:
                messages_rejected += 1
                continue

            # Is the message ID one that we care about?

            # Taken from https://www.navcen.uscg.gov/?pageName=AISMessages
            # 1 = Position report (Class A)
            # 2 = Position report (Class A)
            # 3 = Position report (Class A)
            # 5 = Static and voyage related data
            # 18 = Standard Class B equipment position report
            # 19 = Extended Class B equipment position report
            # 24 = Static data report
            message_ids_to_accept = (1, 2, 3, 5, 18, 19, 24)

            if message["id"] not in message_ids_to_accept:
                messages_rejected += 1
                continue

            # Purely for tracking what message ID's were in the original file.
            if message["id"] not in message_ids_in_file:
                message_ids_in_file.append(message["id"])

            # Begin processing the messages by ID.
            parameters = ()

            # Message ID's 1, 2, and 3 all seem to be the same information for Class A vessels.
            # Message ID 18 is for Class B vessels, but we want the same information from that message ID.
            if message["id"] in (1, 2, 3, 18):
                parameters = ("x", "y", "sog", "cog", "true_heading")

            # Message ID 5 is for Class A vessel information.
            # Message ID 24 is for Class B vessel information and comes in 2 parts.
            # We only care about the information in part 2.
            elif (message["id"] == 5) or (
                message["id"] == 24 and message["part_num"] == 1
            ):
                parameters = ("type_and_cargo",)

            # Message ID 19 is essentially an ID 1, 2, 3, or 18 message with additional fields from ID 5.
            elif message["id"] == 19:
                parameters = ("x", "y", "sog", "cog", "true_heading", "type_and_cargo")

            responses = _get_parameters_from_message(message, parameters)

            if responses:
                messages_accepted += 1
                responses["ais_timestamp"] = ais_timestamp
                responses["mmsi"] = message["mmsi"]
                responses["id"] = message["id"]
                valid_messages.append(responses)

            else:
                messages_rejected += 1

    dump_data_to_json_file(
        os.path.join(
            _parsed_data_directory, _raw_file_path.replace(".txt", "_parsed.json")
        ),
        valid_messages,
    )


def parse_ais_to_json(
    raw_ais_directory, parsed_ais_directory, single_threaded_processing=True
):
    '''
    This function parse the ais messages downloaded from ONC into JSON files,
    filtering by the type of the messages and discarting messages without the
    needed values.
    '''

    print(f"Finding available AIS files to parse...")
    # List available RAW files to parse in the input folder.
    available_files = os.listdir(raw_ais_directory)
    available_files.sort()
    print(f"  Found {bcolors.BOLD}{len(available_files)}{bcolors.ENDC} AIS files to parse")

    # List existing parsed files in the destination folder.
    existing_files = os.listdir(parsed_ais_directory)
    existing_files.sort()
    print(f"  Found {bcolors.BOLD}{len(existing_files)}{bcolors.ENDC} existing JSON files")

    print(f"Working out what files need parsing...")
    files_to_parse = [file for file in available_files if file not in existing_files]
    print(f"  There are {bcolors.BOLD}{len(files_to_parse)}{bcolors.ENDC} files to parse")

    print(f"Beginning to parse AIS files now...")
    if single_threaded_processing:
        if files_to_parse:
            for file in tqdm(files_to_parse):
                parse_all_valid_messages(file, raw_ais_directory, parsed_ais_directory)
        else:
            print(f"{bcolors.WARNING}No files to parse.{bcolors.ENDC}")

    else:
        print("Begin Multi threading processing...")
        start_time = time.time()
        thread_pool = multiprocessing.Pool(2)
        arguments = partial(
            parse_all_valid_messages,
            _raw_data_directory=raw_ais_directory,
            _parsed_data_directory=parsed_ais_directory,
        )
        for _ in tqdm(thread_pool.imap(arguments, available_files), total=len(available_files)):
            pass
        thread_pool.close()
        thread_pool.join()
        print(
            "  This process took {0:.3f} seconds to complete".format(
                time.time() - start_time
            )
        )
