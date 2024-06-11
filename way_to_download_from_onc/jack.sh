:<< EOF
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
    "14/15 - cuting and preprocessing"
EOF

rm -rf ./underwater/04_clean_and_inrange_ais_data/*
python main.py --steps 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15