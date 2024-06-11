import os
import pandas as pd
import matplotlib.pyplot as plt

BASE_DIR = "/workspaces/underwater/dataset/07_classified_wav_files/inclusion_4000_exclusion_6000/metadata/1_second/"
OUTPUT_DIR = "report"
METADATA_FILE = "metadata_1s"

def generate_report(metadata_file, output_dir):
    meta = pd.read_csv(metadata_file)
    classes = ["other", "passengership", "tug", "tanker", "cargo", "background"]
    duration_dict = {}
    data_info = []

    print("Getting info from Metadata")
    for label in classes:
        aux = meta[meta["label"] == label]
        mmsi = aux["MMSI"].nunique()
        max_date = aux["date"].max()
        min_date = aux["date"].min()
        time = aux["duration_sec"].sum()
        timedelta = pd.to_timedelta(f"{time}s")
        duration_dict[label] = time
        data_info.append(f"Class {label}")
        data_info.append(f"  Period: from {min_date} to {max_date}")
        data_info.append(f"  Total Duration: {timedelta}")
        data_info.append(f"  Number of recordings: {len(aux)}")
        data_info.append(f"  Number of unique vessels: {mmsi}")
        data_info.append("")

    classes = duration_dict.keys()
    duration = duration_dict.values()

    print("Save into a figure")
    # Save the plot.
    plt.figure(figsize=[10, 6])
    plt.xlabel("Class")
    plt.ylabel("Duration (s)")
    plt.bar(classes, duration)
    plt.gca().yaxis.grid()
    plt.savefig(output_dir+"_duration.svg")
    plt.show()

    print("Save into a text file")
    # save info into txt file.
    with open(output_dir+"_info.txt", 'w') as f:
        for line in data_info:
            f.write(line)
            f.write('\n')

    print("Finished")

if __name__ == "__main__":
    if not os.path.exists(BASE_DIR):
        print(f"{BASE_DIR} does not exists! Finishing execution.")
        exit()
    complete_output_dir = BASE_DIR+OUTPUT_DIR
    if not os.path.exists(complete_output_dir):
        print(f"{complete_output_dir} will be created!")
        os.makedirs(complete_output_dir)

    generate_report(BASE_DIR+METADATA_FILE+".csv", complete_output_dir+"/"+METADATA_FILE)
