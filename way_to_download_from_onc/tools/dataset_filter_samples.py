import os

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

ALPHA = 0

SECONDS = 10
INCLUSION_RADIUS = 2000

EXCLUSION_RADIUS = 2000 + INCLUSION_RADIUS
ROOT_PATH = f"/workspaces/underwater/dataset/07_classified_wav_files/inclusion_{INCLUSION_RADIUS}_exclusion_{EXCLUSION_RADIUS}/metadata/complete"

def main():
    # Read the entire metadata with Standard Deviation info.
    cleaned_meta=pd.read_csv(os.path.join(ROOT_PATH, f"cleaned_optm_metadata_{SECONDS}s.csv"))

    # Read the vessel and the background splits of the data.
    vessel_metadata = cleaned_meta[cleaned_meta["label"] != "background"]
    background_metadata = cleaned_meta[cleaned_meta["label"]=="background"]

    # Get the mean and standard deviation of the array of background flutuations.
    back_std = list(background_metadata["std"])
    back_std_array = np.array(back_std)

    back_mean = np.mean(back_std_array, axis=0)
    back_std = np.std(back_std_array, axis=0)

    # Considering the normal curve distribution, we stablish the threshold to get all the
    # inferior portion of the curve (50%) plus the ALPHA*sigma of the superior part (X%),
    # totalising (50+X)% of the data. Every entry that is located on this region is considered
    # as background sound and is removed from the vessel entry.
    threshold = back_mean + ALPHA*back_std

    # Separate the vessel info according the threshold.
    valid_vessels = vessel_metadata[vessel_metadata["std"] >= threshold]
    invalid_vessels = vessel_metadata[vessel_metadata["std"] < threshold]

    # Save the valid metadata.
    total_metadata = pd.concat([valid_vessels, background_metadata])
    total_metadata.to_csv(os.path.join(ROOT_PATH, f"filtered_metadata_{SECONDS}s.csv"), index=False)

    # Plot the result on a bar plot graph.
    invalid_vessels = invalid_vessels.sort_values(by=['std'])
    std_dev_list_outlier = list(invalid_vessels["std"])
    outlier_num = len(std_dev_list_outlier)
    outlier_list = list(range(0,outlier_num))

    valid_vessels = valid_vessels.sort_values(by=['std'])
    std_dev_list_not_outlier = list(valid_vessels["std"])
    not_outlier_num = len(std_dev_list_not_outlier)
    not_outlier_list = list(range(outlier_num,outlier_num+not_outlier_num))

    plt.figure(figsize=[30, 6])
    plt.xlabel("Audio Index")
    plt.ylabel("Standard Deviation")
    plt.bar(outlier_list, std_dev_list_outlier, color = 'red')
    plt.bar(not_outlier_list, std_dev_list_not_outlier, color = 'blue')
    plt.savefig(os.path.join(ROOT_PATH, "filtered_plot.svg"))

if __name__ == "__main__":
    main()