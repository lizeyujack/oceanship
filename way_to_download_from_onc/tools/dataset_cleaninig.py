import os

import pandas as pd
import numpy as np

from pydub import AudioSegment
from scipy import ndimage
from tqdm import tqdm

SECONDS = 10
INCLUSION_RADIUS = 4000

EXCLUSION_RADIUS = 2000 + INCLUSION_RADIUS
ROOT_PATH = f"/workspaces/underwater/dataset/07_classified_wav_files/inclusion_{INCLUSION_RADIUS}_exclusion_{EXCLUSION_RADIUS}/metadata/complete"
METADATA = f"{ROOT_PATH}/metadata.csv"

def flutuation_analysis(metadata):
    columns = list(metadata.columns) + ['std', 'sub_init']
    metadata_list = []

    for idx, row in tqdm(metadata.iterrows(), total=metadata.shape[0]):
        # Read the audio signal.
        audio = AudioSegment.from_file(row.path, format="wav")
    
        # Find the proportion of the median filter kernel related to the
        # total size of the chunk (Seconds * Sample Rate).
        chunk_size = SECONDS * row.sample_rate
        kernel_size = int(chunk_size/1000) + 1

        duration = int(row.duration_sec)
        row["duration_sec"] = float(SECONDS)
        row_list = list(row)

        # Split the audio file into chunks of SECONDS.
        for i in range(0, duration, SECONDS):
            # Get only the chunk of this file.
            audio_array = audio[i*1000:(i+SECONDS)*1000].get_array_of_samples()

            # Perform a Median Filter on the signal.
            audio_median = ndimage.median_filter(audio_array, size=kernel_size)

            # Get the difference between the median signal and the original sound.
            # The objective here is to extract only the flutuation of the signal,
            # not the median value.
            audio_diff = audio_array - audio_median

            # Get the standard deviation of the flutuation of the signal.
            std_dev = np.std(audio_diff)

            # Append on the Dataframe list.
            metadata_list.append(row_list + [std_dev, i])

    processed_metadata = pd.DataFrame(metadata_list, columns=columns)
    return processed_metadata


def main():
    print("Starting Dataset Cleaning")

    # Read the scenario metadata.
    meta = pd.read_csv(METADATA)

    # Get the duration of each class (excluding background and others).
    desired_classes = list(meta.label.unique())
    desired_classes.remove("other")
    desired_classes.remove("background")
    duration = []
    for label in desired_classes:
        time = meta[meta["label"] == label]["duration_sec"].sum()
        duration.append(time)

    # Get the maximum class duration.
    max_duration = max(duration)

    # Filter only Background sound from the scenario metadata.
    meta_back = meta[meta.label.isin(["background"])].sample(frac=1, random_state=42).reset_index(drop=True)

    # Get only the same amount of background as the most extense class.
    for idx in range(meta_back.shape[0]):
        if meta_back.iloc[0:idx].duration_sec.sum() > max_duration:
            break
    meta_back = meta_back.iloc[0:idx]

    # Filter only Tug, Tanker, Passengership, and Cargo vessels from the scenario metadata.
    # Background and Others are ignored in this analysis.
    meta_vessel = meta[meta.label.isin(["tug","tanker","passengership","cargo"])]

    total_meta = pd.concat([meta_back, meta_vessel])

    final_meta_df = flutuation_analysis(total_meta)

    final_meta_df.to_csv(os.path.join(ROOT_PATH, f"cleaned_optm_metadata_{SECONDS}s.csv"), index=False)

if __name__ == "__main__":
    main()