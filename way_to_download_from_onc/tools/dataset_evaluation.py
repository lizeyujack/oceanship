import os

import pandas as pd
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
import numpy as np

from pydub import AudioSegment
from scipy import signal
from tqdm import tqdm

INCLUSION_RADIUS = 2000
EXCLUSION_RADIUS = 2000 + INCLUSION_RADIUS
METADATA = f"/workspaces/underwater/dataset/07_classified_wav_files/inclusion_{INCLUSION_RADIUS}_exclusion_{EXCLUSION_RADIUS}/metadata/10_seconds/metadata_10s.csv"
ROOT_PATH = "/workspaces/underwater/dataset/testes/audio"

print("Starting Dataset Evaluation")
meta = pd.read_csv(METADATA)
meta = meta[meta.label.isin(["tug","tanker","passengership","cargo"])].sample(frac=1, random_state=42).reset_index(drop=True)
meta = meta.loc[0:50]
final_meta = {
    "indexes": [],
    "label": [],
    "std": [],
    "original_wav": [],
    "begin": [],
    "end": [],
}

for idx, row in tqdm(meta.iterrows(), total=meta.shape[0]):
    audio = AudioSegment.from_file(row.path, format="wav", start_second=row.sub_init, duration=10.0)
    chunk_size = 10 * row.sample_rate

    kernel_size = int(chunk_size/1000) + 1
    audio_array = audio.get_array_of_samples()
    audio_median = signal.medfilt(audio_array, kernel_size=kernel_size)

    audio_diff = audio_array - audio_median
    audio.export(os.path.join(ROOT_PATH, f"{idx}.wav"), format="wav")

    wav_path = str(row.path)
    wav_name = wav_path.split("/")[-1]
    wav_name = wav_name.split(".")[0]

    final_meta["std"].append(np.std(audio_diff))
    final_meta["original_wav"].append(wav_name)
    final_meta["begin"].append(row.sub_init)
    final_meta["end"].append(chunk_size)
    final_meta["indexes"].append(idx)
    final_meta["label"].append(row.label)

df = pd.DataFrame.from_dict(final_meta)
df.to_csv(os.path.join(ROOT_PATH, f"inclusion_{INCLUSION_RADIUS}_exclusion_{EXCLUSION_RADIUS}_info.csv"), index=False)

labels = list(df["indexes"])
std_dev = list(df["std"])

plt.figure(figsize=[15, 6])
plt.xlabel("Audio Index")
plt.ylabel("Standard Deviation")
plt.bar(labels, std_dev)
plt.xlim([-1, df.shape[0]])
ax = plt.gca()
ax.yaxis.grid()
ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
plt.savefig(os.path.join(ROOT_PATH, "plot.svg"))
plt.show()
