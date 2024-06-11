import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from pydub.utils import mediainfo
from utils import read_data_frame_from_feather_file, get_min_max_normalization, get_min_max_values_from_df

#CLASSES = ["passengership", "tug", "tanker", "cargo", "other", "background"]
CLASSES = ["passengership", "tug", "tanker", "cargo", "background"]

def get_class_from_code(code):
    if code == 0:
        return "background"
    elif code == 52:
        return "tug"
    elif code >= 60 and code <= 69:
        return "passengership"
    elif code >= 70 and code <= 79:
        return "cargo"
    elif code >= 80 and code <= 89:
        return "tanker"
    
    return "other"


def get_mean_ctd_from_range(data_frame, begin_time, end_time):
    columns = ["t1", "c1", "p1", "sal", "sv"]

    ctd_df = data_frame[data_frame['date'].between(begin_time, end_time, inclusive="both")]
    ctd_df = ctd_df[columns]
    t1, c1, p1, sal, sv = ctd_df.apply(pd.to_numeric).mean()

    return t1, c1, p1, sal, sv


def get_full_ctd_dataframe(clean_ctd_directory):
    data_files = [file for file in os.listdir(clean_ctd_directory)]

    files = [
        read_data_frame_from_feather_file(
            os.path.join(clean_ctd_directory, file)
        )
        for file in data_files
    ]

    df = pd.concat(files)
    df.sort_values(by=['date'], ignore_index=True, inplace=True)

    return df


def generate_full_metadata(root_path, clean_ctd_directory, interval_ais_dir, inclusion_radius):

    columns = ["label", "duration_sec", "path", "sample_rate", "class_code",
               "date", "MMSI", "t1", "c1", "p1", "sal", "sv", "t1_norm",
               "c1_norm", "p1_norm", "sal_norm", "sv_norm"]

    metadata = {key:[] for key in columns}

    dir_vessel = os.path.join(root_path, "vessel")
    meta_vessel = os.path.join(dir_vessel, "intervals.csv")
    df_vessel = pd.read_csv(meta_vessel)

    ctd_df = get_full_ctd_dataframe(clean_ctd_directory)
    min_max_ctd = get_min_max_values_from_df(ctd_df, ["t1", "c1", "p1", "sal", "sv"])

    print(f"Vessel Metafile")
    for _, row in tqdm(df_vessel.iterrows(), total=df_vessel.shape[0]):
        begin_time = row["begin"].replace("-","").replace(":","")
        end_time = row["end"].replace("-","").replace(":","")

        interval_file = os.path.join(interval_ais_dir, f"{begin_time}_{end_time}_interval_data.feather")
        metadata_file = read_data_frame_from_feather_file(interval_file)

        class_code = metadata_file[metadata_file["distance_to_hydrophone"] <= inclusion_radius].type_and_cargo.unique()[0]
        mmsi = metadata_file[metadata_file["distance_to_hydrophone"] <= inclusion_radius].mmsi.unique()[0]
        path = os.path.join(dir_vessel, f'{row["wav_file"]}.wav')
        info = mediainfo(path)

        t1, c1, p1, sal, sv = get_mean_ctd_from_range(ctd_df, begin_time, end_time)

        # Append AIS data
        metadata["class_code"].append(class_code)
        metadata["MMSI"].append(mmsi)
        metadata["path"].append(path)
        metadata["date"].append(row["begin"].replace("-","").split("T")[0])
        metadata["duration_sec"].append(info["duration"])
        metadata["sample_rate"].append(info["sample_rate"])
        metadata["label"].append(get_class_from_code(class_code))

        # Append CTD data
        metadata["t1"].append(t1)
        metadata["c1"].append(c1)
        metadata["p1"].append(p1)
        metadata["sal"].append(sal)
        metadata["sv"].append(sv)

        # Append normalized CTD data
        metadata["t1_norm"].append(get_min_max_normalization(t1, min_max_ctd["t1"][0], min_max_ctd["t1"][1]))
        metadata["c1_norm"].append(get_min_max_normalization(c1, min_max_ctd["c1"][0], min_max_ctd["c1"][1]))
        metadata["p1_norm"].append(get_min_max_normalization(p1, min_max_ctd["p1"][0], min_max_ctd["p1"][1]))
        metadata["sal_norm"].append(get_min_max_normalization(sal, min_max_ctd["sal"][0], min_max_ctd["sal"][1]))
        metadata["sv_norm"].append(get_min_max_normalization(sv, min_max_ctd["sv"][0], min_max_ctd["sv"][1]))

    dir_background = os.path.join(root_path, "background")
    meta_backgorund = os.path.join(dir_background, "intervals.csv")
    df_background = pd.read_csv(meta_backgorund)

    print(f"Background Metafile")
    for _, row in tqdm(df_background.iterrows(), total=df_background.shape[0]):
        begin_time = row["begin"].replace("-","").replace(":","")
        end_time = row["end"].replace("-","").replace(":","")

        interval_file = os.path.join(interval_ais_dir, f"{begin_time}_{end_time}_interval_data.feather")    
        metadata_file = read_data_frame_from_feather_file(interval_file)

        path = os.path.join(dir_background, f'{row["wav_file"]}.wav')
        info = mediainfo(path)

        t1, c1, p1, sal, sv = get_mean_ctd_from_range(ctd_df, begin_time, end_time)

        # Append AIS data
        class_code = 0
        metadata["class_code"].append(class_code)
        metadata["MMSI"].append(0)
        metadata["path"].append(path)
        metadata["date"].append(row["begin"].replace("-","").split("T")[0])
        metadata["duration_sec"].append(info["duration"])
        metadata["sample_rate"].append(info["sample_rate"])
        metadata["label"].append(get_class_from_code(class_code))

        # Append CTD data
        metadata["t1"].append(t1)
        metadata["c1"].append(c1)
        metadata["p1"].append(p1)
        metadata["sal"].append(sal)
        metadata["sv"].append(sv)

        # Append normalized CTD data
        metadata["t1_norm"].append(get_min_max_normalization(t1, min_max_ctd["t1"][0], min_max_ctd["t1"][1]))
        metadata["c1_norm"].append(get_min_max_normalization(c1, min_max_ctd["c1"][0], min_max_ctd["c1"][1]))
        metadata["p1_norm"].append(get_min_max_normalization(p1, min_max_ctd["p1"][0], min_max_ctd["p1"][1]))
        metadata["sal_norm"].append(get_min_max_normalization(sal, min_max_ctd["sal"][0], min_max_ctd["sal"][1]))
        metadata["sv_norm"].append(get_min_max_normalization(sv, min_max_ctd["sv"][0], min_max_ctd["sv"][1]))

    final_metadata = pd.DataFrame.from_dict(metadata)
    final_metadata.to_csv(os.path.join(root_path, "metadata.csv"), index=False)


def generate_oversampled_metadata(metadata_file, root_path, inbalance_limit=2):
    meta = pd.read_csv(os.path.join(root_path, metadata_file))
    meta_dict = {label:meta[meta["label"] == label]["duration_sec"].sum() for label in CLASSES}

    bigger_class = max(meta_dict, key=meta_dict.get)
    smaller_class = min(meta_dict, key=meta_dict.get)

    relation = min(inbalance_limit, meta_dict[bigger_class]/meta_dict[smaller_class])

    final_size = meta_dict[smaller_class] * relation
    classes_df = []

    for label in CLASSES:
        sub_metadata = meta[meta["label"] == label].sample(frac=1, random_state=42).reset_index(drop=True)
        current_size = 0
        max_row_num = len(sub_metadata.index)
        row_num = 0
        rows = []
        while current_size < final_size:
            current_size += sub_metadata.iloc[row_num]["duration_sec"]
            rows.append(row_num)
            
            row_num += 1
            if row_num == max_row_num:
                classes_df.append(sub_metadata[sub_metadata.index.isin(rows)])
                row_num = 0
                rows = []
        classes_df.append(sub_metadata[sub_metadata.index.isin(rows)])

    file_name = metadata_file.split(".")[0]
    balanced_set = pd.concat(classes_df, ignore_index=True)
    balanced_set.sample(frac=1, random_state=42).reset_index(drop=True).to_csv(os.path.join(root_path, f"{file_name}_oversampled.csv"), index=False)


def generate_undersampled_metadata(metadata_file, root_path):
    meta = pd.read_csv(os.path.join(root_path, metadata_file))
    meta_dict = {label:meta[meta["label"] == label]["duration_sec"].sum() for label in CLASSES}
    min_time_label = min(meta_dict, key=meta_dict.get)
    idx_dict = {}
    for label in CLASSES:
        base_time = meta_dict[min_time_label]
        idx_dict[label] = []
        for idx, row in meta[meta["label"] == label].iterrows():
            idx_dict[label].append(idx)
            base_time = base_time - row.duration_sec
            if base_time <= 0:
                break
    idx_list = [idx for _, sublist in idx_dict.items() for idx in sublist]
    file_name = metadata_file.split(".")[0]
    meta[meta.index.isin(idx_list)].to_csv(os.path.join(root_path, f"{file_name}_undersampled.csv"), index=False)


def generate_balanced_metadata(metadata_file, root_path):
    generate_oversampled_metadata(metadata_file, root_path)
    generate_undersampled_metadata(metadata_file, root_path)


def get_metadata_for_small_times(root_path, metadata_file, seconds):
    initial_meta = pd.read_csv(os.path.join(root_path, metadata_file))
    desired_classes = CLASSES.copy()
    desired_classes.remove("other")
    desired_classes.remove("background")
    duration = []
    for label in desired_classes:
        time = initial_meta[initial_meta["label"] == label]["duration_sec"].sum()
        duration.append(time)
    
    max_duration = max(duration)

    col = list(initial_meta.columns) + ['sub_init']
    class_duration = {key:0 for key in CLASSES}
    metadata = []
    for index, row in tqdm(initial_meta.iterrows(), total=initial_meta.shape[0]):
        duration = int(row.duration_sec)
        row["duration_sec"] = float(seconds)
        if class_duration[row.label] > max_duration:
            continue
        class_duration[row.label] += duration
        for i in range(0, duration, seconds):
            metadata.append(list(row) + [i])

    metadata_pd = pd.DataFrame(metadata, columns=col)
    file_name = metadata_file.split(".")[0]
    metadata_pd.to_csv(os.path.join(root_path, f"{file_name}_{seconds}s.csv"), index=False)


def split_dataset(root_path, metadata_file, validation_split=0.2, test_split=0.1, random_seed=42):
    metadata = pd.read_csv(os.path.join(root_path, metadata_file))
    metadata = metadata.sample(frac=1, random_state=random_seed).reset_index(drop=True)

    meta_dict = {label:metadata[metadata["label"] == label].count()[0] for label in CLASSES}
    smaller_class = min(meta_dict, key=meta_dict.get)

    # Creating data indices for training and validation splits:
    #dataset_size = len(metadata.index)
    dataset_size = meta_dict[smaller_class]*len(CLASSES)
    test_idx = int(np.floor(test_split * dataset_size))
    validation_idx = test_idx + int(np.floor(validation_split * dataset_size))

    test_dataset = metadata[:test_idx]
    val_dataset = metadata[test_idx:validation_idx]
    train_dataset = metadata[validation_idx:]

    file_name = metadata_file.split(".")[0]

    test_dataset.to_csv(os.path.join(root_path, f"{file_name}_test.csv"), index=False)
    val_dataset.to_csv(os.path.join(root_path, f"{file_name}_validation.csv"), index=False)
    train_dataset.to_csv(os.path.join(root_path, f"{file_name}_train.csv"), index=False)


def main():
    print(f"Saving metadata into CSV")

    # 1 - Generate the metadata for the full dataset.
    print(f"Generate the metadata for the full dataset")
    inclusion_radius = 4000
    root_path = "/workspaces/underwater/dataset/07_classified_wav_files/inclusion_4000_exclusion_6000/metadata/filtered/"
    interval_ais_dir = "/workspaces/underwater/dataset/06b_interval_ais_data/"
    clean_ctd_directory = "/workspaces/underwater/dataset/09_cleaned_ctd_files"
    #generate_full_metadata(root_path, clean_ctd_directory, interval_ais_dir, inclusion_radius)

    # 2 - Split dataset into small periods of time.
    print(f"Split dataset into small periods of time")
    seconds = 1
    metadata_file = os.path.join(root_path, f"metadata.csv")
    #get_metadata_for_small_times(root_path, metadata_file, seconds)

    # 3 - Split the original data into train, test and validation datasets.
    print(f"Split dataset into train, test and validation datasets")
    validation_split = 0.2
    test_split = 0.1
    metadata_file_sec = os.path.join(root_path, f"filtered_metadata_{seconds}s.csv")
    split_dataset(root_path, metadata_file_sec, validation_split=validation_split, test_split=test_split)

    # 4 - Generate balanced versions of the metatdata.
    print(f"Generate balanced versions of the metatdata")
    generate_balanced_metadata(f"filtered_metadata_{seconds}s_train.csv", root_path)



if __name__ == "__main__":
    main()