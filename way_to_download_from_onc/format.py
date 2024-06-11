import os
import re
import sys
import pandas as pd

from tqdm import tqdm
from pydub import AudioSegment
from datetime import datetime, timedelta
from utils import bcolors, create_dir, zulu_string_to_datetime, pandas_timestamp_to_onc_format, read_data_frame_from_feather_file


def split_and_save_wav(raw_wav_directory, output_save_dir, data_from_range, wav_file_names, inclusion_radius=0, interval_ais_data_directory=''):
    # Define project constants.
    five_minutes = timedelta(minutes = 5)
    csv_data_to_fetch = []
    for file_idx, (begin, end) in enumerate(tqdm(zip(data_from_range.begin, data_from_range.end), total=len(data_from_range.index))):
        wav_files_in_range = []
        ais_begin_datetime = zulu_string_to_datetime(begin)
        ais_end_datetime = zulu_string_to_datetime(end)
        for wav_file in wav_file_names:
            # print('wav file\t', wav_file)
            wav_timestamp = os.path.splitext(wav_file)[0].split("_")[-1]
            wav_datetime = zulu_string_to_datetime(wav_timestamp)
            # print('wav_datetime\t', wav_datetime)
            # print('ais_begin_datetime\t', ais_begin_datetime)
            # print('ais_end_datetime\t', ais_end_datetime)
            # print('five_minutes\t', five_minutes)
            # print(wav_datetime >= (ais_begin_datetime - five_minutes),(wav_datetime <= ais_end_datetime))
            # print('-'*10)
            if (wav_datetime >= (ais_begin_datetime - five_minutes)) and (wav_datetime <= ais_end_datetime):
                wav_files_in_range.append((wav_datetime, wav_file))
                # print('wav_files_in_range\t', wav_files_in_range)
                # sys.exit()
        if len(wav_files_in_range) == 0:
            # print('continue')
            continue
        wav_files_in_range.sort()
        try:
            # print('try')
            audio_segment = AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_files_in_range[0][1]))
            start_time = (wav_files_in_range[0][0] - ais_begin_datetime).total_seconds() * 1000
            audio_segment = audio_segment[start_time:]

            for idx, (wav_datetime, wav_file_name) in enumerate(wav_files_in_range):
                if idx == 0 or idx == len(wav_files_in_range):
                    continue
                audio_segment += AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_file_name))

            last_segment = AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_files_in_range[-1][1]))
            end_time = (ais_end_datetime - wav_files_in_range[-1][0]).total_seconds() * 1000
            audio_segment += last_segment[:end_time]

            audio_segment.export(os.path.join(output_save_dir, str(file_idx) + ".wav"), format="wav")
            csv_data_to_fetch.append(
                    (
                        pandas_timestamp_to_onc_format(ais_begin_datetime),
                        pandas_timestamp_to_onc_format(ais_end_datetime),
                        str(file_idx),
                    )
                )
        except:
            print(f"Error while exporting audio segment")

    interval_csv_file = open(os.path.join(output_save_dir, "intervals.csv"), "w")
    interval_csv_file.write("begin,end,wav_file\n")

    for interval in csv_data_to_fetch:
        interval_csv_file.write(",".join(str(entry) for entry in interval) + "\n")

    interval_csv_file.close()

def making_wav(raw_wav_directory, output_save_dir, data_from_range, wav_file_names, inclusion_radius=0, interval_ais_data_directory='',i=0):
    # Define project constants.
    five_minutes = timedelta(minutes = 5)
    wav_files_in_range = []
    for idxjack, wav_file in tqdm(enumerate(wav_file_names), total=len(wav_file_names)):# 遍历音频文件
        # 判断wav对应的csv文件的时间范围
        # print(idxjack)
        if idxjack <= i:
            continue
        if idxjack > i+10:
            break
        wav_timestamp = os.path.splitext(wav_file)[0].split("_")[-1]
        wav_datetime = zulu_string_to_datetime(wav_timestamp)
        wav_basename = os.path.basename(wav_file)
        date_check = wav_basename.split("_")[1].split("T")[0]
        for date_file in data_from_range:#找到ais文件进行读取解析
            if date_check == os.path.basename(date_file).split("_")[1].split('T')[0]:
                single_vessel = pd.read_csv(date_file)# 找到对应日期的ais文件进行解析
                print(f'loading {date_file}')
            else:
                continue
            counter = 0
            for file_idx, (begin, end, ship_id, type_and_cargo) in enumerate(zip(single_vessel.begin, single_vessel.end, single_vessel.ID, single_vessel.Type)):# 不需要这样
                if os.path.exists(os.path.join(output_save_dir, str(begin)+'_'+str(file_idx)+'_id_'+str(ship_id)+"_typecargo_"+str(type_and_cargo) + ".wav")):
                    continue
                if begin == end:
                    continue
                if (type_and_cargo <80 and type_and_cargo >= 70) or type_and_cargo == 52 or type_and_cargo == 37:
                    # print('passing cargo ship')
                    continue
                ais_begin_datetime = zulu_string_to_datetime(begin)
                ais_end_datetime = zulu_string_to_datetime(end)
                if ais_begin_datetime > wav_datetime + five_minutes:# 直接错过，跳出循环
                    break
                if ais_begin_datetime < wav_datetime:
                    continue
                if (wav_datetime > (ais_begin_datetime - five_minutes)) and (wav_datetime <= ais_end_datetime):# 有交集
                    wav_files_in_range.append((wav_datetime, wav_file))

                if len(wav_files_in_range) == 0:
                    continue
                wav_files_in_range.sort()
                try:
                    # print(file_idx, wav_file,begin,end,'*'*10)
                    audio_segment = AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_files_in_range[0][1]))
                    start_time = (ais_begin_datetime - wav_files_in_range[-1][0]).total_seconds() * 1000
                    if start_time < 0:
                        start_time = 0
                    
                    last_segment = AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_files_in_range[-1][1]))
                    end_time = (ais_end_datetime - wav_files_in_range[-1][0]).total_seconds() * 1000
                    if end_time < 0:
                        continue
                    # print("start\tend\t", ais_begin_datetime, ais_end_datetime, wav_files_in_range[-1][0])
                    if end_time > len(last_segment):
                        end_time = len(last_segment)
                    
                    audio_segment = last_segment[start_time:end_time]
                    
                    counter += 1
                    filename = os.path.join(output_save_dir, str(begin)+'_'+str(file_idx)+'_id_'+str(ship_id)+"_typecargo_"+str(type_and_cargo) + ".wav")
                    audio_segment.export(filename, format="wav")
                    print(f'saved to {filename}')
                    
                except:
                    print('except error')
                    break

                if ais_begin_datetime > wav_datetime + five_minutes:# 直接错过，跳出循环
                    print('跳出')
                    break
            if ais_begin_datetime > wav_datetime + five_minutes:# 直接错过，跳出循环
                print('跳出')
                break

def group_wav_from_range(classified_wav_directory, scenario_interval_dir, interval_ais_data_directory, raw_wav_directory, inclusion_radius):

    # Define exclusion range as an offset from the inclusion.
    inclusion_radius = 1000
    exclusion_radius = 10000 + inclusion_radius

    directory_name = "inclusion_" + str(inclusion_radius) + "_exclusion_" + str(exclusion_radius)
    range_directory = create_dir(classified_wav_directory, directory_name)

    # Get a list of the WAV file names.
    wav_file_names = os.listdir(raw_wav_directory)
    interval_file_names = os.listdir(scenario_interval_dir)
    # Read background range data from csv.
    background_interval_file_names = [file for file in interval_file_names if file.lower().endswith('background_intervals.csv')]
    background_interval_data = pd.read_csv(os.path.join(scenario_interval_dir, background_interval_file_names[0]))

    # Get data from only the selected inclusion/exclusion range.
    background_data_from_range = background_interval_data[background_interval_data["exclusion_radius"] == exclusion_radius]

    # Identify, split, and save corresponding wav files into the directory.
    print(f"Generating and saving background files.")
    background_save_dir = create_dir(range_directory, "background")
    # print('background save dir\t', background_save_dir)
    split_and_save_wav(raw_wav_directory, background_save_dir, background_data_from_range, wav_file_names)

    # Read vessel intervals range data from csv.
    vessel_interval_file_names = [file for file in interval_file_names if file.lower().endswith('unique_vessel_intervals.csv')]
    vessel_interval_data = pd.read_csv(os.path.join(scenario_interval_dir, vessel_interval_file_names[0]))

    # Get data from only the selected inclusion/exclusion range.
    vessel_data_from_range = vessel_interval_data[vessel_interval_data["exclusion_radius"] == exclusion_radius]
    # Identify, split, and save corresponding wav files into the directory.
    print(f"Generating and saving unique vessel within range files.")
    vessel_save_dir = create_dir(range_directory, "vessel")
    split_and_save_wav(raw_wav_directory, vessel_save_dir, vessel_data_from_range, wav_file_names, inclusion_radius, interval_ais_data_directory)
    
    
def wav_all(classified_wav_directory, scenario_interval_dir, interval_ais_data_directory, raw_wav_directory, inclusion_radius):
    import glob 
    directory_name = "icat"
    range_directory = create_dir(classified_wav_directory, directory_name)
    # Get a list of the WAV file names.
    wav_file_names = os.listdir(raw_wav_directory)
    interval_file_names = os.listdir(scenario_interval_dir)
    vessel_interval_file_names = [file for file in interval_file_names if file.lower().endswith('unique_vessel_intervals.csv')]
    all_csv_path = glob.glob("./underwater/03ff/*")
    vessel_data_from_range = all_csv_path
    print(f"Generating and saving unique vessel within range files.")
    vessel_save_dir = create_dir(range_directory, "vessel")
    for i in range(20410, 30000, 10):
        making_wav(raw_wav_directory, vessel_save_dir, vessel_data_from_range, wav_file_names, inclusion_radius, interval_ais_data_directory,i)
    
def wav_all_processed(classified_wav_directory, scenario_interval_dir, interval_ais_data_directory, raw_wav_directory, inclusion_radius):
    import glob 
    directory_name = "makek"
    range_directory = create_dir(classified_wav_directory, directory_name)
    # Get a list of the WAV file names.
    wav_file_names = os.listdir(raw_wav_directory)
    interval_file_names = os.listdir(scenario_interval_dir)
    vessel_interval_file_names = [file for file in interval_file_names if file.lower().endswith('unique_vessel_intervals.csv')]
    # vessel_interval_data = pd.read_csv(os.path.join(scenario_interval_dir, vessel_interval_file_names[0]))
    # read all folder from 03b
    all_csv_path = glob.glob("./underwater/03b/*")
    # vessel_interval_data = pd.read_csv("./underwater/03a_parsed_ais_files/ICLISTENAF2523_20200715T000000.000Z_20220129T000000.000Z_unique_vessel_intervals.csv")
    # Get data from only the selected inclusion/exclusion range.
    vessel_data_from_range = all_csv_path
    # Identify, split, and save corresponding wav files into the directory.
    print(f"Generating and saving unique vessel within range files.")
    vessel_save_dir = create_dir(range_directory, "vessel")
    wav_file_preprocess(raw_wav_directory, vessel_save_dir, vessel_data_from_range, wav_file_names, inclusion_radius, interval_ais_data_directory)


import threading

def process_wav(raw_wav_directory, output_save_dir, data_from_range, wav_file, counter):
    # Define project constants.
    five_minutes = timedelta(minutes = 5)
    csv_data_to_fetch = []
    # 判断wav对应的csv文件的时间范围
    wav_timestamp = os.path.splitext(wav_file)[0].split("_")[-1]
    wav_datetime = zulu_string_to_datetime(wav_timestamp)
    wav_basename = os.path.basename(wav_file)
    date_check = wav_basename.split("_")[1].split("T")[0]
    
    for date_file in data_from_range:#找到ais文件进行读取解析
        if date_check == os.path.basename(date_file).split("_")[1].split('T')[0]:
            single_vessel = pd.read_csv(date_file)
        else:
            continue
        
        for file_idx, (begin, end, ship_id, type_and_cargo) in tqdm(enumerate(zip(single_vessel.begin, single_vessel.end, single_vessel.ID, single_vessel.Type))):
            wav_files_in_range = []
            ais_begin_datetime = zulu_string_to_datetime(begin)
            ais_end_datetime = zulu_string_to_datetime(end)
            if ais_begin_datetime > wav_datetime + five_minutes:# 直接错过，跳出循环
                break
            if (wav_datetime > (ais_begin_datetime - five_minutes)) and (wav_datetime <= ais_end_datetime):# 有交集
                wav_files_in_range.append((wav_datetime, wav_file))

            if len(wav_files_in_range) == 0:
                continue
            wav_files_in_range.sort()
            try:
                audio_segment = AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_files_in_range[0][1]))
                start_time = (ais_begin_datetime - wav_files_in_range[0][0]).total_seconds() * 1000
                if start_time < 0:
                    start_time = 0
                audio_segment = audio_segment[start_time:]

                for idx, (wav_datetime, wav_file_name) in enumerate(wav_files_in_range):
                    if idx == 0 or idx == len(wav_files_in_range):
                        continue
                    audio_segment += AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_file_name))

                last_segment = AudioSegment.from_wav(os.path.join(raw_wav_directory, wav_files_in_range[-1][1]))
                end_time = (ais_end_datetime - wav_files_in_range[-1][0]).total_seconds() * 1000
                if end_time > len(last_segment):
                    end_time = len(last_segment)
                if end_time - start_time < 3200:
                    continue
                audio_segment = last_segment[start_time:end_time]
                
                # print(start_time, end_time,file_idx,counter)
                
                save_path = os.path.join(output_save_dir, str(begin)+'_'+str(file_idx)+'_id_'+str(ship_id)+"_typecargo_"+str(type_and_cargo) + ".wav")
                audio_segment.export(save_path, format="wav")
                
                csv_data_to_fetch.append(
                        (
                            pandas_timestamp_to_onc_format(ais_begin_datetime),
                            pandas_timestamp_to_onc_format(ais_end_datetime),
                            str(file_idx),
                            str(type_and_cargo),
                            str(ship_id),
                            str(save_path),
                        )
                    )
            except:
                pass
            
            interval_csv_file = open(os.path.join(output_save_dir, "intervals.csv"), "a")
            if counter==0:
                interval_csv_file.write("begin,end,wav_file,ship_id,typecargo,filepath\n")
                counter += 1
            for interval in csv_data_to_fetch:
                interval_csv_file.write(",".join(str(entry) for entry in interval) + "\n")

            # interval_csv_file.close()
            if ais_begin_datetime > wav_datetime + five_minutes:# 直接错过，跳出循环
                break
        if ais_begin_datetime > wav_datetime + five_minutes:# 直接错过，跳出循环
            break

from concurrent.futures import ThreadPoolExecutor

def wav_file_preprocess(raw_wav_directory, output_save_dir, data_from_range, wav_file_names, inclusion_radius=0, interval_ais_data_directory=''):
    executor = ThreadPoolExecutor(max_workers=32) # 设置线程池大小
    futures = []
    counter = 0
    for wav_file in tqdm(wav_file_names):
        future = executor.submit(process_wav, raw_wav_directory, output_save_dir, data_from_range, wav_file, counter) # 提交任务到线程池
        counter+=1
        futures.append(future)
    
    for future in futures:
        future.result()