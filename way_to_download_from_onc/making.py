import json
import csv
import os
import glob
from tqdm import tqdm
def making_one_file(filename):
    '''
    @Date : 2023-07-29
    @Time : 19:41:59
    '''
    with open(filename,'r') as f:
        data = json.load(f)
    temp_dir = list()
    for i in data:
        if "type_and_cargo" in i.keys():
            temp_dir.append(i)
        
    # 定义存储结果的列表
    id_time_list = []

    # 遍历所有字典
    for i, d in enumerate(temp_dir):
        # 获取字典中的id、timestamp和type
        id_value = d['id']
        timestamp = d['ais_timestamp']
        type_value = d['type_and_cargo']
        
        # 如果是第一个字典或者当前字典的id与前一个字典的id不同，说明出现了一个新的id片段
        if i == 0 or id_value != temp_dir[i-1]['id']:
            # 记录该片段的起始时间、截止时间和种类
            try:
                if id_time_list[-1]['start_time'] == id_time_list[-1]['end_time']:
                    id_time_list.pop()
            except:
                pass
           
            id_time = {
                'id': id_value,
                'start_time': timestamp,
                'end_time': timestamp,
                'type_and_cargo': type_value
            }
            # 将该片段的起始时间、截止时间和种类添加到列表中
            id_time_list.append((id_time['id'], id_time['start_time'], id_time['end_time'], id_time['type_and_cargo']))
        # 如果当前字典的id与前一个字典的id相同，更新当前id片段的截止时间和种类
        else:
            id_time_list[-1] = (id_time['id'], id_time['start_time'], timestamp, id_time['type_and_cargo'])
    save_makeed_file(filename, id_time_list)# 写入
            
def save_makeed_file(filename, id_time_list):
    # 定义保存csv文件的路径和文件名
    # final_path = './underwater/03bb'
    # final_path = './underwater/03ff/'
    final_path = "./underwater/03f84/"
    
    fullpath = os.path.join(final_path, os.path.basename(filename)[:-5])
    filename = f'{fullpath}.csv'
    # 打开csv文件
    with open(filename, mode='w', newline='') as csvfile:
        # 创建csv.writer对象
        writer = csv.writer(csvfile)
        # 写入表头
        writer.writerow(['ID', 'begin', 'end', 'Type','mmsi'])
        
        # 写入元组
        for id_time in id_time_list:
            if id_time[1] == id_time[2]:
                continue
            writer.writerow([id_time[0], id_time[1], id_time[2], id_time[3], id_time[4]])
    csvfile.close()

def making_icat(filename):
    '''
    @Date : 2023-07-29
    @Time : 19:41:59
    '''
    
    with open(filename,'r') as f:
        data = json.load(f)
    temp_dir = list()
    for i in data:
        if "type_and_cargo" in i.keys():
            temp_dir.append(i)
        
    # 定义存储结果的列表
    id_time_list = []

    # 遍历所有字典
    for i, d in enumerate(temp_dir):
        # 获取字典中的id、timestamp和type
        id_value = d['id']
        timestamp = d['ais_timestamp']
        type_value = d['type_and_cargo']
        type_mmsi = d['mmsi']
        
        id_time_list.append((id_value, timestamp, "",type_value,type_mmsi))
    save_makeed_file(filename, id_time_list)
# 定义主函数

def making_latest():
    import glob
    from tqdm import tqdm
    import pandas as pd
    making_dir = glob.glob("./underwater/03f/*.csv")
    for _, filename in tqdm(enumerate(making_dir), total=len(making_dir)):
        df = pd.read_csv(filename)
        id_time_list = list()
        for i, (timestamp, type_value, id_value)  in enumerate(zip(df.begin, df.Type, df.ID)):
            if i == 0 or type_value != df.Type[i-1]:
                # 记录该片段的起始时间、截止时间和种类
                try:
                    if id_time_list[-1]['start_time'] == id_time_list[-1]['end_time']:
                        id_time_list.pop()
                except:
                    pass
            
                id_time = {
                    'id': id_value,
                    'start_time': timestamp,
                    'end_time': timestamp,
                    'type_and_cargo': type_value
                }
                # 将该片段的起始时间、截止时间和种类添加到列表中
                id_time_list.append((id_time['id'], id_time['start_time'], id_time['end_time'], id_time['type_and_cargo']))
            # 如果当前字典的id与前一个字典的id相同，更新当前id片段的截止时间和种类
            else:
                id_time_list[-1] = (id_time['id'], id_time['start_time'], timestamp, id_time['type_and_cargo'])
        save_makeed_file(filename, id_time_list)
if __name__ == '__main__':
    filelist = glob.glob('./underwater/03_parsed_ais_files/*')
    for _, filename in tqdm(enumerate(filelist),total = len(filelist)):
        making_icat(filename)