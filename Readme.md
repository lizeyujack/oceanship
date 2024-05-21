# Oceanship: A Large-Scale Dataset for Underwater Audio Target Recognition
<!-- ![oceanship](.\fig\datafetch.png)
 -->

<div style="text-align: center;">
  <img src="./fig/datafetch.png" alt="oceanship" width="50%">
</div>
The recognition of underwater audio plays a significant role
in identifying a vessel while it is in motion. Classifying underwater vessels involves training a network to extract features from audio data and
predict the vessel type. The current UATR dataset exhibits shortcomings in both duration and sample quantity. In this paper, we propose
Oceanship4, a large-scale and diverse underwater audio dataset. This
dataset comprises 15 categories, spans a total duration of 65 hours, and
includes comprehensive annotation information such as coordinates, velocity, vessel types, and timestamps. We compiled the dataset by crawling
and organizing original communication data from the Ocean Communication Network (ONC5) database between 2021 and 2022. While audio
retrieval tasks are well-established in general audio classification, they
have not been explored in the context of underwater audio recognition.
Leveraging the Oceanship dataset, we introduce a baseline model named
Oceannet for underwater audio retrieval. This model achieves a recall at
1 (R@1) accuracy of 67.11% and a recall at 5 (R@5) accuracy of 99.13%
on the Deepship dataset.

# Oceanship are available here:
- Oceanship(FG.) version: https://pan.baidu.com/s/19-K_QNvINT-ZlfHzd0HuSw password: 2fme 

- Oceanship(Full) version(28GB): https://pan.baidu.com/s/1FzqxKmmENbWzJUJafg9i7Q password: 8igj 

- Oceanship(CG.) version: We will not upload Oceanship(CG.), but you can get by doing: Oceanship(Full) - Oceanship(FG.).

# Way to extract wav files from multiple multi-compressed files
- The original downloaded files should be formatted like this:
```txt
ocean_dataset_archive.aa  ocean_dataset_archive.af  
ocean_dataset_archive.ab  ocean_dataset_archive.ag 
ocean_dataset_archive.ac  ocean_dataset_archive.ah
ocean_dataset_archive.ad  ocean_dataset_archive.ai 
ocean_dataset_archive.ae  ocean_dataset_archive.aj
```
- Then please run the unzip code below:
```bash
cat ocean_dataset_archive.aa ocean_dataset_archive.ab ocean_dataset_archive.ac ocean_dataset_archive.ad ocean_dataset_archive.ae ocean_dataset_archive.af ocean_dataset_archive.ag ocean_dataset_archive.ah ocean_dataset_archive.ai ocean_dataset_archive.aj > ocean_dataset_archive.tar
tar -xvf ocean_dataset_archive.tar
```
- Next step, move these files to your desired location
```bash
mkdir /path to your file/oceanship
find /path to your file/cluster/home/lizeyu/oceandil/dataset/ocean_dataset/v100_preprocessed_89_09_31/ -type f -exec mv {} /path to your file/oceanship/ \;
```

# Data-fetching from ONC is coming soon

# Enhanced information based on MMSI
I have obtained the MMSI information. By crawling [shipsfind艘船网](www.shipfinder.com), I was able to get more details about the ship itself, which contains f"[{formatted_time}],{ais_mmsi_elements},{ais_callsign_elements},{ais_heading_elements},{ais_course_elements},{ais_imo_elements},{ais_sog_elements} elements},{ais_course_elements},{ais_imo_elements},{ais_sog_elements},{ais_shipType_elements},{ais_lon_elements},{ais_lat_elements },{ais_length_elements},{ais_width_elements},{ais_draught_elements},{ais_dest_elements},{ais_eta_elements},{ais_lastTime_elements} {ais_draught_elements},{ais_dest_elements},{ais_eta_elements},{ais_lastTime_elements} \n". You can find these two files from this repo with a ".txt" suffix.
