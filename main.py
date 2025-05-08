import argparse
import json
from pathlib import Path

from src.openmeteo import *
from src.hadoop import *
from src.kaggle import *
from src.utils import *
import logging

# ===================================================
#     This script runs on remote hadoop machine     #
# ===================================================

def execute_command_chain(commands):
    for command in commands:
        run_local_command(command)

def set_replication_factor(container_name, hdfs_target_dir):
    command = set_replication_factor_command(container_name, hdfs_target_dir, 3)
    run_local_command(command)

def upload_to_hadoop(container_name, local_source_dir, hdfs_target_dir, staging_dir_in_container):
    commands = [
        create_hdfs_directory_command(container_name, hdfs_target_dir),
        copy_files_to_docker_command(container_name, local_source_dir, staging_dir_in_container),
        upload_to_hdfs_command(container_name, hdfs_target_dir, staging_dir_in_container),
    ]
    
    execute_command_chain(commands)

def upload_yearly_openmeteo_data(year, openmeteo_dir, hdfs_target_dir, container_name, staging_dir_in_container):
    local_source_dir = openmeteo_dir.joinpath(f"{year}")
    
    upload_to_hadoop(container_name, local_source_dir, hdfs_target_dir, staging_dir_in_container)

def main():
    # 0. Logging configuration
    log_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    log_file = 'acquisition.log'
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(log_formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    logger = logging.getLogger('Logger')
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # 1. Inputs
    # Processed scope
    years = [2022]
    
    # Output directories
    base_output_dir = Path('./data')

    kaggle_dataset_url = 'wisekinder/poland-air-quality-monitoring-dataset-2017-2023'
    files_to_download_from_kaggle = [
        'stations_metadata.csv',
        'joint_data_2017-2023/C6H6_1g_joint_2017-2023.csv',
        'joint_data_2017-2023/NO2_1g_joint_2017-2023.csv',
        'joint_data_2017-2023/PM10_1g_joint_2017-2023.csv',
        'joint_data_2017-2023/PM25_1g_joint_2017-2023.csv',
        'joint_data_2017-2023/SO2_1g_joint_2017-2023.csv'
    ]
    air_quality_kaggle_dir = base_output_dir.joinpath('air_quality_kaggle')
    kaggle_temp_dir = air_quality_kaggle_dir.joinpath('temp')
    stations_metadata_file = air_quality_kaggle_dir.joinpath(files_to_download_from_kaggle[0])
    example_pm10_file = air_quality_kaggle_dir.joinpath(files_to_download_from_kaggle[3])
    stations_lat_lon_file = air_quality_kaggle_dir.joinpath('stations_lat_long.csv')

    openmeteo_dir = base_output_dir.joinpath('openmeteo')

    # Hadoop config
    hadoop_base_target = '/user/hadoop'
    container_name = "master"
    staging_dir_in_container = "/tmp/staging_data"

    # 2. Download files from Kaggle

    download_files_from_kaggle(kaggle_dataset_url, files_to_download_from_kaggle, kaggle_temp_dir,
                                air_quality_kaggle_dir, logger)

    # 3. Extract lat lon for relevant stations
    print_info('Extracting lat and lon of stations')
    extract_lat_lon( \
        example_pm10_file, \
        stations_metadata_file, \
        stations_lat_lon_file \
        )
    print_success('Extracted!\n')

    # 4. Accumulate Open Meteo API data for historical weather
    print_info('Accumulating Open Meteo API readings')
    fail_delay = 10
    for year in years:
        all_downloaded = False
        while all_downloaded == False:
            all_downloaded = download_open_meteo_yearly_measurements( \
                stations_lat_lon_file, \
                openmeteo_dir, \
                year, \
                280  # max calls per run
            )
            if not all_downloaded:
                print_error(f'Not all files downloaded! Waiting {fail_delay} seconds and retrying')
                time.sleep(fail_delay)

    print_success("Open Meteo files downloaded!\n")

    # 5. Upload all the data into hadoop
    
    # 5.1 Upload openmeteo
    openmeteo_hdfs_target_dir = f"{hadoop_base_target}/openmeteo/{year}"
    for year in years:
        upload_yearly_openmeteo_data(year, openmeteo_dir, openmeteo_hdfs_target_dir, container_name, staging_dir_in_container)
        
    for year in years:
        set_replication_factor(container_name, openmeteo_hdfs_target_dir)

    # 7. Setup dynamic API data in hadoop
    pass


if __name__ == '__main__':
    main()
