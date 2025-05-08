# ===================================================
#     This script runs on remote hadoop machine     #
# ===================================================
from src.openmeteo import *
from src.kaggle import *
from pathlib import Path
import logging

from src.utils import *


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

    # 1. Parse arguments
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

    years = [2021]

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
                130  # max calls per run
            )
            if not all_downloaded:
                print_error(f'Not all files downloaded! Waiting {fail_delay} seconds and retrying')
                time.sleep(fail_delay)

    print_success("Open Meteo files downloaded!\n")

    # 6. Upload all the data into hadoop

    # 7. Setup dynamic API data in hadoop
    pass


if __name__ == '__main__':
    main()
