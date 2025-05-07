# ===================================================
#     This script runs on remote hadoop machine     #
# ===================================================
from src.openmeteo import *
from pathlib import Path

from src.utils import *

def main():
  # 0. (Optional) Parse arguments
  base_output_dir = Path('./data')
  
  air_quality_kaggle_dir = base_output_dir.joinpath('air_quality_kaggle')
  stations_metadata_file = air_quality_kaggle_dir.joinpath('stations_metadata.csv')
  example_pm10_file = air_quality_kaggle_dir.joinpath('2017_PM10_1g.csv') # TODO to be changed to the large file
  
  stations_lat_lon_file = air_quality_kaggle_dir.joinpath('stations_lat_long.csv')
  openmeteo_dir = base_output_dir.joinpath('openmeteo')
  
  years = [2021]
  
  # 1. Download stations_metadata.csv

  # 2. Download air pollution readings

  # 3. Extract lat lon for relevant stations
  print_info('Extracting lat and lon of stations')
  extract_lat_lon(\
    example_pm10_file, \
    stations_metadata_file, \
    stations_lat_lon_file  \
  )
  print_success('Extracted!\n')

  # 4. Accumulate Open Meteo API data for historical weather
  print_info('Accumulating Open Meteo API readings')
  fail_delay = 10
  for year in years:
    all_downloaded = False
    while all_downloaded == False:
      all_downloaded = download_open_meteo_yearly_measurements(\
        stations_lat_lon_file, \
        openmeteo_dir, \
        year, \
        130 # max calls per run
      )
      if not all_downloaded:
        print_error(f'Not all files downloaded! Waiting {fail_delay} seconds and retrying')
        time.sleep(fail_delay)
        
  print_success("Open Meteo files downloaded!\n")

  # 5. Upload all the data into hadoop

  # 6. Setup dynamic API data in hadoop
  pass

if __name__ == '__main__':
  main()
