import os
import logging
import time
from kaggle.api.kaggle_api_extended import KaggleApi

log_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_file = 'download.log'
file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger('Logger')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info("Start downloading data from Kaggle.")

try:
    api = KaggleApi()
    api.authenticate()
    logger.info("Authentication in Kaggle API successful.")

    dataset = 'wisekinder/poland-air-quality-monitoring-dataset-2017-2023'
    file_name = 'stations_metadata.csv'
    output_dir = os.path.expanduser('data')

    try:
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Output directory: '{output_dir}' created.")
        else:
            logger.info(f"Output directory: '{output_dir}' exists.")
    except OSError as e:
        logger.error(f"Cannot make output directory '{output_dir}': {e}")

    target_file_path = os.path.join(output_dir, file_name)

    logger.info(f"Started downloading file: '{file_name}' to: '{output_dir}'.")
    start_time = time.time()
    download_success = False
    file_size = -1

    try:
        api.dataset_download_file(dataset, file_name=file_name, path=output_dir)

        if os.path.exists(target_file_path):
            end_time = time.time()
            elapsed_time = end_time - start_time
            file_size = round(os.path.getsize(target_file_path) / 1024, ndigits=2)
            download_success = True
            logger.info(f"File '{file_name}' downloaded.")
            logger.info(f"Status: SUCCESS")
            logger.info(f"Download time: {elapsed_time:.2f} s")
            logger.info(f"File size: {file_size} kB")
            logger.info(f"Saved as: {target_file_path}")
        else:
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.error(f"File '{file_name}' not found in: '{target_file_path}'.")
            logger.error(f"Status: ERROR (file do not exists)")
            logger.error(f"Time: {elapsed_time:.2f} s")

    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.error(f"Error while downloading: '{file_name}'.")
        logger.error(f"Status: ERROR")
        logger.error(f"Time till error: {elapsed_time:.2f} s")
        logger.error(f"Info: {e}")

except Exception as e:
    logger.critical(f"Error while authentication: {e}")

finally:
    logger.info("Process finished.")

logger.removeHandler(file_handler)
logger.removeHandler(console_handler)
file_handler.close()
