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

logger.info("Start downloading data from Source 1.")

try:
    api = KaggleApi()
    api.authenticate()
    logger.info("Authentication in Kaggle API successful.")

    dataset = 'wisekinder/poland-air-quality-monitoring-dataset-2017-2023'
    output_dir = os.path.expanduser('data')

    try:
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Output directory: '{output_dir}' created.")
        else:
            logger.info(f"Output directory: '{output_dir}' exists.")
    except OSError as e:
        logger.error(f"Cannot make output directory '{output_dir}': {e}")

    logger.info(f"Started downloading files to: '{output_dir}'.")
    start_time = time.time()
    file_size = -1

    try:
        api.dataset_download_files(dataset, path=output_dir, quiet=True)
        end_time = time.time()
        elapsed_time = end_time - start_time
        final_path = os.path.join(output_dir, os.listdir(output_dir)[0])
        file_size = round(os.path.getsize(final_path) / 1024, ndigits=2)

        logger.info("Data downloaded.")
        logger.info("Status: SUCCESS")
        logger.info(f"Download time: {elapsed_time:.2f} s")
        logger.info(f"Size: {file_size} kB")
        logger.info(f"Saved as: {final_path}")

    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.error("Error while downloading.")
        logger.error("Status: ERROR")
        logger.error(f"Time till error: {elapsed_time:.2f} s")
        logger.error(f"Info: {e}")

except Exception as e:
    logger.critical(f"Error while authentication: {e}")

finally:
    logger.info("Process finished.")

logger.removeHandler(file_handler)
logger.removeHandler(console_handler)
file_handler.close()
