import os
import logging
import time
import zipfile
import shutil
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

s1_source = 'wisekinder/poland-air-quality-monitoring-dataset-2017-2023'
download_dir = os.path.expanduser('data')
output_dir_s1 = os.path.expanduser('source1')
files_to_extract_and_save = [
    'stations_metadata.csv',
    'joint_data_2017-2023/PM10_1g_joint_2017-2023.csv'
]
cleanup_zip = True

logger.info("Start downloading data from Source 1.")

zip_file_name = None
zip_file_path = None
extracted_files_info = []

try:
    api = KaggleApi()
    api.authenticate()
    logger.info("Authentication in Kaggle API successful.")

    try:
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)
            logger.info(f"Output directory: '{download_dir}' created.")
        else:
            logger.info(f"Output directory: '{download_dir}' exists.")
    except OSError as e:
        logger.error(f"Cannot make output directory '{download_dir}': {e}")

    logger.info(f"Started downloading files to: '{download_dir}'.")
    start_time = time.time()
    file_size = -1

    try:
        api.dataset_download_files(s1_source, path=download_dir, quiet=True)
        end_time = time.time()
        elapsed_time = end_time - start_time
        zip_file_name = os.listdir(download_dir)[0]
        zip_file_path = os.path.join(download_dir, zip_file_name)
        file_size = round(os.path.getsize(zip_file_path) / 1024, ndigits=2)

        logger.info("Data downloaded.")
        logger.info("Status: SUCCESS")
        logger.info(f"Download time: {elapsed_time:.2f} s")
        logger.info(f"Size: {file_size} kB")
        logger.info(f"Saved as: {zip_file_name}")

    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.error("Error while downloading.")
        logger.error("Status: ERROR")
        logger.error(f"Time till error: {elapsed_time:.2f} s")
        logger.error(f"Info: {e}")

    logger.info(f"Started extracting chosen files from: '{zip_file_name}' "
                f"to: '{output_dir_s1}'.")
    start_time = time.time()
    extraction_successful_count = 0
    extraction_failed_count = 0

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            archive_file_list = zip_ref.namelist()

            for file_to_extract in files_to_extract_and_save:
                if file_to_extract in archive_file_list:
                    try:
                        member_info = zip_ref.getinfo(file_to_extract)
                        target_path = os.path.join(output_dir_s1, file_to_extract)

                        os.makedirs(os.path.dirname(target_path), exist_ok=True)

                        with zip_ref.open(member_info) as source, open(target_path, "wb") as target:
                            shutil.copyfileobj(source, target)

                        logger.info(f" - Extracted and saved: '{target_path}'")
                        file_size = os.path.getsize(target_path)
                        extracted_files_info.append({"path": target_path, "size": file_size})
                        extraction_successful_count += 1

                    except Exception as extract_err:
                        logger.error(f" - Error while extracting file: '{file_to_extract}': {extract_err}")
                        extraction_failed_count += 1
                else:
                    logger.warning(
                        f" - File: '{file_to_extract}' does not exist in: '{zip_file_path}' - skipping.")
                    extraction_failed_count += 1

        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Extraction finished in {elapsed_time:.2f} s.")
        logger.info(f"Siccessfully extracted {extraction_successful_count} files.")
        if extraction_failed_count > 0:
            logger.warning(f"Failed to extract {extraction_failed_count} files.")

    except zipfile.BadZipFile:
        logger.error(f"File: '{zip_file_path}' is not a vaild ZIP file.")
        raise

    except Exception as e:
        logger.error(f"Unexpected error while extracting: {e}")
        raise

    if extracted_files_info:
        total_size_bytes = sum(item['size'] for item in extracted_files_info)
        total_size_kb = round(total_size_bytes / 1024, ndigits=2)
        final_status = "SUCCESS" if extraction_failed_count == 0 else "FAILURE"

        logger.info(f"Final status of extraction: {final_status}")
        logger.info(f"Total size of extracted files: {total_size_kb} kB")
        logger.info(f"Files saved in: '{output_dir_s1}'")
        logger.info("Details:")
        for item in extracted_files_info:
            logger.info(f"  - Path: {item['path']}, Size: {round(item['size'] / 1024, 2)} kB")
    else:
        logger.error("Final status of extraction: ERROR")
        logger.error("Cannot extract and save chosen files.")


except Exception as e:
    logger.critical(f"Critical error: {e}")

finally:
    if cleanup_zip and zip_file_path and os.path.exists(zip_file_path):
        try:
            logger.info(f"Deleting downloaded ZIP file: '{zip_file_name}'")
            os.remove(zip_file_path)
            logger.info("ZIP file deleted.")
            if not os.listdir(download_dir):
                logger.info(f"Deleting empty directory: '{download_dir}'")
                os.rmdir(download_dir)

        except Exception as cleanup_err:
            logger.warning(
                f"Failed to delete ZIP file: '{zip_file_path}' or directory: '{download_dir}': {cleanup_err}")
    logger.info("Process finished.")
    logger.removeHandler(file_handler)
    logger.removeHandler(console_handler)
    file_handler.close()
