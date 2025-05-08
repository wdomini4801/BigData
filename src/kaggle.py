import os
from kaggle.api.kaggle_api_extended import KaggleApi
import time
import zipfile
import shutil

s1_source = 'wisekinder/poland-air-quality-monitoring-dataset-2017-2023'

download_dir = os.path.expanduser('data')
output_dir_s1 = os.path.expanduser('source1')


def download_files_from_kaggle(source_url, files, temp_dir, dest_dir, logger):
    zip_file_name = None
    zip_file_path = None
    extracted_files_info = []

    logger.info(f"Start downloading data from Source 1: https://www.kaggle.com/datasets/{source_url}")

    try:
        api = KaggleApi()
        api.authenticate()
        logger.info("Authentication in Kaggle API successful.")

        try:
            if not os.path.isdir(temp_dir):
                os.makedirs(temp_dir)
                logger.info(f"Output directory: '{temp_dir}' created.")
            else:
                logger.info(f"Output directory: '{temp_dir}' exists.")
        except OSError as e:
            logger.error(f"Cannot make output directory '{temp_dir}': {e}")

        logger.info(f"Started downloading files to: '{temp_dir}'.")
        start_time = time.time()
        file_size = -1

        try:
            api.dataset_download_files(source_url, path=temp_dir, quiet=True)
            end_time = time.time()
            elapsed_time = end_time - start_time
            zip_file_name = os.listdir(temp_dir)[0]
            zip_file_path = os.path.join(temp_dir, zip_file_name)
            file_size = os.path.getsize(zip_file_path) / (1024 ** 2)

            logger.info("Data downloaded.")
            logger.info("Status: SUCCESS")
            logger.info(f"Download time: {elapsed_time:.2f} s")
            logger.info(f"Size: {file_size:.2f} MB")
            logger.info(f"Saved as: {zip_file_name}")

        except Exception as e:
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.error("Error while downloading.")
            logger.error("Status: ERROR")
            logger.error(f"Time till error: {elapsed_time:.2f} s")
            logger.error(f"Info: {e}")

        logger.info(f"Started extracting chosen files from: '{zip_file_name}' "
                    f"to: '{dest_dir}'.")
        start_time = time.time()
        extraction_successful_count = 0
        extraction_failed_count = 0

        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                archive_file_list = zip_ref.namelist()

                for file in files:
                    if file in archive_file_list:
                        try:
                            member_info = zip_ref.getinfo(file)
                            target_path = os.path.join(dest_dir, file)

                            os.makedirs(os.path.dirname(target_path), exist_ok=True)

                            with zip_ref.open(member_info) as source, open(target_path, "wb") as target:
                                shutil.copyfileobj(source, target)

                            logger.info(f" - Extracted and saved: '{file}'")
                            file_size = os.path.getsize(target_path)
                            extracted_files_info.append(
                                {"name": file, "path": target_path, "size": file_size})
                            extraction_successful_count += 1

                        except Exception as extract_err:
                            logger.error(f" - Error while extracting file: '{file}': {extract_err}")
                            extraction_failed_count += 1
                    else:
                        logger.warning(
                            f" - File: '{file}' does not exist in: '{zip_file_path}' - skipping.")
                        extraction_failed_count += 1

            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.info(f"Extraction finished in {elapsed_time:.2f} s.")
            final_status = "SUCCESS" if extraction_failed_count == 0 else "FAILURE"
            logger.info(f"Final status of extraction: {final_status}")
            logger.info(f"Successfully extracted {extraction_successful_count} files.")
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
            total_size_mb = total_size_bytes / (1024 ** 2)

            logger.info(f"Total size of extracted files: {total_size_mb:.2f} MB")
            logger.info(f"Files saved in: '{dest_dir}':")
            for item in extracted_files_info:
                logger.info(f"  - Path: {item['path']}, Size: {item['size'] / (1024 ** 2):.2f} MB")
        else:
            logger.error("Final status of extraction: ERROR")
            logger.error("Cannot extract and save chosen files.")

    finally:
        if zip_file_path and os.path.exists(zip_file_path):
            try:
                logger.info("Cleaning up.")
                os.remove(zip_file_path)
                if not os.listdir(temp_dir):
                    os.rmdir(temp_dir)
            except Exception as cleanup_err:
                logger.warning("Cleaning up failed. No permission for deleting files and directories.")

        logger.info("Downloading data from Kaggle finished successfully.")
