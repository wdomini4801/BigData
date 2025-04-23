import os
import logging
import time
import paramiko
import shutil
import zipfile
from pathlib import Path
from scp import SCPClient
from posixpath import join as posix_join

# Configuration
hadoop_config_path = './config/hadoop_config.json'

# Read hadoop_config.json
import json
with open(hadoop_config_path, 'r') as config_file:
    hadoop_config = json.load(config_file)

ssh_host = hadoop_config["ssh_host"]
ssh_port = hadoop_config["ssh_port"]
ssh_user = hadoop_config["ssh_user"]
container_name = "master"
remote_path = f"/home/{ssh_user}/uploads"
staging_dir_in_container = "/tmp/staging_data"

private_key_path = hadoop_config["private_key_path"]
local_source_dir = "./output/2017"  # Directory with the files you want to upload
hdfs_target_dir = "/user/hadoop/openmeteo/2017"  # HDFS target directory
zip_filename = "openmeteo_data_2017.zip"  # Name for the zip file

# Setup logging
log_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_file = 'hadoop_upload.log'
file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger('Logger')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# SSH Client Setup
def create_ssh_client(host, port, username, key_filepath):
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(host, port=port, username=username, key_filename=key_filepath)
        return ssh_client
    except Exception as e:
        logger.error(f"Failed to connect to {host}:{port}. Error: {e}")
        raise

# Execute SSH Command
def execute_ssh_command(ssh_client, command):
    try:
        # Run the command on the remote server
        stdin, stdout, stderr = ssh_client.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()  # Wait for command to finish

        if exit_status == 0:
            logger.info(f"Command succeeded: {command}")
            return stdout.read().decode()  # Return the output
        else:
            error_message = stderr.read().decode()
            logger.error(f"Command failed with exit status {exit_status}: {error_message}")
            raise Exception(f"Command failed: {command}")
    
    except Exception as e:
        logger.error(f"Error executing command {command}: {e}")
        raise

# Step 1: Zip the files in the local source directory
def zip_files(local_source_dir, zip_filename):
    zip_file_path = os.path.join(local_source_dir, zip_filename)
    logger.info(f"Zipping files in {local_source_dir} to {zip_file_path}")
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(local_source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, local_source_dir))
    logger.info(f"Files zipped successfully: {zip_file_path}")
    return zip_file_path

def copy_zip_to_remote(ssh_client, local_zip_path, remote_path="/tmp"):
    
    remote_zip_path = posix_join(remote_path, os.path.basename(local_zip_path))
    logger.info(f"Copying {local_zip_path} to remote: {remote_zip_path}")
    try:
        with SCPClient(ssh_client.get_transport()) as scp:
            scp.put(local_zip_path, remote_zip_path)
        logger.info("Zip file copied successfully.")
        return remote_zip_path
    except Exception as e:
        logger.error(f"Failed to copy zip file: {e}")
        raise

def unzip_remote_into_container(ssh_client, remote_zip_path, container_name, staging_dir_in_container):
    unzip_command = f"""
    unzip -o {remote_zip_path} -d /tmp/unzipped_data && \
    sudo docker cp /tmp/unzipped_data/. {container_name}:{staging_dir_in_container}/
    """
    logger.info(f"Unzipping {remote_zip_path} and copying to Docker container {container_name}")
    execute_ssh_command(ssh_client, unzip_command)

# Step 2: Create the target directory in HDFS
def create_hdfs_directory(ssh_client, container_name, hdfs_target_dir):
    # Command to create target directory in HDFS using Docker
    create_dir_command = f"sudo docker exec {container_name} hdfs dfs -mkdir -p {hdfs_target_dir}"
    logger.info(f"Creating target directory in HDFS: {hdfs_target_dir}")
    
    # Execute SSH command to run the Docker command
    execute_ssh_command(ssh_client, create_dir_command)

# Step 3: Copy the zip file to the remote server
def copy_files_to_docker(ssh_client, local_source_dir, container_name, staging_dir_in_container):
    # Command to copy files from local machine to Docker container
    copy_files_command = f"sudo docker cp {local_source_dir}/. {container_name}:{staging_dir_in_container}/"
    logger.info(f"Copying files from {local_source_dir} to Docker container at {staging_dir_in_container}")
    
    # Execute SSH command to run the Docker copy command
    execute_ssh_command(ssh_client, copy_files_command)

# Step 4: Upload the zip file to HDFS
def upload_to_hdfs(ssh_client, container_name, staging_dir_in_container, hdfs_target_dir):
    # Command to upload files from Docker container to HDFS
    upload_command = f'sudo docker exec {container_name} sh -c "hdfs dfs -put -f {staging_dir_in_container}/* {hdfs_target_dir}/"'
    
    logger.info(f"Uploading files from {staging_dir_in_container} to HDFS at {hdfs_target_dir}")
    
    # Execute SSH command to run the Docker HDFS upload command
    execute_ssh_command(ssh_client, upload_command)

# Main process
try:
    ssh_client = create_ssh_client(ssh_host, ssh_port, ssh_user, private_key_path)

    # Create a safe directory on the remote server for uploading
    remote_upload_dir = f"/home/{ssh_user}/uploads"
    execute_ssh_command(ssh_client, f"mkdir -p {remote_upload_dir}")

    # Step 1: Zip the files
    zip_file_path = zip_files(local_source_dir, zip_filename)

    # Step 2: Copy the zip file to the remote server
    remote_zip_path = copy_zip_to_remote(ssh_client, zip_file_path, remote_upload_dir)

    # Step 3: Unzip the file on the remote server
    execute_ssh_command(ssh_client, f"unzip -o {remote_zip_path} -d {remote_upload_dir}/unzipped")

    # Step 4: Create target directory in HDFS
    create_hdfs_directory(ssh_client, container_name, hdfs_target_dir)

    # Step 5: Copy the unzipped files into the Docker container
    copy_files_to_docker(ssh_client, f"{remote_upload_dir}/unzipped", container_name, staging_dir_in_container)

    # Step 6: Upload to HDFS
    upload_to_hdfs(ssh_client, container_name, staging_dir_in_container, hdfs_target_dir)
    
except Exception as e:
    logger.error(f"An error occurred: {e}")

finally:
    # Close the SSH client
    if 'ssh_client' in locals():
        ssh_client.close()
