import json
import os
import logging
import tempfile
import time
import paramiko
import shutil
import zipfile
from pathlib import Path
from scp import SCPClient
from posixpath import join as posix_join
import argparse
# ===================================================
#              This script runs locally             #
# ===================================================

# Configuration
hadoop_config_path = './config/hadoop_config.json'
with open(hadoop_config_path, 'r') as config_file:
  hadoop_config = json.load(config_file)

ssh_host = hadoop_config["ssh_host"]
ssh_port = hadoop_config["ssh_port"]
ssh_user = hadoop_config["ssh_user"]
private_key_path = hadoop_config["private_key_path"]

container_name = "master"
remote_path = f"/home/{ssh_user}/project"
staging_dir_in_container = "/tmp/staging_data"

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

def create_ssh_client(host, port, username, key_filepath):
  try:
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(host, port=port, username=username, key_filename=key_filepath)
    return ssh_client
  except Exception as e:
    logger.error(f"Failed to connect to {host}:{port}. Error: {e}")
    raise

import sys
import time

def execute_ssh_command(ssh_client, command, working_dir=None):
  try:
    # If a working directory is provided, prepend `cd {working_dir} &&` to the command
    if working_dir:
      command = f"cd {working_dir} && {command}"

    # Run the command on the remote server
    logger.info(f"Executing command: {command}")
    stdin, stdout, stderr = ssh_client.exec_command(command)

    # Continuously read from stdout and stderr without waiting for the end
    while True:
      # Check for new data from stdout
      output = stdout.channel.recv(1024).decode('utf-8')
      if output:
        sys.stdout.write(output)  # Print live output to local console
        sys.stdout.flush()

      # Check for new data from stderr (if any)
      error_output = stderr.channel.recv(1024).decode('utf-8')
      if error_output:
        sys.stderr.write(error_output)  # Print live errors to local console
        sys.stderr.flush()

      # Exit the loop once both stdout and stderr are done (process finished)
      if stdout.channel.exit_status_ready() and stderr.channel.exit_status_ready():
        break

      # Sleep for a short period to avoid high CPU usage
      time.sleep(0.1)

    # Capture exit status after the process finishes
    exit_status = stdout.channel.recv_exit_status()

    if exit_status == 0:
      logger.info(f"Command succeeded: {command}")
    else:
      logger.error(f"Command failed with exit status {exit_status}")

  except Exception as e:
    logger.error(f"Error executing command {command}: {e}")
    raise


def prepare_temp_upload_dir(local_root, files, folders):
  temp_dir = Path(tempfile.mkdtemp())

  # Copy individual files
  for file in files:
    rel_path = file.relative_to(local_root)
    target_path = temp_dir / rel_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(file, target_path)

  # Copy full folders (recursively), ignoring __pycache__
  for folder in folders:
    rel_path = folder.relative_to(local_root)
    target_path = temp_dir / rel_path
    shutil.copytree(folder, target_path, ignore=shutil.ignore_patterns('__pycache__'))

  return temp_dir

def delete_project_folder(ssh_client, remote_path):
  # Delete the project directory on the remote machine if it exists
  delete_command = f"sudo rm -rf {remote_path}"
  logger.info(f"Deleting existing project folder on remote: {remote_path}")
  execute_ssh_command(ssh_client, delete_command)

def install_python_venv_package(ssh_client):
  # Install python3-venv package to enable virtual environment creation
  install_venv_cmd = "sudo apt-get update && sudo apt-get install -y python3.11-venv"
  logger.info("Installing python3-venv package on remote...")
  execute_ssh_command(ssh_client, install_venv_cmd)

def main():
  local_project_root = Path('.').resolve()
  files_to_send = [
    local_project_root / 'main.py',
    local_project_root / 'requirements.txt',
    local_project_root / 'config' / 'kaggle.json',
    local_project_root / 'data' / 'air_quality_kaggle' / '2017_PM10_1g.csv',
    local_project_root / 'data' / 'air_quality_kaggle' / 'stations_metadata.csv'
  ]
  folders_to_send = [
    local_project_root / 'src'
  ]

  # Connect via SSH
  logger.info(f"Connecting to {ssh_host}...")
  ssh = create_ssh_client(ssh_host, ssh_port, ssh_user, private_key_path)
  scp = SCPClient(ssh.get_transport())

  # Ensure the remote project directory is deleted
  delete_project_folder(ssh, remote_path)

  # Ensure remote upload directory exists
  execute_ssh_command(ssh, f"mkdir -p {remote_path}")

  # Prepare clean upload dir preserving folder structure
  temp_upload_dir = prepare_temp_upload_dir(local_project_root, files_to_send, folders_to_send)

  # Upload files and folders
  for item in temp_upload_dir.iterdir():
    logger.info(f"Uploading structured files from: {item}")
    scp.put(str(item), remote_path, recursive=item.is_dir())

  # Ensure python3-venv is installed on the remote machine
  install_python_venv_package(ssh)

  # Create a virtual environment on the remote machine
  remote_venv_path = posix_join(remote_path, 'venv')
  create_venv_cmd = f"python3 -m venv {remote_venv_path}"
  logger.info("Creating virtual environment on remote machine...")
  execute_ssh_command(ssh, create_venv_cmd, working_dir=remote_path)

  # Install Python dependencies in the virtual environment
  remote_req_path = posix_join(remote_path, 'requirements.txt')
  install_cmd = f"{remote_venv_path}/bin/pip install -r {remote_req_path}"
  logger.info("Installing Python dependencies on remote in virtual environment...")
  execute_ssh_command(ssh, install_cmd, working_dir=remote_path)

  # # Run main.py on the remote machine using the virtual environment
  # remote_main_path = posix_join(remote_path, 'main.py')
  # run_cmd = f"{remote_venv_path}/bin/python3 {remote_main_path}"
  # logger.info("Executing main.py on remote using virtual environment...")
  # execute_ssh_command(ssh, run_cmd, working_dir=remote_path)

  # Close SCP/SSH
  scp.close()
  ssh.close()
  logger.info("Done.")

if __name__ == '__main__':
  main()