
def create_hdfs_directory_command(container_name, hdfs_target_dir):
  # Command to create target directory in HDFS using Docker
  create_dir_command = f"sudo docker exec {container_name} hdfs dfs -mkdir -p {hdfs_target_dir}"
  
  return create_dir_command

def copy_files_to_docker_command(container_name, local_source_dir, staging_dir_in_container):
  # Command to copy files from local machine to Docker container
  copy_files_command = f"sudo docker cp {local_source_dir}/. {container_name}:{staging_dir_in_container}/"

  return copy_files_command

def upload_to_hdfs_command(container_name, hdfs_target_dir, staging_dir_in_container):
  # Command to upload files from Docker container to HDFS
  upload_command = f'sudo docker exec {container_name} sh -c "hdfs dfs -put -f {staging_dir_in_container}/* {hdfs_target_dir}/"'
  
  return upload_command
  
def set_replication_factor_command(container_name, hdfs_target_dir, replication_factor=3):
  setrep_command = (
    f'sudo docker exec {container_name} sh -c '
    f'"hdfs dfs -setrep -R -w {replication_factor} {hdfs_target_dir}"'
  )
  
  return setrep_command


