import subprocess

RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
RESET = "\033[0m"

def green(text):
  return f"\033[92m{text}\033[0m"

def print_error(message):
  print(f"{RED}[ERROR] {message}{RESET}")

def print_info(message):
  print(f"{YELLOW}[INFO] {message}{RESET}")

def print_success(message):
  print(f"{GREEN}[SUCCESS] {message}{RESET}")
  
def run_local_command(command, logger):
  try:
    result = subprocess.run(command, shell=True, check=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info(result.stdout.decode())
  except subprocess.CalledProcessError as e:
    logger.Error(f"Command failed: {e.cmd}")
    logger.info(e.stderr.decode())
