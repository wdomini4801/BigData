import subprocess

RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
RESET = "\033[0m"

def print_error(message):
  print(f"{RED}[ERROR] {message}{RESET}")

def print_info(message):
  print(f"{YELLOW}[INFO] {message}{RESET}")

def print_success(message):
  print(f"{GREEN}[SUCCESS] {message}{RESET}")
  
def run_local_command(command):
  try:
    result = subprocess.run(command, shell=True, check=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(result.stdout.decode())
  except subprocess.CalledProcessError as e:
    print_error(f"[ERROR] Command failed: {e.cmd}")
    print(e.stderr.decode())
