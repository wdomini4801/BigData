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
