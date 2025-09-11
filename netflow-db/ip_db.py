from HLL import HyperLogLog
import subprocess
import os
import sys
from pathlib import Path

# Load environment variables from .env file
def load_env_file(env_path='../.env'):
    """
    Load environment variables from a dotenv-style file into os.environ.
    
    Reads the file at env_path (default '../.env'), ignoring empty lines and lines
    starting with '#'. Each non-comment line containing '=' is split on the first
    '=' and the left/right parts are stripped and set as KEY=VALUE in os.environ.
    
    If the file does not exist, prints an error message and exits the process with
    status code 1.
    """
    env_file = Path(env_path)
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    else:
        print(f"ERROR: Environment file '{env_path}' not found!")
        print("Please copy .env.example to .env and configure your settings.")
        sys.exit(1)

# Load environment variables
load_env_file()

# Get required environment variables with error handling
def get_required_env(key):
    """Get required environment variable or exit with error"""
    value = os.environ.get(key)
    if not value:
        print(f"ERROR: Required environment variable '{key}' is not set!")
        print("Please check your .env file configuration.")
        sys.exit(1)
    return value

# Configuration from environment variables
NETFLOW_DATA_PATH = get_required_env('NETFLOW_DATA_PATH')


def main():
  hll = HyperLogLog()

  file = "/research/tango_cis/uonet-in/oh-ir1-gw/2025/07/01/nfcapd.202507010000"
  command = ["nfdump", "-r", file, "-q", "-o", "fmt:%sa", "-n", "0"]
  
  result = subprocess.run(command, capture_output=True, text=True)
  total = 0
  for line in result.stdout.split("\n"):
    ip = line.strip()
    hll.add(ip)
    total += 1
  
  print(hll.cardinality())
  print(total)
  print(str(round(hll.cardinality() / total * 100, 2)) + "%")

if __name__ == "__main__":
  main()