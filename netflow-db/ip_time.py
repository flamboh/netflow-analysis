import subprocess
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from multiprocessing import Pool

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
AVAILABLE_ROUTERS = get_required_env('AVAILABLE_ROUTERS').split(',')
DATABASE_PATH = get_required_env('DATABASE_PATH')
FIRST_RUN = os.environ.get('FIRST_RUN', 'False').lower() in ('true', '1', 'yes')
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '8'))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '50'))


def process_file(file_path, delta, start_time):
    print(f"Processing {file_path} at {delta} interval")
    file_paths = []
    current_time = start_time
    end_time = start_time + delta
    while current_time < end_time:
        timestamp_str = current_time.strftime('%Y%m%d%H%M')
        file_path = f"{NETFLOW_DATA_PATH}/cc-ir1-gw/2025/01/01/nfcapd.{timestamp_str}"
        if os.path.exists(file_path):
            file_paths.append(file_path)
        current_time += delta
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv4"]
    ipv6_command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv6", "-6"]
    ipv4_result = subprocess.run(command, capture_output=True, text=True, timeout=300)
    ipv6_result = subprocess.run(ipv6_command, capture_output=True, text=True, timeout=300)
    sa_v4_set = set()
    sa_v6_set = set()
    da_v4_set = set()
    da_v6_set = set()
    if ipv4_result.returncode == 0:
        ipv4_out = ipv4_result.stdout.strip().split("\n")
        for line in ipv4_out:
            if line.strip() and ',' in line:
                try:
                    source_ip, destination_ip = line.strip().split(",")
                    sa_v4_set.add(source_ip)
                    da_v4_set.add(destination_ip)
                except ValueError:
                    continue
    else:
        print(f"Error processing {file_path}: {ipv4_result.stderr}")
        return None
    if ipv6_result.returncode == 0:
        ipv6_out = ipv6_result.stdout.strip().split("\n")
        for line in ipv6_out:
            if line.strip() and ',' in line:
                try:
                    source_ip, destination_ip = line.strip().split(",")
                    sa_v6_set.add(source_ip)
                    da_v6_set.add(destination_ip)
                except ValueError:
                    continue
    else:
        print(f"Error processing {file_path}: {ipv6_result.stderr}")
        return None
    return len(sa_v4_set), len(da_v4_set), len(sa_v6_set), len(da_v6_set)

def main():

    start_time = datetime(2025, 1, 1)

    deltas = [timedelta(minutes=5), timedelta(minutes=30), timedelta(hours=1), timedelta(days=1)]
    times = []

    for delta in deltas:
      timer = datetime.now()
      current_time = start_time
      print(f"processing 1 day at {delta} interval")
      tasks = []
      while current_time < datetime(2025, 1, 2):
          timestamp_str = current_time.strftime('%Y%m%d%H%M')
          file_path = f"{NETFLOW_DATA_PATH}/cc-ir1-gw/2025/01/01/nfcapd.{timestamp_str}"
          if os.path.exists(file_path):
              tasks.append((file_path, delta))
          current_time += delta
      print(f"Found {len(tasks)} tasks")
      with Pool(processes=MAX_WORKERS) as pool:
          results = pool.map(process_file, tasks)
      for result in results:
          print(result)

      times.append(datetime.now() - timer)
      

    for i in range(len(deltas)):
      print(f"{deltas[i]} time taken: {times[i]}")

if __name__ == "__main__":
    main()