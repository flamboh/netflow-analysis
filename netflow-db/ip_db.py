from HLL import HyperLogLog
import pickle
import subprocess
import os
import sys
import sqlite3
from pathlib import Path
import datetime as dt
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


def process_file(conn, file_path, router, timestamp_unix):
  source_ipv4_hll = HyperLogLog()
  source_ipv6_hll = HyperLogLog()
  destination_ipv4_hll = HyperLogLog()
  destination_ipv6_hll = HyperLogLog()
  total_ips = 0
  if os.path.exists(file_path):
    ipv4_command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv4"]
    ipv6_command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv6", "-6"]
    ipv4_result = subprocess.run(ipv4_command, capture_output=True, text=True)
    ipv6_result = subprocess.run(ipv6_command, capture_output=True, text=True)

    ipv4_out = ipv4_result.stdout.split("\n")
    ipv6_out = ipv6_result.stdout.split("\n")
    for line in ipv4_out:
      source_ip, destination_ip = line.strip().split(",")
      source_ipv4_hll.add(source_ip)
      destination_ipv4_hll.add(destination_ip)
      total_ips += 2
    for line in ipv6_out:
      source_ip, destination_ip = line.strip().split(",")
      source_ipv6_hll.add(source_ip)
      destination_ipv6_hll.add(destination_ip)
      total_ips += 2
    conn.execute("""
      INSERT OR REPLACE INTO ip_stats (
        file_path, router, timestamp,
        source_ipv4_hll, destination_ipv4_hll, source_ipv6_hll, destination_ipv6_hll, source_ipv4_cardinality, destination_ipv4_cardinality, source_ipv6_cardinality, destination_ipv6_cardinality, total_ips
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (file_path, router, timestamp_unix, pickle.dumps(source_ipv4_hll), pickle.dumps(destination_ipv4_hll), pickle.dumps(source_ipv6_hll), pickle.dumps(destination_ipv6_hll), source_ipv4_hll.cardinality(), destination_ipv4_hll.cardinality(), source_ipv6_hll.cardinality(), destination_ipv6_hll.cardinality(), total_ips))
    return total_ips
  return -1

def main():
  start_time = dt.datetime.now()
  conn = sqlite3.connect(DATABASE_PATH)
  cursor = conn.cursor()

  first_file = dt.datetime(2025, 7, 1, 0, 0)
  last_file = dt.datetime(2025, 7, 1, 23, 59)

  total = 0
  all_files = [time for time in range(first_file, last_file, dt.timedelta(minutes=5))]
  with Pool(processes=10) as pool:
    results = pool.map(process_file, all_files)
  for result in results:
    total += result[1]
    

  print(hll.cardinality())
  print(total)
  print(dt.datetime.now() - start_time)
  print(str(round(hll.cardinality() / total * 100, 2)) + "%")

if __name__ == "__main__":
  main()


#   file = "/research/tango_cis/uonet-in/oh-ir1-gw/2025/07/01/nfcapd.202507010000"
#   command = ["nfdump", "-r", file, "-q", "-o", "fmt:%sa", "-n", "0"]
  
#   result = subprocess.run(command, capture_output=True, text=True)
#   total = 0
#   for line in result.stdout.split("\n"):
#     ip = line.strip()
#     hll.add(ip)
#     total += 1
  
#   print(hll.cardinality())
#   print(total)
#   print(str(round(hll.cardinality() / total * 100, 2)) + "%")
# if __name__ == "__main__":
#   main()