import subprocess
import os
import sys
import sqlite3
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


def init_database():

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ip_stats (
            router TEXT NOT NULL,
            granularity TEXT NOT NULL CHECK (granularity IN ('5m','30m','1h','1d')),
            bucket_start INTEGER NOT NULL,
            bucket_end   INTEGER NOT NULL,
            sa_ipv4_count INTEGER NOT NULL DEFAULT 0,
            da_ipv4_count INTEGER NOT NULL DEFAULT 0,
            sa_ipv6_count INTEGER NOT NULL DEFAULT 0,
            da_ipv6_count INTEGER NOT NULL DEFAULT 0,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (router, granularity, bucket_start)
        ) WITHOUT ROWID;
    """)
    conn.commit()
    conn.close()

def process_file(file_path):
    print(f"Processing {file_path}")
    sa_v4_res = set()
    da_v4_res = set()
    sa_v6_res = set()
    da_v6_res = set()
    
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv4"]
    ipv6_command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv6", "-6"]
    ipv4_result = subprocess.run(command, capture_output=True, text=True, timeout=300)
    ipv6_result = subprocess.run(ipv6_command, capture_output=True, text=True, timeout=300)

    if ipv4_result.returncode == 0:
        ipv4_out = ipv4_result.stdout.strip().split("\n")
        for line in ipv4_out:
            if line.strip() and ',' in line:
                try:
                    source_ip, destination_ip = line.strip().split(",")
                    sa_v4_res.add(source_ip)
                    da_v4_res.add(destination_ip)
                except ValueError:
                    continue
    if ipv6_result.returncode == 0:
        ipv6_out = ipv6_result.stdout.strip().split("\n")
        for line in ipv6_out:
            if line.strip() and ',' in line:
                try:
                    source_ip, destination_ip = line.strip().split(",")
                    sa_v6_res.add(source_ip)
                    da_v6_res.add(destination_ip)
                except ValueError:
                    continue

    return sa_v4_res, da_v4_res, sa_v6_res, da_v6_res


class Result:
    def __init__(self, router, granularity):
        self.router = router
        self.granularity = granularity
        self.sa_v4_res = set()
        self.da_v4_res = set()
        self.sa_v6_res = set()
        self.da_v6_res = set()


    def update_result(self, sa_v4_res, da_v4_res, sa_v6_res, da_v6_res):
        print(f"Updating result for {self.router} {self.granularity}")
        self.sa_v4_res.update(sa_v4_res)
        self.da_v4_res.update(da_v4_res)
        self.sa_v6_res.update(sa_v6_res)
        self.da_v6_res.update(da_v6_res)


    def write_result(self, bucket_start, bucket_end, conn):
        print(f"Writing result for {self.router} {self.granularity}")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ip_stats (router, granularity, bucket_start, bucket_end, sa_ipv4_count, da_ipv4_count, sa_ipv6_count, da_ipv6_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.router, self.granularity, bucket_start.timestamp(), bucket_end.timestamp(), len(self.sa_v4_res), len(self.da_v4_res), len(self.sa_v6_res), len(self.da_v6_res)))
        self.sa_v4_res = set()
        self.da_v4_res = set()
        self.sa_v6_res = set()
        self.da_v6_res = set()
        conn.commit()




def process_day(start_day):
    buckets = {
        "5m": 0,
        "30m": 1,
        "1h": 2,
        "1d": 3
    }
    conn = sqlite3.connect(DATABASE_PATH)
    print(f"Processing {start_day}")
    for router in AVAILABLE_ROUTERS:
        current_time = start_day
        results = [Result(router, "5m"), Result(router, "30m"), Result(router, "1h"), Result(router, "1d")]
        mins = 0

        end_time = start_day + timedelta(days=1)
        while current_time < end_time:
            timestamp_str = current_time.strftime('%Y%m%d%H%M')
            file_path = f"{NETFLOW_DATA_PATH}/{router}/{timestamp_str[:4]}/{timestamp_str[4:6]}/{timestamp_str[6:8]}/nfcapd.{timestamp_str}"
            if os.path.exists(file_path):
                sa_v4_res, da_v4_res, sa_v6_res, da_v6_res = process_file(file_path) 

                results[buckets["5m"]].update_result(sa_v4_res, da_v4_res, sa_v6_res, da_v6_res)
                results[buckets["30m"]].update_result(sa_v4_res, da_v4_res, sa_v6_res, da_v6_res)
                results[buckets["1h"]].update_result(sa_v4_res, da_v4_res, sa_v6_res, da_v6_res)
                results[buckets["1d"]].update_result(sa_v4_res, da_v4_res, sa_v6_res, da_v6_res)


                results[buckets["5m"]].write_result(current_time, current_time + timedelta(minutes=5), conn)
                if mins != 0 and mins % 30 == 0:
                    results[buckets["30m"]].write_result(current_time, current_time + timedelta(minutes=30), conn)
                if mins != 0 and mins % 60 == 0:
                    results[buckets["1h"]].write_result(current_time, current_time + timedelta(hours=1), conn)
            mins += 5
            current_time += timedelta(minutes=5)
        results[buckets["5m"]].write_result(current_time, current_time + timedelta(minutes=5), conn)
        results[buckets["30m"]].write_result(current_time, current_time + timedelta(minutes=30), conn)
        results[buckets["1h"]].write_result(current_time, current_time + timedelta(hours=1), conn)
        results[buckets["1d"]].write_result(current_time, current_time + timedelta(days=1), conn)
       
    return 0

def main():

    init_database()

    start_time = datetime(2025, 1, 1)

    timer = datetime.now()
    current_time = start_time
    delta = timedelta(days=1)
    tasks = []
    now = datetime.now()
    while current_time < now:
        tasks.append(current_time)
        current_time += delta
    print(f"Found {len(tasks)} tasks")
    with Pool(processes=MAX_WORKERS) as pool:
        pool.map(process_day, tasks)

    print(f"Time taken: {datetime.now() - timer}")
if __name__ == "__main__":
    main()

