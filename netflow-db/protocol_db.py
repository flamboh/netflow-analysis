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
# FIRST_RUN = os.environ.get('FIRST_RUN', 'False').lower() in ('true', '1', 'yes')
FIRST_RUN = True
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '8'))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '50'))
DATA_START_DATE = datetime(2025, 2, 1)

def init_database():

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS protocol_stats (
            router TEXT NOT NULL,
            granularity TEXT NOT NULL CHECK (granularity IN ('5m','30m','1h','1d')),
            bucket_start INTEGER NOT NULL,
            bucket_end   INTEGER NOT NULL,
            unique_protocols_count_ipv4 INTEGER NOT NULL DEFAULT 0,
            unique_protocols_count_ipv6 INTEGER NOT NULL DEFAULT 0,
            protocols_list_ipv4 TEXT NOT NULL DEFAULT '',
            protocols_list_ipv6 TEXT NOT NULL DEFAULT '',
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (router, granularity, bucket_start)
        ) WITHOUT ROWID;
    """)
    conn.commit()
    conn.close()

def process_file(file_path):
    print(f"Processing {file_path}")
    protocols_ipv4 = set()
    protocols_ipv6 = set()
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%pr", "-A", "proto"]
    ipv4_result = subprocess.run(command + ["ipv4"], capture_output=True, text=True, timeout=300)
    ipv6_result = subprocess.run(command + ["ipv6"], capture_output=True, text=True, timeout=300)

    if ipv4_result.returncode == 0:
        out = ipv4_result.stdout.strip().split("\n")
        for line in out:
            if line.strip():
                protocols_ipv4.add(line.strip())
    if ipv6_result.returncode == 0:
        out = ipv6_result.stdout.strip().split("\n")
        for line in out:
            if line.strip():
                protocols_ipv6.add(line.strip())
    return protocols_ipv4, protocols_ipv6

class Result:
    def __init__(self, router, granularity):
        self.router = router
        self.granularity = granularity
        self.protocols_ipv4 = set()
        self.protocols_ipv6 = set()


    def update_result(self, protocols_ipv4, protocols_ipv6):
        print(f"Updating result for {self.router} {self.granularity}")
        self.protocols_ipv4.update(protocols_ipv4)
        self.protocols_ipv6.update(protocols_ipv6)


    def write_result(self, bucket_start, bucket_end, conn):
        print(f"Writing result for {self.router} {self.granularity}")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO protocol_stats (router, granularity, bucket_start, bucket_end, unique_protocols_count_ipv4, unique_protocols_count_ipv6, protocols_list_ipv4, protocols_list_ipv6)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.router, self.granularity, int(bucket_start.timestamp()), int(bucket_end.timestamp()), len(self.protocols_ipv4), len(self.protocols_ipv6), ",".join(sorted(self.protocols_ipv4)), ",".join(sorted(self.protocols_ipv6))))
        self.protocols_ipv4 = set()
        self.protocols_ipv6 = set()
        conn.commit()




def process_day(task):
    start_day, day_end = task
    buckets = {
        "5m": 0,
        "30m": 1,
        "1h": 2,
        "1d": 3
    }
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=60000;")
        print(f"Processing window {start_day} -> {day_end}")
        for router in AVAILABLE_ROUTERS:
            current_time = start_day
            results = [Result(router, "5m"), Result(router, "30m"), Result(router, "1h"), Result(router, "1d")]
            mins = 0

            end_time = day_end
            while current_time < end_time:
                timestamp_str = current_time.strftime('%Y%m%d%H%M')
                file_path = f"{NETFLOW_DATA_PATH}/{router}/{timestamp_str[:4]}/{timestamp_str[4:6]}/{timestamp_str[6:8]}/nfcapd.{timestamp_str}"
                bucket_end = current_time + timedelta(minutes=5)
                if bucket_end > end_time:
                    bucket_end = end_time

                if os.path.exists(file_path):
                    protocols_ipv4, protocols_ipv6 = process_file(file_path) 

                    results[buckets["5m"]].update_result(protocols_ipv4, protocols_ipv6)
                    results[buckets["30m"]].update_result(protocols_ipv4, protocols_ipv6)
                    results[buckets["1h"]].update_result(protocols_ipv4, protocols_ipv6)
                    results[buckets["1d"]].update_result(protocols_ipv4, protocols_ipv6)


                    results[buckets["5m"]].write_result(current_time, bucket_end, conn)

                mins += 5
                current_time = bucket_end

                if mins % 30 == 0:
                    results[buckets["30m"]].write_result(current_time - timedelta(minutes=30), current_time, conn)
                if mins % 60 == 0:
                    results[buckets["1h"]].write_result(current_time - timedelta(hours=1), current_time, conn)

            if end_time - start_day >= timedelta(days=1):
                results[buckets["1d"]].write_result(end_time - timedelta(days=1), end_time, conn)
        
        return 0


def determine_start_day():
    if FIRST_RUN:
        return DATA_START_DATE

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    row = cursor.execute(
        "SELECT MAX(bucket_end) FROM protocol_stats WHERE granularity = '5m'"
    ).fetchone()
    conn.close()

    if row and row[0]:
        last_bucket_end = datetime.fromtimestamp(row[0])
        # Reprocess the day containing the next bucket to keep aggregates consistent
        return datetime(last_bucket_end.year, last_bucket_end.month, last_bucket_end.day)

    return DATA_START_DATE


def determine_end_day():
    # Only process fully completed days to keep 1d aggregates accurate
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

def main():

    init_database()

    timer = datetime.now()
    start_day = determine_start_day()
    end_day = determine_end_day()

    if start_day >= end_day:
        print("No completed days to process.")
        return

    delta = timedelta(days=1)
    tasks = []
    current_day = start_day
    while current_day + delta <= end_day:
        tasks.append((current_day, current_day + delta))
        current_day += delta

    if not tasks:
        print("No completed days to process.")
        return

    print(f"Found {len(tasks)} day-long tasks to process")
    with Pool(processes=MAX_WORKERS) as pool:
        pool.map(process_day, tasks)

    print(f"Time taken: {datetime.now() - timer}")
if __name__ == "__main__":
    main()
