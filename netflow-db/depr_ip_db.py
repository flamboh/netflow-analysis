from HLL import HyperLogLog
import pickle
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
    """Initialize the ip_stats table if it doesn't exist"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ip_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL UNIQUE,
            router TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            source_ipv4_hll BLOB,
            destination_ipv4_hll BLOB,
            source_ipv6_hll BLOB,
            destination_ipv6_hll BLOB,
            source_ipv4_cardinality INTEGER,
            destination_ipv4_cardinality INTEGER,
            source_ipv6_cardinality INTEGER,
            destination_ipv6_cardinality INTEGER,
            ipv4_count INTEGER,
            ipv6_count INTEGER,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ip_stats_timestamp ON ip_stats(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ip_stats_router ON ip_stats(router)")
    conn.commit()
    conn.close()

def process_file_worker(args):
    """Worker function for processing a single netflow file"""
    file_path, router, timestamp_unix = args

    source_ipv4_hll = HyperLogLog()
    source_ipv6_hll = HyperLogLog()
    destination_ipv4_hll = HyperLogLog()
    destination_ipv6_hll = HyperLogLog()
    ipv4_count = 0
    ipv6_count = 0

    if not os.path.exists(file_path):
        return {'success': False, 'file_path': file_path, 'error': 'File not found'}

    try:
        # Process IPv4 traffic
        ipv4_command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv4"]
        ipv4_result = subprocess.run(ipv4_command, capture_output=True, text=True, timeout=300)

        if ipv4_result.returncode == 0:
            ipv4_out = ipv4_result.stdout.strip().split("\n")
            for line in ipv4_out:
                if line.strip() and ',' in line:
                    try:
                        source_ip, destination_ip = line.strip().split(",")
                        source_ipv4_hll.add(source_ip)
                        destination_ipv4_hll.add(destination_ip)
                        ipv4_count += 1
                    except ValueError:
                        continue

        # Process IPv6 traffic
        ipv6_command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv6", "-6"]
        ipv6_result = subprocess.run(ipv6_command, capture_output=True, text=True, timeout=300)

        if ipv6_result.returncode == 0:
            ipv6_out = ipv6_result.stdout.strip().split("\n")
            for line in ipv6_out:
                if line.strip() and ',' in line:
                    try:
                        source_ip, destination_ip = line.strip().split(",")
                        source_ipv6_hll.add(source_ip)
                        destination_ipv6_hll.add(destination_ip)
                        ipv6_count += 1
                    except ValueError:
                        continue

        # Return processed data for batch database insertion
        return {
            'success': True,
            'file_path': file_path,
            'router': router,
            'timestamp': timestamp_unix,
            'source_ipv4_hll': pickle.dumps(source_ipv4_hll),
            'destination_ipv4_hll': pickle.dumps(destination_ipv4_hll),
            'source_ipv6_hll': pickle.dumps(source_ipv6_hll),
            'destination_ipv6_hll': pickle.dumps(destination_ipv6_hll),
            'source_ipv4_cardinality': source_ipv4_hll.cardinality(),
            'destination_ipv4_cardinality': destination_ipv4_hll.cardinality(),
            'source_ipv6_cardinality': source_ipv6_hll.cardinality(),
            'destination_ipv6_cardinality': destination_ipv6_hll.cardinality(),
            'ipv4_count': ipv4_count,
            'ipv6_count': ipv6_count,
        }

    except subprocess.TimeoutExpired:
        return {'success': False, 'file_path': file_path, 'error': 'Processing timeout'}
    except Exception as e:
        return {'success': False, 'file_path': file_path, 'error': str(e)}

def batch_insert_results(results):
    """Insert processed results into database in batches"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    successful_results = [r for r in results if r['success']]

    if successful_results:
        cursor.executemany("""
            INSERT OR REPLACE INTO ip_stats (
                file_path, router, timestamp,
                source_ipv4_hll, destination_ipv4_hll, source_ipv6_hll, destination_ipv6_hll,
                source_ipv4_cardinality, destination_ipv4_cardinality,
                source_ipv6_cardinality, destination_ipv6_cardinality, ipv4_count, ipv6_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (r['file_path'], r['router'], r['timestamp'],
             r['source_ipv4_hll'], r['destination_ipv4_hll'],
             r['source_ipv6_hll'], r['destination_ipv6_hll'],
             r['source_ipv4_cardinality'], r['destination_ipv4_cardinality'],
             r['source_ipv6_cardinality'], r['destination_ipv6_cardinality'], r['ipv4_count'], r['ipv6_count'])
            for r in successful_results
        ])

    conn.commit()
    conn.close()
    return len(successful_results)

def main():
    # Initialize database
    init_database()

    # Determine start time
    if FIRST_RUN:
        # nfcapd starts at 2024-03-01 00:00:00
        start_time = datetime(2024, 3, 1)
        print("Starting initial processing from 2024-03-01")
    else:
        # Get the last processed file from the database
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        last_file = cursor.execute("""
            SELECT file_path FROM ip_stats ORDER BY timestamp DESC LIMIT 1
        """).fetchone()
        conn.close()

        if last_file:
            last_file_path = last_file[0]
            timestamp_str = last_file_path.split('/')[-1].split('.')[1]
            year = timestamp_str[:4]
            month = timestamp_str[4:6]
            day = timestamp_str[6:8]
            hour = timestamp_str[8:10]
            minute = timestamp_str[10:12]
            last_file_time = datetime(int(year), int(month), int(day), int(hour), int(minute))
            start_time = last_file_time + timedelta(minutes=5)
            print(f"Resuming processing from {start_time}")
        else:
            start_time = datetime(2025, 1, 1)
            print("No previous processing found, starting from 2025-01-01")

    # Collect all file tasks to process
    current_time = start_time
    tasks = []

    print("Scanning for files to process...")
    while current_time < datetime(2025, 2, 1):
        timestamp_str = current_time.strftime('%Y%m%d%H%M')
        timestamp_unix = int(current_time.timestamp())

        for router in AVAILABLE_ROUTERS:
            file_path = f"{NETFLOW_DATA_PATH}/{router}/{current_time.strftime('%Y')}/{current_time.strftime('%m')}/{current_time.strftime('%d')}/nfcapd.{timestamp_str}"
            if os.path.exists(file_path):
                tasks.append((file_path, router, timestamp_unix))

        current_time += timedelta(minutes=5)

    if not tasks:
        print("No files found to process.")
        return

    print(f"Found {len(tasks)} files to process")

    # Process files in parallel batches
    num_workers = min(os.cpu_count() or 4, MAX_WORKERS)
    processed_count = 0
    error_count = 0

    print(f"Processing with {num_workers} workers in batches of {BATCH_SIZE}")

    for i in range(0, len(tasks), BATCH_SIZE):
        batch_tasks = tasks[i:i + BATCH_SIZE]

        with Pool(processes=num_workers) as pool:
            print(f"Processing batch {i//BATCH_SIZE + 1}/{(len(tasks) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(batch_tasks)} files)")
            results = pool.map(process_file_worker, batch_tasks)

        # Insert results into database
        successful_inserts = batch_insert_results(results)
        batch_errors = len([r for r in results if not r['success']])

        processed_count += successful_inserts
        error_count += batch_errors

        print(f"Batch complete: {successful_inserts} processed, {batch_errors} errors")

        # Print error details for failed files
        for result in results:
            if not result['success']:
                print(f"  Error processing {result['file_path']}: {result['error']}")

    print(f"Processing complete: {processed_count} files processed, {error_count} errors")

if __name__ == "__main__":
    main()