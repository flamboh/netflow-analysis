"""
Shared utilities for NetFlow database processing modules.

Centralizes environment loading, database helpers, and path utilities.
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional
from contextlib import contextmanager


def load_env_file(env_path: str = '../.env') -> None:
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


def get_required_env(key: str) -> str:
    """Get required environment variable or exit with error."""
    value = os.environ.get(key)
    if not value:
        print(f"ERROR: Required environment variable '{key}' is not set!")
        print("Please check your .env file configuration.")
        sys.exit(1)
    return value


def get_optional_env(key: str, default: str = '') -> str:
    """Get optional environment variable with a default value."""
    return os.environ.get(key, default)


# Initialize environment on module import
load_env_file()

# Configuration from environment variables
NETFLOW_DATA_PATH = get_required_env('NETFLOW_DATA_PATH')
AVAILABLE_ROUTERS = get_required_env('AVAILABLE_ROUTERS').split(',')
DATABASE_PATH = get_required_env('DATABASE_PATH')
MAX_WORKERS = int(get_optional_env('MAX_WORKERS', '8'))
BATCH_SIZE = int(get_optional_env('BATCH_SIZE', '50'))

# Data before Feb 2025 is corrupt/unusable - only process from this date forward
DATA_START_DATE = datetime(2025, 2, 1)


@contextmanager
def get_db_connection(wal_mode: bool = True):
    """
    Context manager for database connections with optional WAL mode.
    
    Args:
        wal_mode: If True, enables WAL journal mode and sets busy timeout.
                  Recommended for concurrent access.
    
    Yields:
        sqlite3.Connection object
    """
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        if wal_mode:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=60000;")
        yield conn
    finally:
        conn.close()


def construct_file_path(router: str, timestamp: datetime) -> str:
    """
    Construct the expected file path for a NetFlow capture file.
    
    Args:
        router: Router name (e.g., 'cc-ir1-gw')
        timestamp: Datetime object for the capture time
        
    Returns:
        Full path to the expected nfcapd file
    """
    timestamp_str = timestamp.strftime('%Y%m%d%H%M')
    year = timestamp.strftime('%Y')
    month = timestamp.strftime('%m')
    day = timestamp.strftime('%d')
    
    return f"{NETFLOW_DATA_PATH}/{router}/{year}/{month}/{day}/nfcapd.{timestamp_str}"


def parse_file_path(file_path: str) -> tuple[str, datetime]:
    """
    Parse a NetFlow file path to extract router and timestamp.
    
    Args:
        file_path: Full path to an nfcapd file
        
    Returns:
        Tuple of (router_name, timestamp_datetime)
        
    Raises:
        ValueError: If the file path cannot be parsed
    """
    path = Path(file_path)
    filename = path.name  # e.g., 'nfcapd.202403010000'
    
    if not filename.startswith('nfcapd.'):
        raise ValueError(f"Invalid NetFlow filename: {filename}")
    
    timestamp_str = filename.split('.')[1]  # '202403010000'
    
    if len(timestamp_str) != 12:
        raise ValueError(f"Invalid timestamp format in filename: {filename}")
    
    year = int(timestamp_str[0:4])
    month = int(timestamp_str[4:6])
    day = int(timestamp_str[6:8])
    hour = int(timestamp_str[8:10])
    minute = int(timestamp_str[10:12])
    
    timestamp = datetime(year, month, day, hour, minute)
    
    # Extract router from path: .../router/year/month/day/nfcapd.timestamp
    # Path parts: ['', 'path', 'to', 'netflow', 'router', 'year', 'month', 'day', 'nfcapd.xxx']
    parts = file_path.split('/')
    # Router is 4 levels up from the filename
    router_idx = len(parts) - 5
    if router_idx < 0:
        raise ValueError(f"Cannot extract router from path: {file_path}")
    
    router = parts[router_idx]
    
    return router, timestamp


def timestamp_to_unix(dt: datetime) -> int:
    """Convert datetime to Unix timestamp."""
    return int(dt.timestamp())


def unix_to_timestamp(unix_ts: int) -> datetime:
    """Convert Unix timestamp to datetime."""
    return datetime.fromtimestamp(unix_ts)


def init_processed_files_table(conn: sqlite3.Connection) -> None:
    """
    Create the processed_files table if it doesn't exist.
    
    This table centralizes tracking of all NetFlow files (both existing and gap placeholders).
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_path TEXT PRIMARY KEY,
            router TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            file_exists INTEGER NOT NULL DEFAULT 1,
            discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            processed_at DATETIME,
            
            flow_stats_status INTEGER,
            ip_stats_status INTEGER,
            protocol_stats_status INTEGER,
            spectrum_stats_status INTEGER,
            structure_stats_status INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_processed_files_timestamp 
        ON processed_files(timestamp)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_processed_files_router_timestamp 
        ON processed_files(router, timestamp)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_processed_files_pending 
        ON processed_files(processed_at) 
        WHERE processed_at IS NULL
    """)
    
    conn.commit()
