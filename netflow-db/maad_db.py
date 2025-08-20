import sqlite3
import subprocess
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

# Load environment variables from .env file
def load_env_file(env_path='.env'):
    """Load environment variables from .env file"""
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

# Validate paths exist
if not os.path.exists(NETFLOW_DATA_PATH):
    print(f"ERROR: NETFLOW_DATA_PATH '{NETFLOW_DATA_PATH}' does not exist!")
    sys.exit(1)

def main():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # TODO: create tables and insert