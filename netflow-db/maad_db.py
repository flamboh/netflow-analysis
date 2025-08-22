import sqlite3
import subprocess
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path
import tempfile
import threading
import time

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

    # Create MAAD analysis tables if they don't exist
    create_tables(cursor)
    
    # TODO: Process netflow files and generate MAAD analysis
    
    conn.close()

def create_tables(cursor):
    """Create MAAD analysis tables with optimized denormalized schema"""
    
    # Create metadata table linking MAAD analysis to netflow_stats
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS maad_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        netflow_stats_id INTEGER NOT NULL,
        source BOOLEAN NOT NULL,  -- 1 = source addresses, 0 = destination addresses
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (netflow_stats_id) REFERENCES netflow_stats(id),
        UNIQUE(netflow_stats_id, source)
    )
    """)

    # Create structure function results table with denormalized fields for fast querying
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS structure_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metadata_id INTEGER NOT NULL,
        -- Denormalized fields for fast filtering (avoid joins)
        netflow_stats_id INTEGER NOT NULL,
        router TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        source BOOLEAN NOT NULL,
        -- Analysis data
        q_tau_sd_blob BLOB NOT NULL,  -- CSV data: q,tauTilde,sd
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (metadata_id) REFERENCES maad_metadata(id),
        FOREIGN KEY (netflow_stats_id) REFERENCES netflow_stats(id),
        UNIQUE(metadata_id)
    )
    """)

    # Create spectrum results table with denormalized fields for fast querying
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS spectrum_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metadata_id INTEGER NOT NULL,
        -- Denormalized fields for fast filtering (avoid joins)
        netflow_stats_id INTEGER NOT NULL,
        router TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        source BOOLEAN NOT NULL,
        -- Analysis data
        alpha_f_blob BLOB NOT NULL,  -- CSV data: alpha,f
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (metadata_id) REFERENCES maad_metadata(id),
        FOREIGN KEY (netflow_stats_id) REFERENCES netflow_stats(id),
        UNIQUE(metadata_id)
    )
    """)

    # Create optimized indexes for common query patterns
    create_indexes(cursor)

def create_indexes(cursor):
    """Create performance indexes for common MAAD analysis query patterns"""
    
    # Metadata table indexes
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_maad_metadata_netflow_source 
    ON maad_metadata(netflow_stats_id, source)
    """)

    # Structure function result indexes for time-series queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_structure_source_timestamp 
    ON structure_results(source, timestamp)
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_structure_router_timestamp 
    ON structure_results(router, timestamp)
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_structure_source_router_timestamp 
    ON structure_results(source, router, timestamp)
    """)

    # Spectrum result indexes for time-series queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_spectrum_source_timestamp 
    ON spectrum_results(source, timestamp)
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_spectrum_router_timestamp 
    ON spectrum_results(router, timestamp)
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_spectrum_source_router_timestamp 
    ON spectrum_results(source, router, timestamp)
    """)

    print("MAAD database tables and indexes created successfully")


def run_parallel_maad_analysis(file_path, is_source=True, timeout_seconds=120):
    """
    Run both StructureFunction and Spectrum analysis in parallel using single nfdump execution
    
    Args:
        file_path (str): Path to NetFlow file (nfcapd.YYYYMMDDHHmm)
        is_source (bool): True for source addresses, False for destination addresses
        timeout_seconds (int): Timeout for entire analysis process
        
    Returns:
        tuple: (structure_function_csv, spectrum_csv) or (None, None) on error
        
    Raises:
        FileNotFoundError: If NetFlow file doesn't exist
        subprocess.TimeoutExpired: If analysis takes too long
        subprocess.CalledProcessError: If any command fails
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"NetFlow file not found: {file_path}")
    
    # Get MAAD executable paths
    maad_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'maad')
    structure_function_path = os.path.join(maad_path, 'StructureFunction')
    spectrum_path = os.path.join(maad_path, 'Spectrum')
    
    # Verify MAAD executables exist
    if not os.path.exists(structure_function_path):
        raise FileNotFoundError(f"StructureFunction executable not found: {structure_function_path}")
    if not os.path.exists(spectrum_path):
        raise FileNotFoundError(f"Spectrum executable not found: {spectrum_path}")
    
    # Create named pipe for streaming addresses
    pipe_path = tempfile.mktemp(suffix='.fifo')
    
    try:
        os.mkfifo(pipe_path)
        
        # Use %sa for source addresses, %da for destination addresses
        address_format = '%sa' if is_source else '%da'
        
        print(f"Running parallel MAAD analysis for {'source' if is_source else 'destination'} addresses from {file_path}")
        
        # Variables to store results and errors
        structure_result = None
        spectrum_result = None
        nfdump_error = None
        structure_error = None
        spectrum_error = None
        
        def run_nfdump():
            """Extract addresses and write to named pipe"""
            nonlocal nfdump_error
            try:
                command = f"nfdump -r '{file_path}' 'ipv4' -o 'fmt:{address_format}' -q > {pipe_path}"
                result = subprocess.run(
                    command,
                    shell=True,
                    timeout=timeout_seconds,
                    check=True,
                    capture_output=True,
                    text=True
                )
            except Exception as e:
                nfdump_error = e
        
        def run_structure_function():
            """Run StructureFunction analysis reading from named pipe"""
            nonlocal structure_result, structure_error
            try:
                result = subprocess.run(
                    [structure_function_path, pipe_path],
                    timeout=timeout_seconds,
                    check=True,
                    capture_output=True,
                    text=True,
                    cwd=maad_path
                )
                structure_result = result.stdout
            except Exception as e:
                structure_error = e
        
        def run_spectrum():
            """Run Spectrum analysis reading from named pipe"""
            nonlocal spectrum_result, spectrum_error
            try:
                result = subprocess.run(
                    [spectrum_path, pipe_path],
                    timeout=timeout_seconds,
                    check=True,
                    capture_output=True,
                    text=True,
                    cwd=maad_path
                )
                spectrum_result = result.stdout
            except Exception as e:
                spectrum_error = e
        
        # Start all processes
        nfdump_thread = threading.Thread(target=run_nfdump)
        structure_thread = threading.Thread(target=run_structure_function)
        spectrum_thread = threading.Thread(target=run_spectrum)
        
        # Start nfdump first
        nfdump_thread.start()
        
        # Small delay to ensure pipe is ready
        time.sleep(0.1)
        
        # Start analysis tools
        structure_thread.start()
        spectrum_thread.start()
        
        # Wait for all to complete
        nfdump_thread.join(timeout=timeout_seconds)
        structure_thread.join(timeout=timeout_seconds)
        spectrum_thread.join(timeout=timeout_seconds)
        
        # Check for errors
        if nfdump_error:
            raise nfdump_error
        if structure_error:
            raise structure_error
        if spectrum_error:
            raise spectrum_error
        
        print(f"Parallel MAAD analysis completed successfully")
        return structure_result, spectrum_result
        
    finally:
        # Clean up named pipe
        try:
            if os.path.exists(pipe_path):
                os.unlink(pipe_path)
        except OSError:
            pass  # Ignore cleanup errors


def parse_structure_function_csv(csv_output):
    """
    Parse StructureFunction CSV output into list of dictionaries
    
    Args:
        csv_output (str): CSV output from StructureFunction executable
        
    Returns:
        list: List of dicts with keys: q, tauTilde, sd
    """
    lines = csv_output.strip().split('\n')
    if not lines or lines[0] != 'q,tauTilde,sd':
        raise ValueError("Invalid StructureFunction output format")
    
    data = []
    for line in lines[1:]:  # Skip header
        parts = line.split(',')
        if len(parts) == 3:
            try:
                data.append({
                    'q': float(parts[0]),
                    'tauTilde': float(parts[1]),
                    'sd': float(parts[2])
                })
            except ValueError:
                continue  # Skip invalid lines
    
    return data


def parse_spectrum_csv(csv_output):
    """
    Parse Spectrum CSV output into list of dictionaries
    
    Args:
        csv_output (str): CSV output from Spectrum executable
        
    Returns:
        list: List of dicts with keys: alpha, f
    """
    lines = csv_output.strip().split('\n')
    if not lines or lines[0] != 'alpha,f':
        raise ValueError("Invalid Spectrum output format")
    
    data = []
    for line in lines[1:]:  # Skip header
        parts = line.split(',')
        if len(parts) == 2:
            try:
                data.append({
                    'alpha': float(parts[0]),
                    'f': float(parts[1])
                })
            except ValueError:
                continue  # Skip invalid lines
    
    return data


if __name__ == "__main__":
    main()