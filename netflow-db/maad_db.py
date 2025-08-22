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
    print(f"Starting MAAD database processing...")
    print(f"Database path: {DATABASE_PATH}")
    print(f"NetFlow data path: {NETFLOW_DATA_PATH}")
    print(f"Available routers: {AVAILABLE_ROUTERS}")
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        print(f"Connected to database successfully")

        # Create MAAD analysis tables if they don't exist
        print("Creating/verifying MAAD tables...")
        create_tables(cursor)
        
        # Process all netflow records chronologically
        process_netflow_records(conn, cursor)
        
        conn.close()
        print("Database connection closed successfully")
        
    except Exception as e:
        print(f"FATAL ERROR in main(): {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def get_netflow_records(cursor):
    """
    Get all netflow_stats records in chronological order
    
    Returns:
        list: List of tuples (id, file_path, router, timestamp)
    """
    print("Querying netflow_stats records...")
    try:
        cursor.execute("""
            SELECT id, file_path, router, timestamp 
            FROM netflow_stats 
            ORDER BY timestamp ASC
        """)
        records = cursor.fetchall()
        print(f"Found {len(records)} netflow_stats records")
        if len(records) > 0:
            print(f"First record: {records[0]}")
            print(f"Last record: {records[-1]}")
        return records
    except Exception as e:
        print(f"ERROR querying netflow_stats: {e}")
        raise


def analysis_exists(cursor, netflow_stats_id, is_source):
    """
    Check if MAAD analysis already exists for given netflow record and address type
    
    Args:
        cursor: Database cursor
        netflow_stats_id (int): ID from netflow_stats table
        is_source (bool): True for source addresses, False for destination
        
    Returns:
        bool: True if analysis already exists
    """
    cursor.execute("""
        SELECT 1 FROM maad_metadata 
        WHERE netflow_stats_id = ? AND source = ?
    """, (netflow_stats_id, 1 if is_source else 0))
    
    return cursor.fetchone() is not None


def insert_maad_results(cursor, netflow_record, is_source, structure_csv, spectrum_csv):
    """
    Insert MAAD analysis results into database tables
    
    Args:
        cursor: Database cursor
        netflow_record: Tuple (id, file_path, router, timestamp) from netflow_stats
        is_source (bool): True for source addresses, False for destination
        structure_csv (str): StructureFunction CSV output
        spectrum_csv (str): Spectrum CSV output
    """
    netflow_id, file_path, router, timestamp = netflow_record
    source_bit = 1 if is_source else 0
    address_type = 'source' if is_source else 'destination'
    
    print(f"DEBUG: Inserting MAAD results for {file_path} ({address_type})")
    print(f"DEBUG: NetFlow ID: {netflow_id}, Router: {router}, Timestamp: {timestamp}")
    print(f"DEBUG: Structure CSV length: {len(structure_csv)}, Spectrum CSV length: {len(spectrum_csv)}")
    
    try:
        # Insert metadata record
        print(f"DEBUG: Inserting metadata record...")
        cursor.execute("""
            INSERT INTO maad_metadata (netflow_stats_id, source)
            VALUES (?, ?)
        """, (netflow_id, source_bit))
        
        metadata_id = cursor.lastrowid
        print(f"DEBUG: Metadata inserted with ID: {metadata_id}")
        
        # Convert CSV to BLOB for storage
        structure_blob = structure_csv.encode('utf-8')
        spectrum_blob = spectrum_csv.encode('utf-8')
        
        # Insert structure function results with denormalized fields
        print(f"DEBUG: Inserting structure function results...")
        cursor.execute("""
            INSERT INTO structure_results (
                metadata_id, netflow_stats_id, router, timestamp, source, q_tau_sd_blob
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (metadata_id, netflow_id, router, timestamp, source_bit, structure_blob))
        
        # Insert spectrum results with denormalized fields
        print(f"DEBUG: Inserting spectrum results...")
        cursor.execute("""
            INSERT INTO spectrum_results (
                metadata_id, netflow_stats_id, router, timestamp, source, alpha_f_blob
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (metadata_id, netflow_id, router, timestamp, source_bit, spectrum_blob))
        
        print(f"Stored MAAD results for {router} {datetime.fromtimestamp(timestamp)} ({address_type})")
        
    except Exception as e:
        print(f"ERROR storing MAAD results for {file_path} ({address_type}): {e}")
        import traceback
        traceback.print_exc()
        raise


def process_netflow_records(conn, cursor):
    """
    Process all netflow records chronologically, generating MAAD analysis
    """
    print("Starting MAAD analysis processing...")
    
    # Get all netflow records
    records = get_netflow_records(cursor)
    total_records = len(records)
    total_analyses = total_records * 2  # source + destination for each file
    
    print(f"Found {total_records} netflow records, need {total_analyses} total analyses")
    
    processed_count = 0
    error_count = 0
    
    for record in records:
        netflow_id, file_path, router, timestamp = record
        
        # Process both source and destination addresses
        for is_source in [True, False]:
            address_type = 'source' if is_source else 'destination'
            
            # Skip if analysis already exists
            if analysis_exists(cursor, netflow_id, is_source):
                print(f"Skipping {file_path} ({address_type}) - already processed")
                continue
            
            try:
                print(f"Processing {file_path} ({address_type})...")
                
                # Run sequential MAAD analysis
                structure_csv, spectrum_csv = run_sequential_maad_analysis(file_path, is_source)
                
                # Store results in database
                insert_maad_results(cursor, record, is_source, structure_csv, spectrum_csv)
                
                processed_count += 1
                
                # Commit every 50 analyses for reliability
                if processed_count % 50 == 0:
                    conn.commit()
                    print(f"Progress: {processed_count} analyses completed, {error_count} errors")
                
            except Exception as e:
                error_count += 1
                print(f"Error processing {file_path} ({address_type}): {e}")
                # Continue processing other files
                continue
    
    # Final commit
    conn.commit()
    print(f"MAAD processing complete: {processed_count} analyses processed, {error_count} errors")


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


def run_sequential_maad_analysis(file_path, is_source=True, timeout_seconds=120):
    """
    Run both StructureFunction and Spectrum analysis sequentially using direct nfdump piping
    
    Args:
        file_path (str): Path to NetFlow file (nfcapd.YYYYMMDDHHmm)
        is_source (bool): True for source addresses, False for destination addresses
        timeout_seconds (int): Timeout for each analysis process
        
    Returns:
        tuple: (structure_function_csv, spectrum_csv) or (None, None) on error
        
    Raises:
        FileNotFoundError: If NetFlow file doesn't exist
        subprocess.TimeoutExpired: If analysis takes too long
        subprocess.CalledProcessError: If any command fails
    """
    print(f"DEBUG: Starting MAAD analysis for {file_path} ({'source' if is_source else 'destination'})")
    
    if not os.path.exists(file_path):
        print(f"DEBUG: NetFlow file not found: {file_path}")
        raise FileNotFoundError(f"NetFlow file not found: {file_path}")
    
    # Get MAAD executable paths
    maad_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'maad')
    structure_function_path = os.path.join(maad_path, 'StructureFunction')
    spectrum_path = os.path.join(maad_path, 'Spectrum')
    
    print(f"DEBUG: MAAD path: {maad_path}")
    print(f"DEBUG: StructureFunction path: {structure_function_path}")
    print(f"DEBUG: Spectrum path: {spectrum_path}")
    
    # Verify MAAD executables exist
    if not os.path.exists(structure_function_path):
        print(f"DEBUG: StructureFunction executable not found at: {structure_function_path}")
        raise FileNotFoundError(f"StructureFunction executable not found: {structure_function_path}")
    if not os.path.exists(spectrum_path):
        print(f"DEBUG: Spectrum executable not found at: {spectrum_path}")
        raise FileNotFoundError(f"Spectrum executable not found: {spectrum_path}")
    
    # Use %sa for source addresses, %da for destination addresses
    address_format = '%sa' if is_source else '%da'
    
    print(f"Running sequential MAAD analysis for {'source' if is_source else 'destination'} addresses from {file_path}")
    
    try:
        # Run StructureFunction with direct nfdump pipe (like the API endpoint)
        structure_command = f"nfdump -r '{file_path}' 'ipv4' -o 'fmt:{address_format}' -q | '{structure_function_path}' /dev/stdin"
        print(f"DEBUG: Running StructureFunction command: {structure_command}")
        
        structure_result = subprocess.run(
            structure_command,
            shell=True,
            timeout=timeout_seconds,
            check=True,
            capture_output=True,
            text=True,
            cwd=maad_path
        )
        print(f"DEBUG: StructureFunction completed, output length: {len(structure_result.stdout)}")
        
        # Run Spectrum with direct nfdump pipe
        spectrum_command = f"nfdump -r '{file_path}' 'ipv4' -o 'fmt:{address_format}' -q | '{spectrum_path}' /dev/stdin"
        print(f"DEBUG: Running Spectrum command: {spectrum_command}")
        
        spectrum_result = subprocess.run(
            spectrum_command,
            shell=True,
            timeout=timeout_seconds,
            check=True,
            capture_output=True,
            text=True,
            cwd=maad_path
        )
        print(f"DEBUG: Spectrum completed, output length: {len(spectrum_result.stdout)}")
        
        print(f"Sequential MAAD analysis completed successfully")
        return structure_result.stdout, spectrum_result.stdout
        
    except Exception as e:
        print(f"DEBUG: MAAD analysis failed: {e}")
        raise


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