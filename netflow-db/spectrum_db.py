"""
MAAD Spectrum statistics processor.

Processes nfcapd files to compute multifractal spectrum for IP addresses.
"""

import sqlite3
import subprocess
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from common import (
    NETFLOW_DATA_PATH,
    AVAILABLE_ROUTERS,
    DATABASE_PATH,
    MAX_WORKERS,
    get_db_connection,
    get_optional_env,
    construct_file_path,
    timestamp_to_unix,
)
from discovery import (
    sync_processed_files_table,
    get_files_needing_processing,
    mark_file_processed,
)

FIRST_RUN = get_optional_env('FIRST_RUN', 'False').lower() in ('true', '1', 'yes')

# Spectrum binary path
SPECTRUM_BIN = get_optional_env(
    'SPECTRUM_BIN',
    str(Path(__file__).parent.parent / 'maad' / 'Spectrum')
)


def init_spectrum_stats_table(conn: sqlite3.Connection) -> None:
    """Create the spectrum_stats table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS spectrum_stats (
            router TEXT NOT NULL,
            granularity TEXT NOT NULL CHECK (granularity IN ('5m', '30m', '1h', '1d')),
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            ip_version INTEGER NOT NULL CHECK (ip_version IN (4, 6)),
            spectrum_json_sa TEXT NOT NULL,
            spectrum_json_da TEXT NOT NULL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (router, granularity, bucket_start, ip_version)
        ) WITHOUT ROWID;
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_spectrum_granularity_time 
        ON spectrum_stats(granularity, bucket_start);
    """)
    conn.commit()


def extract_ips(file_path: str) -> tuple[set, set]:
    """
    Extract unique IPv4 source and destination addresses from a NetFlow file.
    
    Args:
        file_path: Path to the nfcapd file
        
    Returns:
        Tuple of (source_ips, dest_ips) sets
    """
    print(f"[spectrum_stats] Processing {file_path}")
    sa_ips = set()
    da_ips = set()
    
    try:
        # Extract source addresses
        sa_result = subprocess.run(
            ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa", "ipv4"],
            capture_output=True, text=True, timeout=300
        )
        
        # Extract destination addresses
        da_result = subprocess.run(
            ["nfdump", "-r", file_path, "-q", "-o", "fmt:%da", "ipv4"],
            capture_output=True, text=True, timeout=300
        )
        
        if sa_result.returncode == 0:
            for line in sa_result.stdout.strip().split("\n"):
                if line.strip():
                    sa_ips.add(line.strip())
        
        if da_result.returncode == 0:
            for line in da_result.stdout.strip().split("\n"):
                if line.strip():
                    da_ips.add(line.strip())
    except subprocess.TimeoutExpired:
        print(f"Timeout extracting IPs from {file_path}")
    except Exception as e:
        print(f"Error extracting IPs from {file_path}: {e}")
    
    return sa_ips, da_ips


def compute_spectrum(ips: set) -> list[dict]:
    """
    Compute spectrum for a set of IP addresses.
    
    Calls the Haskell Spectrum binary via stdin and parses the CSV output.
    
    Args:
        ips: Set of IP addresses (as strings)
        
    Returns:
        List of dicts with keys: alpha, f
    """
    if len(ips) < 100:
        # Not enough data for meaningful spectrum
        return []
    
    input_data = '\n'.join(ips)
    
    try:
        result = subprocess.run(
            [SPECTRUM_BIN, "/dev/stdin"],
            input=input_data,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode != 0:
            print(f"Spectrum error (returncode {result.returncode}): {result.stderr}")
            return []
        
        # Parse CSV output: alpha,f
        points = []
        lines = result.stdout.strip().split('\n')
        if len(lines) < 2:
            return []
        
        # Skip header line
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) == 2:
                try:
                    points.append({
                        "alpha": float(parts[0]),
                        "f": float(parts[1])
                    })
                except ValueError:
                    continue
        
        return points
    except subprocess.TimeoutExpired:
        print(f"Spectrum timed out for {len(ips)} IPs")
        return []
    except FileNotFoundError:
        print(f"Spectrum binary not found at {SPECTRUM_BIN}")
        return []
    except Exception as e:
        print(f"Spectrum error: {e}")
        return []


def process_file_for_stats(
    conn: sqlite3.Connection,
    file_path: str,
    router: str,
    timestamp_unix: int,
    file_exists: bool = True
) -> bool:
    """
    Process a single file and insert 5-minute granularity spectrum stats.
    
    Args:
        conn: Database connection
        file_path: Path to the nfcapd file
        router: Router name
        timestamp_unix: Unix timestamp for this file
        file_exists: If False, insert empty spectrum (gap placeholder)
        
    Returns:
        True if processing succeeded, False otherwise
    """
    bucket_start = timestamp_unix
    bucket_end = timestamp_unix + 300  # 5 minutes
    
    if not file_exists:
        # Insert empty spectrum for gap
        try:
            conn.execute("""
                INSERT OR REPLACE INTO spectrum_stats 
                (router, granularity, bucket_start, bucket_end, ip_version,
                 spectrum_json_sa, spectrum_json_da)
                VALUES (?, '5m', ?, ?, 4, '[]', '[]')
            """, (router, bucket_start, bucket_end))
            return True
        except Exception as e:
            print(f"Error inserting zero row for {file_path}: {e}")
            return False
    
    try:
        sa_ips, da_ips = extract_ips(file_path)
        
        # Compute spectrum for source and destination IPs
        spectrum_sa = compute_spectrum(sa_ips)
        spectrum_da = compute_spectrum(da_ips)
        
        conn.execute("""
            INSERT OR REPLACE INTO spectrum_stats 
            (router, granularity, bucket_start, bucket_end, ip_version,
             spectrum_json_sa, spectrum_json_da)
            VALUES (?, '5m', ?, ?, 4, ?, ?)
        """, (router, bucket_start, bucket_end, 
              json.dumps(spectrum_sa), json.dumps(spectrum_da)))
        
        return True
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


class BucketAggregator:
    """Aggregates IP sets across multiple files for larger time buckets."""
    
    def __init__(self, router: str, granularity: str):
        self.router = router
        self.granularity = granularity
        self.ips_sa = set()
        self.ips_da = set()

    def update(self, ips_sa: set, ips_da: set):
        """Add IPs to the aggregated sets."""
        self.ips_sa.update(ips_sa)
        self.ips_da.update(ips_da)

    def write(self, bucket_start: int, bucket_end: int, conn: sqlite3.Connection):
        """Compute spectrum and write to database."""
        spectrum_sa = compute_spectrum(self.ips_sa)
        spectrum_da = compute_spectrum(self.ips_da)
        
        conn.execute("""
            INSERT OR REPLACE INTO spectrum_stats 
            (router, granularity, bucket_start, bucket_end, ip_version,
             spectrum_json_sa, spectrum_json_da)
            VALUES (?, ?, ?, ?, 4, ?, ?)
        """, (self.router, self.granularity, bucket_start, bucket_end,
              json.dumps(spectrum_sa), json.dumps(spectrum_da)))
        
        # Reset for next bucket
        self.ips_sa = set()
        self.ips_da = set()


def aggregate_buckets(conn: sqlite3.Connection, router: str, day_start: datetime) -> None:
    """
    Compute 30m, 1h, and 1d spectrum aggregates for a given day.
    
    Args:
        conn: Database connection
        router: Router name
        day_start: Start of the day to aggregate
    """
    day_end = day_start + timedelta(days=1)
    
    # Initialize aggregators
    agg_30m = BucketAggregator(router, '30m')
    agg_1h = BucketAggregator(router, '1h')
    agg_1d = BucketAggregator(router, '1d')
    
    current = day_start
    mins = 0
    
    while current < day_end:
        file_path = construct_file_path(router, current)
        
        # Check if file exists by querying processed_files
        cursor = conn.cursor()
        row = cursor.execute("""
            SELECT file_exists FROM processed_files WHERE file_path = ?
        """, (file_path,)).fetchone()
        
        if row and row[0]:
            # File exists, extract IPs
            sa_ips, da_ips = extract_ips(file_path)
            agg_30m.update(sa_ips, da_ips)
            agg_1h.update(sa_ips, da_ips)
            agg_1d.update(sa_ips, da_ips)
        
        mins += 5
        current += timedelta(minutes=5)
        
        # Write 30m bucket
        if mins % 30 == 0:
            bucket_start = timestamp_to_unix(current - timedelta(minutes=30))
            bucket_end = timestamp_to_unix(current)
            agg_30m.write(bucket_start, bucket_end, conn)
        
        # Write 1h bucket
        if mins % 60 == 0:
            bucket_start = timestamp_to_unix(current - timedelta(hours=1))
            bucket_end = timestamp_to_unix(current)
            agg_1h.write(bucket_start, bucket_end, conn)
    
    # Write 1d bucket
    agg_1d.write(timestamp_to_unix(day_start), timestamp_to_unix(day_end), conn)
    conn.commit()


def process_pending_files(conn: sqlite3.Connection, limit: int = None) -> dict:
    """
    Process all pending files for the spectrum_stats table.
    
    Args:
        conn: Database connection
        limit: Optional limit on number of files to process
        
    Returns:
        Dictionary with counts: {'processed': N, 'errors': N}
    """
    init_spectrum_stats_table(conn)
    
    pending = get_files_needing_processing(conn, 'spectrum_stats', limit)
    stats = {'processed': 0, 'errors': 0}
    
    print(f"Processing {len(pending)} pending files for spectrum_stats...")
    
    for file_path, router, timestamp, file_exists in pending:
        success = process_file_for_stats(conn, file_path, router, timestamp, file_exists)
        mark_file_processed(conn, file_path, 'spectrum_stats', success)
        
        if success:
            stats['processed'] += 1
        else:
            stats['errors'] += 1
        
        # Commit periodically
        if (stats['processed'] + stats['errors']) % 100 == 0:
            conn.commit()
            print(f"[spectrum_stats] Progress: {stats['processed']} processed, {stats['errors']} errors")
    
    conn.commit()
    return stats


def main():
    """Main entry point for standalone execution."""
    print("--------------------------------")
    print(f"Spectrum DB processing at {datetime.now()}")
    print("--------------------------------")
    
    with get_db_connection() as conn:
        # First sync the processed_files table
        sync_processed_files_table(conn)
        
        # Then process pending files
        stats = process_pending_files(conn)
        
        print(f"Processing complete: {stats}")


if __name__ == "__main__":
    main()
