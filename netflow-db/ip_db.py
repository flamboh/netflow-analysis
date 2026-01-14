"""
IP statistics processor.

Processes nfcapd files to extract unique IP address counts at various granularities.
"""

import sqlite3
import subprocess
from datetime import datetime, timedelta
from multiprocessing import Pool
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


def init_ip_stats_table(conn: sqlite3.Connection) -> None:
    """Create the ip_stats table if it doesn't exist."""
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


def process_file(file_path: str) -> tuple[set, set, set, set]:
    """
    Extract unique IP addresses from a NetFlow file.
    
    Args:
        file_path: Path to the nfcapd file
        
    Returns:
        Tuple of (sa_v4, da_v4, sa_v6, da_v6) sets of IP addresses
    """
    print(f"[ip_stats] Processing {file_path}")
    sa_v4_res = set()
    da_v4_res = set()
    sa_v6_res = set()
    da_v6_res = set()
    
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv4"]
    ipv6_command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv6", "-6"]
    
    try:
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
    except subprocess.TimeoutExpired:
        print(f"Timeout processing {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

    return sa_v4_res, da_v4_res, sa_v6_res, da_v6_res


def process_file_for_stats(
    conn: sqlite3.Connection,
    file_path: str,
    router: str,
    timestamp_unix: int,
    file_exists: bool = True
) -> bool:
    """
    Process a single file and insert 5-minute granularity stats.
    
    Args:
        conn: Database connection
        file_path: Path to the nfcapd file
        router: Router name
        timestamp_unix: Unix timestamp for this file
        file_exists: If False, insert zero-valued row (gap placeholder)
        
    Returns:
        True if processing succeeded, False otherwise
    """
    bucket_start = timestamp_unix
    bucket_end = timestamp_unix + 300  # 5 minutes
    
    if not file_exists:
        # Insert zero-valued row for gap
        try:
            conn.execute("""
                INSERT OR REPLACE INTO ip_stats 
                (router, granularity, bucket_start, bucket_end, 
                 sa_ipv4_count, da_ipv4_count, sa_ipv6_count, da_ipv6_count)
                VALUES (?, '5m', ?, ?, 0, 0, 0, 0)
            """, (router, bucket_start, bucket_end))
            return True
        except Exception as e:
            print(f"Error inserting zero row for {file_path}: {e}")
            return False
    
    try:
        sa_v4, da_v4, sa_v6, da_v6 = process_file(file_path)
        
        conn.execute("""
            INSERT OR REPLACE INTO ip_stats 
            (router, granularity, bucket_start, bucket_end, 
             sa_ipv4_count, da_ipv4_count, sa_ipv6_count, da_ipv6_count)
            VALUES (?, '5m', ?, ?, ?, ?, ?, ?)
        """, (router, bucket_start, bucket_end, 
              len(sa_v4), len(da_v4), len(sa_v6), len(da_v6)))
        
        return True
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


class BucketAggregator:
    """Aggregates IP sets across multiple files for larger time buckets."""
    
    def __init__(self, router: str, granularity: str):
        self.router = router
        self.granularity = granularity
        self.sa_v4 = set()
        self.da_v4 = set()
        self.sa_v6 = set()
        self.da_v6 = set()

    def update(self, sa_v4: set, da_v4: set, sa_v6: set, da_v6: set):
        """Add IP addresses to the aggregated sets."""
        self.sa_v4.update(sa_v4)
        self.da_v4.update(da_v4)
        self.sa_v6.update(sa_v6)
        self.da_v6.update(da_v6)

    def write(self, bucket_start: int, bucket_end: int, conn: sqlite3.Connection):
        """Write the aggregated stats to the database."""
        conn.execute("""
            INSERT OR REPLACE INTO ip_stats 
            (router, granularity, bucket_start, bucket_end, 
             sa_ipv4_count, da_ipv4_count, sa_ipv6_count, da_ipv6_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.router, self.granularity, bucket_start, bucket_end,
              len(self.sa_v4), len(self.da_v4), len(self.sa_v6), len(self.da_v6)))
        
        # Reset for next bucket
        self.sa_v4 = set()
        self.da_v4 = set()
        self.sa_v6 = set()
        self.da_v6 = set()


def aggregate_buckets(conn: sqlite3.Connection, router: str, day_start: datetime) -> None:
    """
    Compute 30m, 1h, and 1d aggregates for a given day.
    
    This function reads the raw IP data from files and aggregates
    into larger time buckets. Should be called after all 5m data
    for a day has been processed.
    
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
            # File exists, process it
            sa_v4, da_v4, sa_v6, da_v6 = process_file(file_path)
            agg_30m.update(sa_v4, da_v4, sa_v6, da_v6)
            agg_1h.update(sa_v4, da_v4, sa_v6, da_v6)
            agg_1d.update(sa_v4, da_v4, sa_v6, da_v6)
        
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
    Process all pending files for the ip_stats table.
    
    Args:
        conn: Database connection
        limit: Optional limit on number of files to process
        
    Returns:
        Dictionary with counts: {'processed': N, 'errors': N}
    """
    init_ip_stats_table(conn)
    
    pending = get_files_needing_processing(conn, 'ip_stats', limit)
    stats = {'processed': 0, 'errors': 0}
    
    print(f"Processing {len(pending)} pending files for ip_stats...")
    
    for file_path, router, timestamp, file_exists in pending:
        success = process_file_for_stats(conn, file_path, router, timestamp, file_exists)
        mark_file_processed(conn, file_path, 'ip_stats', success)
        
        if success:
            stats['processed'] += 1
        else:
            stats['errors'] += 1
        
        # Commit periodically
        if (stats['processed'] + stats['errors']) % 100 == 0:
            conn.commit()
            print(f"[ip_stats] Progress: {stats['processed']} processed, {stats['errors']} errors")
    
    conn.commit()
    return stats


def main():
    """Main entry point for standalone execution."""
    print("--------------------------------")
    print(f"IP DB processing at {datetime.now()}")
    print("--------------------------------")
    
    with get_db_connection() as conn:
        # First sync the processed_files table
        sync_processed_files_table(conn)
        
        # Then process pending files
        stats = process_pending_files(conn)
        
        print(f"Processing complete: {stats}")


if __name__ == "__main__":
    main()
