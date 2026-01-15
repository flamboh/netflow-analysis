"""
Protocol statistics processor.

Processes nfcapd files to extract unique protocol counts at various granularities.
"""

import sqlite3
import subprocess
from datetime import datetime, timedelta
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


def init_protocol_stats_table(conn: sqlite3.Connection) -> None:
    """Create the protocol_stats table if it doesn't exist."""
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


def process_file(file_path: str) -> tuple[set, set]:
    """
    Extract unique protocols from a NetFlow file.
    
    Args:
        file_path: Path to the nfcapd file
        
    Returns:
        Tuple of (protocols_ipv4, protocols_ipv6) sets
    """
    print(f"Processing {file_path}")
    protocols_ipv4 = set()
    protocols_ipv6 = set()
    
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%pr", "-A", "proto"]
    
    try:
        ipv4_result = subprocess.run(command + ["ipv4", "-N"], capture_output=True, text=True, timeout=300)
        ipv6_result = subprocess.run(command + ["ipv6", "-N"], capture_output=True, text=True, timeout=300)

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
    except subprocess.TimeoutExpired:
        print(f"Timeout processing {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
    
    return protocols_ipv4, protocols_ipv6


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
                INSERT OR REPLACE INTO protocol_stats 
                (router, granularity, bucket_start, bucket_end, 
                 unique_protocols_count_ipv4, unique_protocols_count_ipv6,
                 protocols_list_ipv4, protocols_list_ipv6)
                VALUES (?, '5m', ?, ?, 0, 0, '', '')
            """, (router, bucket_start, bucket_end))
            return True
        except Exception as e:
            print(f"Error inserting zero row for {file_path}: {e}")
            return False
    
    try:
        protocols_ipv4, protocols_ipv6 = process_file(file_path)
        
        conn.execute("""
            INSERT OR REPLACE INTO protocol_stats 
            (router, granularity, bucket_start, bucket_end, 
             unique_protocols_count_ipv4, unique_protocols_count_ipv6,
             protocols_list_ipv4, protocols_list_ipv6)
            VALUES (?, '5m', ?, ?, ?, ?, ?, ?)
        """, (router, bucket_start, bucket_end, 
              len(protocols_ipv4), len(protocols_ipv6),
              ",".join(sorted(protocols_ipv4)), ",".join(sorted(protocols_ipv6))))
        
        return True
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


class BucketAggregator:
    """Aggregates protocol sets across multiple files for larger time buckets."""
    
    def __init__(self, router: str, granularity: str):
        self.router = router
        self.granularity = granularity
        self.protocols_ipv4 = set()
        self.protocols_ipv6 = set()

    def update(self, protocols_ipv4: set, protocols_ipv6: set):
        """Add protocols to the aggregated sets."""
        self.protocols_ipv4.update(protocols_ipv4)
        self.protocols_ipv6.update(protocols_ipv6)

    def write(self, bucket_start: int, bucket_end: int, conn: sqlite3.Connection):
        """Write the aggregated stats to the database."""
        conn.execute("""
            INSERT OR REPLACE INTO protocol_stats 
            (router, granularity, bucket_start, bucket_end, 
             unique_protocols_count_ipv4, unique_protocols_count_ipv6,
             protocols_list_ipv4, protocols_list_ipv6)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.router, self.granularity, bucket_start, bucket_end,
              len(self.protocols_ipv4), len(self.protocols_ipv6),
              ",".join(sorted(self.protocols_ipv4)), ",".join(sorted(self.protocols_ipv6))))
        
        # Reset for next bucket
        self.protocols_ipv4 = set()
        self.protocols_ipv6 = set()


def aggregate_buckets(conn: sqlite3.Connection, router: str, day_start: datetime) -> None:
    """
    Compute 30m, 1h, and 1d aggregates for a given day.
    
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
            protocols_ipv4, protocols_ipv6 = process_file(file_path)
            agg_30m.update(protocols_ipv4, protocols_ipv6)
            agg_1h.update(protocols_ipv4, protocols_ipv6)
            agg_1d.update(protocols_ipv4, protocols_ipv6)
        
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
    Process all pending files for the protocol_stats table.
    
    Args:
        conn: Database connection
        limit: Optional limit on number of files to process
        
    Returns:
        Dictionary with counts: {'processed': N, 'errors': N}
    """
    init_protocol_stats_table(conn)
    
    pending = get_files_needing_processing(conn, 'protocol_stats', limit)
    stats = {'processed': 0, 'errors': 0}
    
    print(f"Processing {len(pending)} pending files for protocol_stats...")
    
    for file_path, router, timestamp, file_exists in pending:
        success = process_file_for_stats(conn, file_path, router, timestamp, file_exists)
        mark_file_processed(conn, file_path, 'protocol_stats', success)
        
        if success:
            stats['processed'] += 1
        else:
            stats['errors'] += 1
        
        # Commit periodically
        if (stats['processed'] + stats['errors']) % 100 == 0:
            conn.commit()
            print(f"Progress: {stats['processed']} processed, {stats['errors']} errors")
    
    conn.commit()
    return stats


def main():
    """Main entry point for standalone execution."""
    print("--------------------------------")
    print(f"Protocol DB processing at {datetime.now()}")
    print("--------------------------------")
    
    with get_db_connection() as conn:
        # First sync the processed_files table
        sync_processed_files_table(conn)
        
        # Then process pending files
        stats = process_pending_files(conn)
        
        print(f"Processing complete: {stats}")


if __name__ == "__main__":
    main()
