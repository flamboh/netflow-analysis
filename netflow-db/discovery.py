"""
File discovery module for NetFlow database processing.

Handles filesystem scanning, gap detection, and processed_files table synchronization.
"""

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

from common import (
    NETFLOW_DATA_PATH,
    AVAILABLE_ROUTERS,
    DATA_START_DATE,
    construct_file_path,
    parse_file_path,
    timestamp_to_unix,
    init_processed_files_table,
)


def scan_filesystem() -> Iterator[tuple[str, str, datetime]]:
    """
    Walk the NetFlow directory tree and yield all nfcapd files.
    
    Yields:
        Tuples of (file_path, router, timestamp) for each discovered file
    """
    base_path = Path(NETFLOW_DATA_PATH)
    
    for router in AVAILABLE_ROUTERS:
        router_path = base_path / router
        if not router_path.exists():
            print(f"Warning: Router path does not exist: {router_path}")
            continue
        
        # Walk through year/month/day directories
        for year_dir in sorted(router_path.iterdir()):
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
                
            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir() or not month_dir.name.isdigit():
                    continue
                    
                for day_dir in sorted(month_dir.iterdir()):
                    if not day_dir.is_dir() or not day_dir.name.isdigit():
                        continue
                    
                    # Find all nfcapd files in this day directory
                    for file_path in sorted(day_dir.glob('nfcapd.*')):
                        try:
                            router_parsed, timestamp = parse_file_path(str(file_path))
                            # Skip files before DATA_START_DATE
                            if timestamp < DATA_START_DATE:
                                continue
                            yield str(file_path), router_parsed, timestamp
                        except ValueError as e:
                            print(f"Warning: Could not parse file {file_path}: {e}")
                            continue


def compute_data_horizon(conn: sqlite3.Connection) -> datetime:
    """
    Compute the data horizon - the latest timestamp where files exist.
    
    This is the cutoff point: gaps before this are zero-filled,
    gaps after this are considered pending/future data.
    
    Args:
        conn: Database connection with processed_files table
        
    Returns:
        The maximum timestamp found, or DATA_START_DATE if no files exist
    """
    cursor = conn.cursor()
    row = cursor.execute("""
        SELECT MAX(timestamp) FROM processed_files WHERE file_exists = 1
    """).fetchone()
    
    if row and row[0]:
        return datetime.fromtimestamp(row[0])
    
    return DATA_START_DATE


def identify_gaps(
    conn: sqlite3.Connection,
    router: str,
    start_time: datetime,
    end_time: datetime,
    interval_minutes: int = 5
) -> list[datetime]:
    """
    Identify missing timestamps for a router within a time range.
    
    Args:
        conn: Database connection
        router: Router name to check
        start_time: Start of the time range
        end_time: End of the time range (exclusive, this is the data horizon)
        interval_minutes: Expected interval between files (default 5 minutes)
        
    Returns:
        List of datetime objects representing missing timestamps
    """
    cursor = conn.cursor()
    
    # Get all existing timestamps for this router in the range
    existing = set()
    rows = cursor.execute("""
        SELECT timestamp FROM processed_files 
        WHERE router = ? AND timestamp >= ? AND timestamp < ?
    """, (router, timestamp_to_unix(start_time), timestamp_to_unix(end_time))).fetchall()
    
    for row in rows:
        existing.add(row[0])
    
    # Generate expected timestamps
    gaps = []
    current = start_time
    delta = timedelta(minutes=interval_minutes)
    
    while current < end_time:
        unix_ts = timestamp_to_unix(current)
        if unix_ts not in existing:
            gaps.append(current)
        current += delta
    
    return gaps


def sync_processed_files_table(conn: sqlite3.Connection, include_gaps: bool = True) -> dict:
    """
    Synchronize the processed_files table with the filesystem.
    
    This function:
    1. Scans the filesystem for all nfcapd files
    2. Inserts any newly discovered files into processed_files
    3. Optionally identifies and inserts gap placeholders
    
    Args:
        conn: Database connection
        include_gaps: If True, also insert gap placeholders (file_exists=0)
        
    Returns:
        Dictionary with counts: {'discovered': N, 'new_files': N, 'gaps': N}
    """
    init_processed_files_table(conn)
    cursor = conn.cursor()
    
    stats = {'discovered': 0, 'new_files': 0, 'gaps': 0}
    
    # Phase 1: Insert all discovered files
    print("Scanning filesystem for NetFlow files...")
    for file_path, router, timestamp in scan_filesystem():
        stats['discovered'] += 1
        
        # Insert or ignore if already exists
        cursor.execute("""
            INSERT OR IGNORE INTO processed_files 
            (file_path, router, timestamp, file_exists, discovered_at)
            VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
        """, (file_path, router, timestamp_to_unix(timestamp)))
        
        if cursor.rowcount > 0:
            stats['new_files'] += 1
        
        # Commit periodically
        if stats['discovered'] % 1000 == 0:
            conn.commit()
            print(f"  Scanned {stats['discovered']} files...")
    
    conn.commit()
    print(f"Discovered {stats['discovered']} files, {stats['new_files']} new")
    
    if not include_gaps:
        return stats
    
    # Phase 2: Identify and insert gap placeholders
    print("Identifying gaps in data...")
    data_horizon = compute_data_horizon(conn)
    print(f"Data horizon: {data_horizon}")
    
    for router in AVAILABLE_ROUTERS:
        # Find the earliest timestamp for this router
        row = cursor.execute("""
            SELECT MIN(timestamp) FROM processed_files 
            WHERE router = ? AND file_exists = 1
        """, (router,)).fetchone()
        
        if not row or not row[0]:
            print(f"  No files found for router {router}, skipping gap detection")
            continue
        
        router_start = datetime.fromtimestamp(row[0])
        gaps = identify_gaps(conn, router, router_start, data_horizon)
        
        for gap_timestamp in gaps:
            # Construct the expected file path for this gap
            gap_path = construct_file_path(router, gap_timestamp)
            
            cursor.execute("""
                INSERT OR IGNORE INTO processed_files 
                (file_path, router, timestamp, file_exists, discovered_at)
                VALUES (?, ?, ?, 0, CURRENT_TIMESTAMP)
            """, (gap_path, router, timestamp_to_unix(gap_timestamp)))
            
            if cursor.rowcount > 0:
                stats['gaps'] += 1
        
        print(f"  Router {router}: {len(gaps)} gaps identified, {stats['gaps']} new gap entries")
    
    conn.commit()
    print(f"Total: {stats['gaps']} gap placeholders inserted")
    
    return stats


def get_pending_files(conn: sqlite3.Connection, limit: int = None) -> list[tuple[str, str, int, bool]]:
    """
    Get files that haven't been fully processed yet.
    
    Args:
        conn: Database connection
        limit: Optional limit on number of files to return
        
    Returns:
        List of tuples: (file_path, router, timestamp, file_exists)
    """
    cursor = conn.cursor()
    
    query = """
        SELECT file_path, router, timestamp, file_exists
        FROM processed_files
        WHERE processed_at IS NULL
        ORDER BY timestamp ASC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    rows = cursor.execute(query).fetchall()
    return [(row[0], row[1], row[2], bool(row[3])) for row in rows]


def get_files_needing_processing(
    conn: sqlite3.Connection, 
    table_name: str,
    limit: int = None
) -> list[tuple[str, str, int, bool]]:
    """
    Get files that need processing for a specific table.
    
    Args:
        conn: Database connection
        table_name: One of 'flow_stats', 'ip_stats', 'protocol_stats', 
                    'spectrum_stats', 'structure_stats'
        limit: Optional limit on number of files to return
        
    Returns:
        List of tuples: (file_path, router, timestamp, file_exists)
    """
    valid_tables = ['flow_stats', 'ip_stats', 'protocol_stats', 'spectrum_stats', 'structure_stats']
    if table_name not in valid_tables:
        raise ValueError(f"Invalid table name: {table_name}. Must be one of {valid_tables}")
    
    status_column = f"{table_name}_status"
    cursor = conn.cursor()
    
    query = f"""
        SELECT file_path, router, timestamp, file_exists
        FROM processed_files
        WHERE {status_column} IS NULL
        ORDER BY timestamp ASC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    rows = cursor.execute(query).fetchall()
    return [(row[0], row[1], row[2], bool(row[3])) for row in rows]


def mark_file_processed(
    conn: sqlite3.Connection,
    file_path: str,
    table_name: str,
    success: bool
) -> None:
    """
    Mark a file as processed for a specific table.
    
    Args:
        conn: Database connection
        file_path: Path to the file
        table_name: One of 'flow_stats', 'ip_stats', 'protocol_stats', 
                    'spectrum_stats', 'structure_stats'
        success: True if processing succeeded, False if it failed
    """
    valid_tables = ['flow_stats', 'ip_stats', 'protocol_stats', 'spectrum_stats', 'structure_stats']
    if table_name not in valid_tables:
        raise ValueError(f"Invalid table name: {table_name}. Must be one of {valid_tables}")
    
    status_column = f"{table_name}_status"
    cursor = conn.cursor()
    
    cursor.execute(f"""
        UPDATE processed_files
        SET {status_column} = ?,
            processed_at = CASE 
                WHEN flow_stats_status IS NOT NULL 
                     AND ip_stats_status IS NOT NULL 
                     AND protocol_stats_status IS NOT NULL 
                     AND spectrum_stats_status IS NOT NULL 
                     AND structure_stats_status IS NOT NULL
                THEN CURRENT_TIMESTAMP
                ELSE processed_at
            END
        WHERE file_path = ?
    """, (1 if success else 0, file_path))


def update_processed_at(conn: sqlite3.Connection, file_path: str) -> None:
    """
    Update the processed_at timestamp when all tables have been processed.
    
    Args:
        conn: Database connection
        file_path: Path to the file
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE processed_files
        SET processed_at = CURRENT_TIMESTAMP
        WHERE file_path = ?
          AND flow_stats_status IS NOT NULL 
          AND ip_stats_status IS NOT NULL 
          AND protocol_stats_status IS NOT NULL 
          AND spectrum_stats_status IS NOT NULL 
          AND structure_stats_status IS NOT NULL
          AND processed_at IS NULL
    """, (file_path,))


if __name__ == "__main__":
    # Test the discovery module
    import sqlite3
    from common import DATABASE_PATH
    
    print(f"Testing discovery module...")
    print(f"NetFlow data path: {NETFLOW_DATA_PATH}")
    print(f"Available routers: {AVAILABLE_ROUTERS}")
    print(f"Database path: {DATABASE_PATH}")
    
    with sqlite3.connect(DATABASE_PATH) as conn:
        stats = sync_processed_files_table(conn)
        print(f"\nSync complete: {stats}")
        
        pending = get_pending_files(conn, limit=10)
        print(f"\nFirst 10 pending files:")
        for fp, router, ts, exists in pending:
            print(f"  {router} @ {datetime.fromtimestamp(ts)}: exists={exists}")
