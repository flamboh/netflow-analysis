"""
NetFlow basic statistics processor.

Processes nfcapd files to extract flow, packet, and byte statistics.
"""

import sqlite3
import subprocess
from datetime import datetime
from multiprocessing import Pool

from common import (
    NETFLOW_DATA_PATH,
    AVAILABLE_ROUTERS,
    DATABASE_PATH,
    MAX_WORKERS,
    BATCH_SIZE,
    get_db_connection,
    get_optional_env,
    timestamp_to_unix,
)
from discovery import (
    sync_processed_files_table,
    get_files_needing_processing,
    batch_mark_processed,
    handle_stale_days,
)

FIRST_RUN = get_optional_env('FIRST_RUN', 'False').lower() in ('true', '1', 'yes')


def init_netflow_stats_table(conn: sqlite3.Connection) -> None:
    """Create the netflow_stats table if it doesn't exist."""
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS netflow_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path TEXT NOT NULL UNIQUE,
        router TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        
        flows INTEGER NOT NULL,
        flows_tcp INTEGER NOT NULL,
        flows_udp INTEGER NOT NULL,
        flows_icmp INTEGER NOT NULL,
        flows_other INTEGER NOT NULL,
        
        packets INTEGER NOT NULL,
        packets_tcp INTEGER NOT NULL,
        packets_udp INTEGER NOT NULL,
        packets_icmp INTEGER NOT NULL,
        packets_other INTEGER NOT NULL,
        
        bytes INTEGER NOT NULL,
        bytes_tcp INTEGER NOT NULL,
        bytes_udp INTEGER NOT NULL,
        bytes_icmp INTEGER NOT NULL,
        bytes_other INTEGER NOT NULL,
        
        first_timestamp INTEGER NOT NULL,
        last_timestamp INTEGER NOT NULL,
        msec_first INTEGER NOT NULL,
        msec_last INTEGER NOT NULL,
        
        sequence_failures INTEGER NOT NULL,
        
        processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_router_timestamp ON netflow_stats (router, timestamp)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_file_path ON netflow_stats (file_path)
    """)
    
    conn.commit()


def parse_nfdump_output(output: str) -> dict:
    """Parse nfdump -I output and return a dictionary of values."""
    data = {}
    for line in output.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip().lower()
            try:
                data[key] = int(value.strip())
            except ValueError:
                data[key] = value.strip()
    return data


def process_file_worker(task: tuple) -> dict:
    """
    Worker function to process a single file (no DB access).
    
    Args:
        task: Tuple of (file_path, router, timestamp, file_exists)
        
    Returns:
        Dict with file_path, success, and data fields
    """
    file_path, router, timestamp_unix, file_exists = task
    
    result = {
        'file_path': file_path,
        'router': router,
        'timestamp': timestamp_unix,
        'success': False,
        'data': None,
        'error': None
    }
    
    if not file_exists:
        # Gap placeholder - return zeros
        result['success'] = True
        result['data'] = {
            'flows': 0, 'flows_tcp': 0, 'flows_udp': 0, 'flows_icmp': 0, 'flows_other': 0,
            'packets': 0, 'packets_tcp': 0, 'packets_udp': 0, 'packets_icmp': 0, 'packets_other': 0,
            'bytes': 0, 'bytes_tcp': 0, 'bytes_udp': 0, 'bytes_icmp': 0, 'bytes_other': 0,
            'first': 0, 'last': 0, 'msec_first': 0, 'msec_last': 0, 'sequence_failures': 0
        }
        return result
    
    command = ["nfdump", "-I", "-r", file_path]
    
    try:
        proc_result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        
        if proc_result.returncode == 0:
            data = parse_nfdump_output(proc_result.stdout)
            result['success'] = True
            result['data'] = data
            print(f"[flow_stats] Processed {file_path}: {data.get('flows', 0)} flows")
        else:
            result['error'] = proc_result.stderr
            print(f"[flow_stats] Error processing {file_path}: {proc_result.stderr}")
            
    except subprocess.TimeoutExpired:
        result['error'] = "Timeout"
        print(f"[flow_stats] Timeout processing {file_path}")
    except Exception as e:
        result['error'] = str(e)
        print(f"[flow_stats] Exception processing {file_path}: {e}")
    
    return result


def batch_insert_results(conn: sqlite3.Connection, results: list[dict]) -> int:
    """
    Batch insert processing results into the database.
    
    Args:
        conn: Database connection
        results: List of result dicts from process_file_worker
        
    Returns:
        Number of successfully inserted rows
    """
    cursor = conn.cursor()
    inserted = 0
    
    for result in results:
        if not result['success'] or result['data'] is None:
            continue
        
        data = result['data']
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO netflow_stats (
                    file_path, router, timestamp,
                    flows, flows_tcp, flows_udp, flows_icmp, flows_other,
                    packets, packets_tcp, packets_udp, packets_icmp, packets_other,
                    bytes, bytes_tcp, bytes_udp, bytes_icmp, bytes_other,
                    first_timestamp, last_timestamp, msec_first, msec_last, sequence_failures
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result['file_path'], result['router'], result['timestamp'],
                data.get('flows', 0),
                data.get('flows_tcp', 0),
                data.get('flows_udp', 0),
                data.get('flows_icmp', 0),
                data.get('flows_other', 0),
                data.get('packets', 0),
                data.get('packets_tcp', 0),
                data.get('packets_udp', 0),
                data.get('packets_icmp', 0),
                data.get('packets_other', 0),
                data.get('bytes', 0),
                data.get('bytes_tcp', 0),
                data.get('bytes_udp', 0),
                data.get('bytes_icmp', 0),
                data.get('bytes_other', 0),
                data.get('first', 0),
                data.get('last', 0),
                data.get('msec_first', 0),
                data.get('msec_last', 0),
                data.get('sequence_failures', 0)
            ))
            inserted += 1
        except Exception as e:
            print(f"[flow_stats] Error inserting {result['file_path']}: {e}")
    
    conn.commit()
    return inserted


def process_pending_files(conn: sqlite3.Connection, limit: int = None) -> dict:
    """
    Process all pending files for the netflow_stats table using parallel processing.
    
    Args:
        conn: Database connection
        limit: Optional limit on number of files to process
        
    Returns:
        Dictionary with counts: {'processed': N, 'errors': N}
    """
    init_netflow_stats_table(conn)
    
    # Handle stale days - reset days that have new files mixed with already-processed files
    # (flow_stats has no aggregates, but we reset for consistency)
    handle_stale_days(conn, 'flow_stats')
    
    pending = get_files_needing_processing(conn, 'flow_stats', limit)
    stats = {'processed': 0, 'errors': 0}
    
    if not pending:
        print("[flow_stats] No pending files to process")
        return stats
    
    print(f"[flow_stats] Processing {len(pending)} pending files with {MAX_WORKERS} workers...")
    
    # Process in batches
    for i in range(0, len(pending), BATCH_SIZE):
        batch = pending[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(pending) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"[flow_stats] Processing batch {batch_num}/{total_batches} ({len(batch)} files)")
        
        # Process batch in parallel
        with Pool(processes=MAX_WORKERS) as pool:
            results = pool.map(process_file_worker, batch)
        
        # Batch insert results
        inserted = batch_insert_results(conn, results)
        
        # Batch update processed_files status
        batch_mark_processed(conn, 'flow_stats', results)
        
        # Update stats
        batch_errors = len([r for r in results if not r['success']])
        stats['processed'] += inserted
        stats['errors'] += batch_errors
        
        print(f"[flow_stats] Batch complete: {inserted} inserted, {batch_errors} errors")
    
    return stats


def main():
    """Main entry point for standalone execution."""
    print("--------------------------------")
    print(f"Flow DB processing at {datetime.now()}")
    print("--------------------------------")
    
    with get_db_connection() as conn:
        # First sync the processed_files table
        sync_processed_files_table(conn)
        
        # Then process pending files
        stats = process_pending_files(conn)
        
        print(f"Processing complete: {stats}")


if __name__ == "__main__":
    main()
