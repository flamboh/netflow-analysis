"""
NetFlow basic statistics processor.

Processes nfcapd files to extract flow, packet, and byte statistics.
"""

import sqlite3
import subprocess
from datetime import datetime

from common import (
    NETFLOW_DATA_PATH,
    AVAILABLE_ROUTERS,
    DATABASE_PATH,
    get_db_connection,
    get_optional_env,
    timestamp_to_unix,
)
from discovery import (
    sync_processed_files_table,
    get_files_needing_processing,
    mark_file_processed,
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


def process_file(
    conn: sqlite3.Connection, 
    file_path: str, 
    router: str, 
    timestamp_unix: int,
    file_exists: bool = True
) -> bool:
    """
    Process a single NetFlow file and insert data into the netflow_stats table.
    
    Args:
        conn: Database connection
        file_path: Path to the nfcapd file
        router: Router name
        timestamp_unix: Unix timestamp for this file
        file_exists: If False, insert zero-valued row (gap placeholder)
        
    Returns:
        True if processing succeeded, False otherwise
    """
    if not file_exists:
        # Insert zero-valued row for gap
        try:
            conn.execute("""
                INSERT OR REPLACE INTO netflow_stats (
                    file_path, router, timestamp,
                    flows, flows_tcp, flows_udp, flows_icmp, flows_other,
                    packets, packets_tcp, packets_udp, packets_icmp, packets_other,
                    bytes, bytes_tcp, bytes_udp, bytes_icmp, bytes_other,
                    first_timestamp, last_timestamp, msec_first, msec_last, sequence_failures
                ) VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            """, (file_path, router, timestamp_unix))
            return True
        except Exception as e:
            print(f"Error inserting zero row for {file_path}: {e}")
            return False
    
    command = ["nfdump", "-I", "-r", file_path]
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            data = parse_nfdump_output(result.stdout)
            
            conn.execute("""
                INSERT OR REPLACE INTO netflow_stats (
                    file_path, router, timestamp,
                    flows, flows_tcp, flows_udp, flows_icmp, flows_other,
                    packets, packets_tcp, packets_udp, packets_icmp, packets_other,
                    bytes, bytes_tcp, bytes_udp, bytes_icmp, bytes_other,
                    first_timestamp, last_timestamp, msec_first, msec_last, sequence_failures
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                file_path, router, timestamp_unix,
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
            
            print(f"[flow_stats] Processed {file_path}: {data.get('flows', 0)} flows")
            return True
        else:
            print(f"Error processing {file_path}: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"Timeout processing {file_path}")
        return False
    except Exception as e:
        print(f"Exception processing {file_path}: {e}")
        return False


def process_pending_files(conn: sqlite3.Connection, limit: int = None) -> dict:
    """
    Process all pending files for the netflow_stats table.
    
    Args:
        conn: Database connection
        limit: Optional limit on number of files to process
        
    Returns:
        Dictionary with counts: {'processed': N, 'errors': N}
    """
    init_netflow_stats_table(conn)
    
    pending = get_files_needing_processing(conn, 'flow_stats', limit)
    stats = {'processed': 0, 'errors': 0}
    
    print(f"Processing {len(pending)} pending files for flow_stats...")
    
    for file_path, router, timestamp, file_exists in pending:
        success = process_file(conn, file_path, router, timestamp, file_exists)
        mark_file_processed(conn, file_path, 'flow_stats', success)
        
        if success:
            stats['processed'] += 1
        else:
            stats['errors'] += 1
        
        # Commit periodically
        if (stats['processed'] + stats['errors']) % 100 == 0:
            conn.commit()
            print(f"[flow_stats] Progress: {stats['processed']} processed, {stats['errors']} errors")
    
    conn.commit()
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
