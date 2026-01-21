"""
Structure function statistics processor.

Computes IP structure function from unique source IPs at various granularities.
Uses day-parallel processing with WAL-enabled direct DB writes.
"""

import sqlite3
import subprocess
import ipaddress
from datetime import datetime, timedelta
from multiprocessing import Pool
from typing import Optional
from collections import defaultdict
import os
import json
import tempfile

from common import (
    NETFLOW_DATA_PATH,
    AVAILABLE_ROUTERS,
    DATABASE_PATH,
    MAX_WORKERS,
    BATCH_SIZE,
    get_db_connection,
    get_optional_env,
    construct_file_path,
    timestamp_to_unix,
    unix_to_timestamp,
)
from discovery import (
    sync_processed_files_table,
    get_files_needing_processing,
    group_files_by_day,
    batch_mark_processed,
    handle_stale_days,
)

MAAD_PATH = get_optional_env('MAAD_PATH', '/home/obo/oliver/netflow-analysis/maad')


def init_structure_stats_table(conn: sqlite3.Connection) -> None:
    """Create the structure_stats table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS structure_stats (
            router TEXT NOT NULL,
            granularity TEXT NOT NULL CHECK (granularity IN ('5m', '30m', '1h', '1d')),
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            ip_version INTEGER NOT NULL CHECK (ip_version IN (4, 6)),
            structure_json_sa TEXT NOT NULL,
            structure_json_da TEXT NOT NULL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (router, granularity, bucket_start, ip_version)
        ) WITHOUT ROWID;
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_structure_granularity_time
        ON structure_stats(granularity, bucket_start);
    """)
    conn.commit()


def extract_ips(file_path: str) -> tuple[set, set]:
    """
    Extract unique source and destination IPv4 addresses from a netflow file.
    
    Returns:
        Tuple of (source_ips, dest_ips) as sets of IPv4Address objects
    """
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv4"]
    
    source_ips: set = set()
    dest_ips: set = set()
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if not line or "," not in line:
                    continue
                parts = line.split(",")
                if len(parts) >= 2:
                    try:
                        source_ips.add(ipaddress.IPv4Address(parts[0].strip()))
                    except (ipaddress.AddressValueError, ValueError):
                        pass
                    try:
                        dest_ips.add(ipaddress.IPv4Address(parts[1].strip()))
                    except (ipaddress.AddressValueError, ValueError):
                        pass
    except Exception:
        pass
    
    return source_ips, dest_ips


def compute_structure_function(ips: set) -> list:
    """
    Compute structure function using MAAD StructureFunction binary.
    
    Returns:
        List of dicts with 'q', 'tau', and 'sd' keys (matching StructureFunctionPoint interface)
    """
    if not ips or len(ips) < 10:
        return []
    
    structure_path = os.path.join(MAAD_PATH, "StructureFunction")
    if not os.path.exists(structure_path):
        return []
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("ip\n")
        for ip in ips:
            f.write(f"{ip}\n")
        temp_path = f.name
    
    try:
        result = subprocess.run(
            [structure_path, "10", temp_path],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            structure = []
            for line in result.stdout.strip().split("\n"):
                parts = line.strip().split(",")
                if len(parts) >= 3:
                    try:
                        structure.append({
                            "q": float(parts[0]),
                            "tau": float(parts[1]),
                            "sd": float(parts[2])
                        })
                    except ValueError:
                        continue
            return structure
    except Exception:
        pass
    finally:
        os.unlink(temp_path)
    
    return []


def process_file(file_info: tuple) -> dict:
    """
    Process a single file and return results for IPv4.
    
    Note: IPv6 support is not yet available in MAAD binaries.
    
    Args:
        file_info: Tuple of (file_path, router, timestamp, file_exists)
        
    Returns:
        Dict with file_path, success, and raw_ips for source/dest
    """
    file_path, router, timestamp_unix, file_exists = file_info
    
    result = {
        'file_path': file_path,
        'router': router,
        'timestamp': timestamp_unix,
        'success': False,
        'raw_ips_sa': set(),  # Source IPs (IPv4)
        'raw_ips_da': set(),  # Dest IPs (IPv4)
        'error': None
    }
    
    if not file_exists:
        result['success'] = True
        return result
    
    print(f"[structure_stats] Processing {file_path}")
    
    try:
        # Extract IPv4 source and dest (IPv6 not yet supported by MAAD)
        sa_v4, da_v4 = extract_ips(file_path)
        
        result['success'] = True
        result['raw_ips_sa'] = sa_v4
        result['raw_ips_da'] = da_v4
        
    except Exception as e:
        result['error'] = str(e)
        print(f"[structure_stats] Error processing {file_path}: {e}")
    
    return result


def compute_aggregates(results: list[dict], router: str, day_start: int) -> list[dict]:
    """
    Compute 30m, 1h, and 1d aggregates from 5m results for a single day.
    Returns results for IPv4 with separate source/dest structure functions.
    """
    aggregates = []
    
    # Buckets keyed by (granularity, bucket_ts)
    buckets_sa: dict[tuple, set] = defaultdict(set)
    buckets_da: dict[tuple, set] = defaultdict(set)
    
    for result in results:
        if not result['success']:
            continue
        
        timestamp = result['timestamp']
        dt = unix_to_timestamp(timestamp)
        
        bucket_30m = dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)
        bucket_30m_ts = timestamp_to_unix(bucket_30m)
        
        bucket_1h = dt.replace(minute=0, second=0, microsecond=0)
        bucket_1h_ts = timestamp_to_unix(bucket_1h)
        
        bucket_1d_ts = day_start
        
        for granularity, bucket_ts in [('30m', bucket_30m_ts), ('1h', bucket_1h_ts), ('1d', bucket_1d_ts)]:
            buckets_sa[(granularity, bucket_ts)].update(result['raw_ips_sa'])
            buckets_da[(granularity, bucket_ts)].update(result['raw_ips_da'])
    
    durations = {'30m': 1800, '1h': 3600, '1d': 86400}
    
    # Compute structure functions for each bucket
    for (granularity, bucket_ts), sa_ips in buckets_sa.items():
        da_ips = buckets_da.get((granularity, bucket_ts), set())
        
        structure_sa = compute_structure_function(sa_ips)
        structure_da = compute_structure_function(da_ips)
        
        aggregates.append({
            'router': router,
            'granularity': granularity,
            'bucket_start': bucket_ts,
            'bucket_end': bucket_ts + durations[granularity],
            'ip_version': 4,  # IPv4 only for now
            'structure_sa': structure_sa,
            'structure_da': structure_da,
        })
    
    return aggregates


def insert_results(conn: sqlite3.Connection, results: list[dict], aggregates: list[dict]) -> tuple[int, int]:
    """Insert 5m results and aggregate results into the database."""
    cursor = conn.cursor()
    inserted_5m = 0
    inserted_agg = 0
    
    # Process 5m results - IPv4 only for now
    for result in results:
        if not result['success']:
            continue
        
        bucket_start = result['timestamp']
        bucket_end = bucket_start + 300
        
        structure_sa = compute_structure_function(result['raw_ips_sa'])
        structure_da = compute_structure_function(result['raw_ips_da'])
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO structure_stats 
                (router, granularity, bucket_start, bucket_end, ip_version, structure_json_sa, structure_json_da)
                VALUES (?, '5m', ?, ?, 4, ?, ?)
            """, (result['router'], bucket_start, bucket_end,
                  json.dumps(structure_sa), json.dumps(structure_da)))
            inserted_5m += 1
        except Exception as e:
            print(f"[structure_stats] Error inserting {result['file_path']}: {e}")
    
    for agg in aggregates:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO structure_stats 
                (router, granularity, bucket_start, bucket_end, ip_version, structure_json_sa, structure_json_da)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (agg['router'], agg['granularity'], agg['bucket_start'], agg['bucket_end'],
                  agg['ip_version'], json.dumps(agg['structure_sa']), json.dumps(agg['structure_da'])))
            inserted_agg += 1
        except Exception as e:
            print(f"[structure_stats] Error inserting aggregate: {e}")
    
    conn.commit()
    return inserted_5m, inserted_agg


def process_day_worker(day_info: tuple) -> dict:
    """
    Process a complete day - runs in separate process with own DB connection.
    
    Args:
        day_info: Tuple of (router, day_start, day_files)
        
    Returns:
        Dict with processing statistics
    """
    router, day_start, day_files = day_info
    day_dt = unix_to_timestamp(day_start)
    
    print(f"[structure_stats] Worker starting {router} {day_dt.strftime('%Y-%m-%d')} ({len(day_files)} files)")
    
    # Process all files for this day sequentially within the worker
    results = [process_file(f) for f in day_files]
    
    # Compute aggregates from accumulated data
    aggregates = compute_aggregates(results, router, day_start)
    
    # Each worker opens its own connection (WAL allows concurrent writes)
    with get_db_connection() as conn:
        init_structure_stats_table(conn)
        
        # Write directly to DB
        inserted_5m, inserted_agg = insert_results(conn, results, aggregates)
        
        # Mark files as processed
        batch_mark_processed(conn, 'structure_stats', results)
    
    errors = len([r for r in results if not r['success']])
    
    print(f"[structure_stats] Worker complete {router} {day_dt.strftime('%Y-%m-%d')}: "
          f"{inserted_5m} 5m, {inserted_agg} agg, {errors} errors")
    
    return {
        'router': router,
        'day': day_start,
        'processed': inserted_5m,
        'aggregates': inserted_agg,
        'errors': errors
    }


def process_pending_files(conn: sqlite3.Connection, limit: int = None) -> dict:
    """
    Process all pending files for the structure_stats table.
    Uses day-parallel processing where each worker handles a complete day.
    """
    init_structure_stats_table(conn)
    
    # Handle stale days - reset days that have new files mixed with already-processed files
    handle_stale_days(conn, 'structure_stats')
    
    pending = get_files_needing_processing(conn, 'structure_stats', limit)
    stats = {'processed': 0, 'errors': 0, 'aggregates': 0, 'days': 0}
    
    if not pending:
        print("[structure_stats] No pending files to process")
        return stats
    
    # Group by day
    days = group_files_by_day(pending)
    print(f"[structure_stats] Processing {len(pending)} files across {len(days)} complete days...")
    
    # Prepare day tasks: (router, day_start, files_list)
    day_tasks = [(router, day_start, files) 
                 for (router, day_start), files in sorted(days.items())]
    
    # Process days in parallel - each worker writes via own connection
    with Pool(processes=MAX_WORKERS) as pool:
        results = pool.map(process_day_worker, day_tasks)
    
    # Aggregate stats from all workers
    for result in results:
        stats['processed'] += result['processed']
        stats['aggregates'] += result['aggregates']
        stats['errors'] += result['errors']
        stats['days'] += 1
    
    return stats


def main():
    """Main entry point for standalone execution."""
    print("--------------------------------")
    print(f"Structure DB processing at {datetime.now()}")
    print("--------------------------------")
    
    with get_db_connection() as conn:
        sync_processed_files_table(conn)
        stats = process_pending_files(conn)
        print(f"Processing complete: {stats}")


if __name__ == "__main__":
    main()
