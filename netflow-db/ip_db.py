"""
IP statistics processor.

Processes nfcapd files to extract unique IP address counts at various granularities.
Uses day-parallel processing with WAL-enabled direct DB writes.
"""

import sqlite3
import subprocess
import ipaddress
from datetime import datetime, timedelta
from multiprocessing import Pool
from typing import Optional
from collections import defaultdict

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


def process_file(file_info: tuple) -> dict:
    """
    Process a single file and return results with IP address objects.
    
    Args:
        file_info: Tuple of (file_path, router, timestamp, file_exists)
        
    Returns:
        Dict with file_path, success, data, and raw_ips (as ipaddress objects)
    """
    file_path, router, timestamp_unix, file_exists = file_info
    
    result = {
        'file_path': file_path,
        'router': router,
        'timestamp': timestamp_unix,
        'success': False,
        'data': None,
        'raw_ips': None,
        'error': None
    }
    
    if not file_exists:
        result['success'] = True
        result['data'] = {
            'sa_ipv4_count': 0, 'da_ipv4_count': 0,
            'sa_ipv6_count': 0, 'da_ipv6_count': 0
        }
        result['raw_ips'] = {'sa_v4': [], 'da_v4': [], 'sa_v6': [], 'da_v6': []}
        return result
    
    print(f"[ip_stats] Processing {file_path}")
    
    sa_v4: set[ipaddress.IPv4Address] = set()
    da_v4: set[ipaddress.IPv4Address] = set()
    sa_v6: set[ipaddress.IPv6Address] = set()
    da_v6: set[ipaddress.IPv6Address] = set()
    
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv4"]
    ipv6_command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "-n", "0", "ipv6", "-6"]
    
    try:
        ipv4_result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        ipv6_result = subprocess.run(ipv6_command, capture_output=True, text=True, timeout=300)

        if ipv4_result.returncode == 0:
            for line in ipv4_result.stdout.strip().split("\n"):
                if line.strip() and ',' in line:
                    try:
                        source_ip, dest_ip = line.strip().split(",")
                        sa_v4.add(ipaddress.IPv4Address(source_ip))
                        da_v4.add(ipaddress.IPv4Address(dest_ip))
                    except (ValueError, ipaddress.AddressValueError):
                        continue
                        
        if ipv6_result.returncode == 0:
            for line in ipv6_result.stdout.strip().split("\n"):
                if line.strip() and ',' in line:
                    try:
                        source_ip, dest_ip = line.strip().split(",")
                        sa_v6.add(ipaddress.IPv6Address(source_ip))
                        da_v6.add(ipaddress.IPv6Address(dest_ip))
                    except (ValueError, ipaddress.AddressValueError):
                        continue
        
        result['success'] = True
        result['data'] = {
            'sa_ipv4_count': len(sa_v4),
            'da_ipv4_count': len(da_v4),
            'sa_ipv6_count': len(sa_v6),
            'da_ipv6_count': len(da_v6)
        }
        # Store as lists of ipaddress objects (picklable)
        result['raw_ips'] = {
            'sa_v4': list(sa_v4),
            'da_v4': list(da_v4),
            'sa_v6': list(sa_v6),
            'da_v6': list(da_v6)
        }
        
    except subprocess.TimeoutExpired:
        result['error'] = "Timeout"
        print(f"[ip_stats] Timeout processing {file_path}")
    except Exception as e:
        result['error'] = str(e)
        print(f"[ip_stats] Error processing {file_path}: {e}")

    return result


def compute_aggregates(results: list[dict], router: str, day_start: int) -> list[dict]:
    """
    Compute 30m, 1h, and 1d aggregates from 5m results for a single day.
    """
    aggregates = []
    
    buckets = {
        '30m': defaultdict(lambda: {'sa_v4': set(), 'da_v4': set(), 'sa_v6': set(), 'da_v6': set()}),
        '1h': defaultdict(lambda: {'sa_v4': set(), 'da_v4': set(), 'sa_v6': set(), 'da_v6': set()}),
        '1d': defaultdict(lambda: {'sa_v4': set(), 'da_v4': set(), 'sa_v6': set(), 'da_v6': set()}),
    }
    
    for result in results:
        if not result['success'] or result['raw_ips'] is None:
            continue
        
        timestamp = result['timestamp']
        raw = result['raw_ips']
        
        dt = unix_to_timestamp(timestamp)
        
        bucket_30m = dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)
        bucket_30m_ts = timestamp_to_unix(bucket_30m)
        
        bucket_1h = dt.replace(minute=0, second=0, microsecond=0)
        bucket_1h_ts = timestamp_to_unix(bucket_1h)
        
        bucket_1d_ts = day_start
        
        for granularity, bucket_ts in [('30m', bucket_30m_ts), ('1h', bucket_1h_ts), ('1d', bucket_1d_ts)]:
            bucket = buckets[granularity][bucket_ts]
            bucket['sa_v4'].update(raw['sa_v4'])
            bucket['da_v4'].update(raw['da_v4'])
            bucket['sa_v6'].update(raw['sa_v6'])
            bucket['da_v6'].update(raw['da_v6'])
    
    durations = {'30m': 1800, '1h': 3600, '1d': 86400}
    
    for granularity in ['30m', '1h', '1d']:
        for bucket_start, data in buckets[granularity].items():
            aggregates.append({
                'router': router,
                'granularity': granularity,
                'bucket_start': bucket_start,
                'bucket_end': bucket_start + durations[granularity],
                'sa_ipv4_count': len(data['sa_v4']),
                'da_ipv4_count': len(data['da_v4']),
                'sa_ipv6_count': len(data['sa_v6']),
                'da_ipv6_count': len(data['da_v6']),
            })
    
    return aggregates


def insert_results(conn: sqlite3.Connection, results: list[dict], aggregates: list[dict]) -> tuple[int, int]:
    """Insert 5m results and aggregate results into the database."""
    cursor = conn.cursor()
    inserted_5m = 0
    inserted_agg = 0
    
    for result in results:
        if not result['success'] or result['data'] is None:
            continue
        
        data = result['data']
        bucket_start = result['timestamp']
        bucket_end = bucket_start + 300
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO ip_stats 
                (router, granularity, bucket_start, bucket_end, 
                 sa_ipv4_count, da_ipv4_count, sa_ipv6_count, da_ipv6_count)
                VALUES (?, '5m', ?, ?, ?, ?, ?, ?)
            """, (result['router'], bucket_start, bucket_end,
                  data['sa_ipv4_count'], data['da_ipv4_count'],
                  data['sa_ipv6_count'], data['da_ipv6_count']))
            inserted_5m += 1
        except Exception as e:
            print(f"[ip_stats] Error inserting {result['file_path']}: {e}")
    
    for agg in aggregates:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO ip_stats 
                (router, granularity, bucket_start, bucket_end, 
                 sa_ipv4_count, da_ipv4_count, sa_ipv6_count, da_ipv6_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (agg['router'], agg['granularity'], agg['bucket_start'], agg['bucket_end'],
                  agg['sa_ipv4_count'], agg['da_ipv4_count'],
                  agg['sa_ipv6_count'], agg['da_ipv6_count']))
            inserted_agg += 1
        except Exception as e:
            print(f"[ip_stats] Error inserting aggregate: {e}")
    
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
    
    print(f"[ip_stats] Worker starting {router} {day_dt.strftime('%Y-%m-%d')} ({len(day_files)} files)")
    
    # Process all files for this day sequentially within the worker
    results = [process_file(f) for f in day_files]
    
    # Compute aggregates from accumulated data
    aggregates = compute_aggregates(results, router, day_start)
    
    # Each worker opens its own connection (WAL allows concurrent writes)
    with get_db_connection() as conn:
        init_ip_stats_table(conn)
        
        # Write directly to DB
        inserted_5m, inserted_agg = insert_results(conn, results, aggregates)
        
        # Mark files as processed
        batch_mark_processed(conn, 'ip_stats', results)
    
    errors = len([r for r in results if not r['success']])
    
    print(f"[ip_stats] Worker complete {router} {day_dt.strftime('%Y-%m-%d')}: "
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
    Process all pending files for the ip_stats table.
    Uses day-parallel processing where each worker handles a complete day.
    """
    init_ip_stats_table(conn)
    
    pending = get_files_needing_processing(conn, 'ip_stats', limit)
    stats = {'processed': 0, 'errors': 0, 'aggregates': 0, 'days': 0}
    
    if not pending:
        print("[ip_stats] No pending files to process")
        return stats
    
    # Group by day
    days = group_files_by_day(pending)
    print(f"[ip_stats] Processing {len(pending)} files across {len(days)} complete days...")
    
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
    print(f"IP DB processing at {datetime.now()}")
    print("--------------------------------")
    
    with get_db_connection() as conn:
        sync_processed_files_table(conn)
        stats = process_pending_files(conn)
        print(f"Processing complete: {stats}")


if __name__ == "__main__":
    main()
