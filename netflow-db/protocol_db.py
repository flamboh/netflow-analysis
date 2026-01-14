"""
Protocol statistics processor.

Processes nfcapd files to extract unique protocol counts at various granularities.
"""

import sqlite3
import subprocess
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


def process_file_worker(task: tuple) -> dict:
    """
    Worker function to process a single file (no DB access).
    """
    file_path, router, timestamp_unix, file_exists = task
    
    result = {
        'file_path': file_path,
        'router': router,
        'timestamp': timestamp_unix,
        'success': False,
        'data': None,
        'raw_protocols': None,
        'error': None
    }
    
    if not file_exists:
        result['success'] = True
        result['data'] = {'protocols_ipv4': [], 'protocols_ipv6': []}
        result['raw_protocols'] = {'ipv4': [], 'ipv6': []}
        return result
    
    print(f"[protocol_stats] Processing {file_path}")
    
    protocols_ipv4 = set()
    protocols_ipv6 = set()
    
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%pr", "-A", "proto"]
    
    try:
        ipv4_result = subprocess.run(command + ["ipv4", "-N"], capture_output=True, text=True, timeout=300)
        ipv6_result = subprocess.run(command + ["ipv6", "-N"], capture_output=True, text=True, timeout=300)

        if ipv4_result.returncode == 0:
            for line in ipv4_result.stdout.strip().split("\n"):
                if line.strip():
                    protocols_ipv4.add(line.strip())
                    
        if ipv6_result.returncode == 0:
            for line in ipv6_result.stdout.strip().split("\n"):
                if line.strip():
                    protocols_ipv6.add(line.strip())
        
        result['success'] = True
        result['data'] = {
            'protocols_ipv4': list(protocols_ipv4),
            'protocols_ipv6': list(protocols_ipv6)
        }
        result['raw_protocols'] = {
            'ipv4': list(protocols_ipv4),
            'ipv6': list(protocols_ipv6)
        }
        
    except subprocess.TimeoutExpired:
        result['error'] = "Timeout"
        print(f"[protocol_stats] Timeout processing {file_path}")
    except Exception as e:
        result['error'] = str(e)
        print(f"[protocol_stats] Error processing {file_path}: {e}")
    
    return result


def compute_aggregates(results: list[dict], router: str, day_start: int) -> list[dict]:
    """
    Compute 30m, 1h, and 1d aggregates from 5m results for a single day.
    """
    aggregates = []
    
    buckets = {
        '30m': defaultdict(lambda: {'ipv4': set(), 'ipv6': set()}),
        '1h': defaultdict(lambda: {'ipv4': set(), 'ipv6': set()}),
        '1d': defaultdict(lambda: {'ipv4': set(), 'ipv6': set()}),
    }
    
    for result in results:
        if not result['success'] or result['raw_protocols'] is None:
            continue
        
        timestamp = result['timestamp']
        raw = result['raw_protocols']
        
        dt = unix_to_timestamp(timestamp)
        
        bucket_30m = dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)
        bucket_30m_ts = timestamp_to_unix(bucket_30m)
        
        bucket_1h = dt.replace(minute=0, second=0, microsecond=0)
        bucket_1h_ts = timestamp_to_unix(bucket_1h)
        
        bucket_1d_ts = day_start
        
        for granularity, bucket_ts in [('30m', bucket_30m_ts), ('1h', bucket_1h_ts), ('1d', bucket_1d_ts)]:
            bucket = buckets[granularity][bucket_ts]
            bucket['ipv4'].update(raw['ipv4'])
            bucket['ipv6'].update(raw['ipv6'])
    
    durations = {'30m': 1800, '1h': 3600, '1d': 86400}
    
    for granularity in ['30m', '1h', '1d']:
        for bucket_start, data in buckets[granularity].items():
            aggregates.append({
                'router': router,
                'granularity': granularity,
                'bucket_start': bucket_start,
                'bucket_end': bucket_start + durations[granularity],
                'protocols_ipv4': data['ipv4'],
                'protocols_ipv6': data['ipv6'],
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
                INSERT OR REPLACE INTO protocol_stats 
                (router, granularity, bucket_start, bucket_end, 
                 unique_protocols_count_ipv4, unique_protocols_count_ipv6,
                 protocols_list_ipv4, protocols_list_ipv6)
                VALUES (?, '5m', ?, ?, ?, ?, ?, ?)
            """, (result['router'], bucket_start, bucket_end,
                  len(data['protocols_ipv4']), len(data['protocols_ipv6']),
                  ",".join(sorted(data['protocols_ipv4'])),
                  ",".join(sorted(data['protocols_ipv6']))))
            inserted_5m += 1
        except Exception as e:
            print(f"[protocol_stats] Error inserting {result['file_path']}: {e}")
    
    for agg in aggregates:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO protocol_stats 
                (router, granularity, bucket_start, bucket_end, 
                 unique_protocols_count_ipv4, unique_protocols_count_ipv6,
                 protocols_list_ipv4, protocols_list_ipv6)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (agg['router'], agg['granularity'], agg['bucket_start'], agg['bucket_end'],
                  len(agg['protocols_ipv4']), len(agg['protocols_ipv6']),
                  ",".join(sorted(agg['protocols_ipv4'])),
                  ",".join(sorted(agg['protocols_ipv6']))))
            inserted_agg += 1
        except Exception as e:
            print(f"[protocol_stats] Error inserting aggregate: {e}")
    
    conn.commit()
    return inserted_5m, inserted_agg


def process_pending_files(conn: sqlite3.Connection, limit: int = None) -> dict:
    """
    Process all pending files for the protocol_stats table using parallel processing.
    Processes complete days only, computing aggregates after each day.
    """
    init_protocol_stats_table(conn)
    
    pending = get_files_needing_processing(conn, 'protocol_stats', limit)
    stats = {'processed': 0, 'errors': 0, 'aggregates': 0, 'days': 0}
    
    if not pending:
        print("[protocol_stats] No pending files to process")
        return stats
    
    days = group_files_by_day(pending)
    print(f"[protocol_stats] Processing {len(pending)} files across {len(days)} complete days with {MAX_WORKERS} workers...")
    
    for (router, day_start), day_files in sorted(days.items()):
        day_dt = unix_to_timestamp(day_start)
        print(f"[protocol_stats] Processing {router} {day_dt.strftime('%Y-%m-%d')} ({len(day_files)} files)")
        
        with Pool(processes=MAX_WORKERS) as pool:
            results = pool.map(process_file_worker, day_files)
        
        aggregates = compute_aggregates(results, router, day_start)
        inserted_5m, inserted_agg = insert_results(conn, results, aggregates)
        batch_mark_processed(conn, 'protocol_stats', results)
        
        errors = len([r for r in results if not r['success']])
        stats['processed'] += inserted_5m
        stats['aggregates'] += inserted_agg
        stats['errors'] += errors
        stats['days'] += 1
        
        print(f"[protocol_stats] Day complete: {inserted_5m} 5m records, {inserted_agg} aggregates, {errors} errors")
    
    return stats


def main():
    """Main entry point for standalone execution."""
    print("--------------------------------")
    print(f"Protocol DB processing at {datetime.now()}")
    print("--------------------------------")
    
    with get_db_connection() as conn:
        sync_processed_files_table(conn)
        stats = process_pending_files(conn)
        print(f"Processing complete: {stats}")


if __name__ == "__main__":
    main()
