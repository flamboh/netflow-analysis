"""
Spectrum statistics processor.

Computes IP singularity spectrum from unique source IPs at various granularities.
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
)

MAAD_PATH = get_optional_env('MAAD_PATH', '/home/obo/oliver/netflow-analysis/maad')


def init_spectrum_stats_table(conn: sqlite3.Connection) -> None:
    """Create the spectrum_stats table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS spectrum_stats (
            router TEXT NOT NULL,
            granularity TEXT NOT NULL CHECK (granularity IN ('5m','30m','1h','1d')),
            bucket_start INTEGER NOT NULL,
            bucket_end   INTEGER NOT NULL,
            spectrum_data TEXT NOT NULL DEFAULT '{}',
            ip_count INTEGER NOT NULL DEFAULT 0,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (router, granularity, bucket_start)
        ) WITHOUT ROWID;
    """)
    conn.commit()


def extract_ips(file_path: str) -> set[ipaddress.IPv4Address]:
    """Extract unique source IPv4 addresses from a netflow file."""
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa", "-n", "0", "ipv4"]
    
    ips: set[ipaddress.IPv4Address] = set()
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if line.strip():
                    try:
                        ips.add(ipaddress.IPv4Address(line.strip()))
                    except ipaddress.AddressValueError:
                        continue
    except Exception:
        pass
    
    return ips


def compute_spectrum(ips: set[ipaddress.IPv4Address]) -> dict:
    """Compute spectrum using MAAD Singularities binary."""
    if not ips or len(ips) < 10:
        return {}
    
    singularities_path = os.path.join(MAAD_PATH, "Singularities")
    if not os.path.exists(singularities_path):
        return {}
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("ip\n")
        for ip in ips:
            f.write(f"{ip}\n")
        temp_path = f.name
    
    try:
        result = subprocess.run(
            [singularities_path, "10", temp_path],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            spectrum = {}
            for line in result.stdout.strip().split("\n"):
                if "," in line:
                    try:
                        alpha, f_alpha = line.strip().split(",")
                        spectrum[alpha] = f_alpha
                    except ValueError:
                        continue
            return spectrum
    except Exception:
        pass
    finally:
        os.unlink(temp_path)
    
    return {}


def process_file(file_info: tuple) -> dict:
    """
    Process a single file and return results.
    
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
        result['data'] = {'spectrum': {}, 'ip_count': 0}
        result['raw_ips'] = set()
        return result
    
    print(f"[spectrum_stats] Processing {file_path}")
    
    try:
        ips = extract_ips(file_path)
        spectrum = compute_spectrum(ips)
        
        result['success'] = True
        result['data'] = {'spectrum': spectrum, 'ip_count': len(ips)}
        result['raw_ips'] = ips
        
    except Exception as e:
        result['error'] = str(e)
        print(f"[spectrum_stats] Error processing {file_path}: {e}")
    
    return result


def compute_aggregates(results: list[dict], router: str, day_start: int) -> list[dict]:
    """
    Compute 30m, 1h, and 1d aggregates from 5m results for a single day.
    """
    aggregates = []
    
    buckets: dict[str, dict[int, set[ipaddress.IPv4Address]]] = {
        '30m': defaultdict(set),
        '1h': defaultdict(set),
        '1d': defaultdict(set),
    }
    
    for result in results:
        if not result['success'] or result['raw_ips'] is None:
            continue
        
        timestamp = result['timestamp']
        ips = result['raw_ips']
        
        dt = unix_to_timestamp(timestamp)
        
        bucket_30m = dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)
        bucket_30m_ts = timestamp_to_unix(bucket_30m)
        
        bucket_1h = dt.replace(minute=0, second=0, microsecond=0)
        bucket_1h_ts = timestamp_to_unix(bucket_1h)
        
        bucket_1d_ts = day_start
        
        for granularity, bucket_ts in [('30m', bucket_30m_ts), ('1h', bucket_1h_ts), ('1d', bucket_1d_ts)]:
            buckets[granularity][bucket_ts].update(ips)
    
    durations = {'30m': 1800, '1h': 3600, '1d': 86400}
    
    for granularity in ['30m', '1h', '1d']:
        for bucket_start, ips in buckets[granularity].items():
            spectrum = compute_spectrum(ips)
            aggregates.append({
                'router': router,
                'granularity': granularity,
                'bucket_start': bucket_start,
                'bucket_end': bucket_start + durations[granularity],
                'spectrum': spectrum,
                'ip_count': len(ips),
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
                INSERT OR REPLACE INTO spectrum_stats 
                (router, granularity, bucket_start, bucket_end, spectrum_data, ip_count)
                VALUES (?, '5m', ?, ?, ?, ?)
            """, (result['router'], bucket_start, bucket_end,
                  json.dumps(data['spectrum']), data['ip_count']))
            inserted_5m += 1
        except Exception as e:
            print(f"[spectrum_stats] Error inserting {result['file_path']}: {e}")
    
    for agg in aggregates:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO spectrum_stats 
                (router, granularity, bucket_start, bucket_end, spectrum_data, ip_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (agg['router'], agg['granularity'], agg['bucket_start'], agg['bucket_end'],
                  json.dumps(agg['spectrum']), agg['ip_count']))
            inserted_agg += 1
        except Exception as e:
            print(f"[spectrum_stats] Error inserting aggregate: {e}")
    
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
    
    print(f"[spectrum_stats] Worker starting {router} {day_dt.strftime('%Y-%m-%d')} ({len(day_files)} files)")
    
    # Process all files for this day sequentially within the worker
    results = [process_file(f) for f in day_files]
    
    # Compute aggregates from accumulated data
    aggregates = compute_aggregates(results, router, day_start)
    
    # Each worker opens its own connection (WAL allows concurrent writes)
    with get_db_connection() as conn:
        init_spectrum_stats_table(conn)
        
        # Write directly to DB
        inserted_5m, inserted_agg = insert_results(conn, results, aggregates)
        
        # Mark files as processed
        batch_mark_processed(conn, 'spectrum_stats', results)
    
    errors = len([r for r in results if not r['success']])
    
    print(f"[spectrum_stats] Worker complete {router} {day_dt.strftime('%Y-%m-%d')}: "
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
    Process all pending files for the spectrum_stats table.
    Uses day-parallel processing where each worker handles a complete day.
    """
    init_spectrum_stats_table(conn)
    
    pending = get_files_needing_processing(conn, 'spectrum_stats', limit)
    stats = {'processed': 0, 'errors': 0, 'aggregates': 0, 'days': 0}
    
    if not pending:
        print("[spectrum_stats] No pending files to process")
        return stats
    
    # Group by day
    days = group_files_by_day(pending)
    print(f"[spectrum_stats] Processing {len(pending)} files across {len(days)} complete days...")
    
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
    print(f"Spectrum DB processing at {datetime.now()}")
    print("--------------------------------")
    
    with get_db_connection() as conn:
        sync_processed_files_table(conn)
        stats = process_pending_files(conn)
        print(f"Processing complete: {stats}")


if __name__ == "__main__":
    main()
