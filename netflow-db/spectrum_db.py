"""
Spectrum statistics processor.

Computes IP spectrum from unique source and destination IPs at various granularities.
Uses day-parallel processing with WAL-enabled direct DB writes.
"""

import sqlite3
import subprocess
import ipaddress
import json
from datetime import datetime, timedelta
from multiprocessing import Pool
from pathlib import Path
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
    handle_stale_days,
)

# Spectrum binary path
SPECTRUM_BIN = get_optional_env(
    'SPECTRUM_BIN',
    str(Path(__file__).parent.parent / 'maad' / 'Spectrum')
)
MIN_IPS_FOR_SPECTRUM = 100


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


def extract_ips(file_path: str) -> tuple[set[ipaddress.IPv4Address], set[ipaddress.IPv4Address]]:
    """Extract unique source and destination IPv4 addresses from a netflow file."""
    source_ips: set[ipaddress.IPv4Address] = set()
    dest_ips: set[ipaddress.IPv4Address] = set()
    
    try:
        result = subprocess.run(
            ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa,%da", "ipv4"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if line.strip() and "," in line:
                    parts = line.split(",")
                    if len(parts) >= 2:
                        try:
                            source_ips.add(ipaddress.IPv4Address(parts[0].strip()))
                        except ipaddress.AddressValueError:
                            pass
                        try:
                            dest_ips.add(ipaddress.IPv4Address(parts[1].strip()))
                        except ipaddress.AddressValueError:
                            pass
    except subprocess.TimeoutExpired:
        print(f"Timeout extracting IPs from {file_path}")
    except Exception as e:
        print(f"Error extracting IPs from {file_path}: {e}")
    
    return source_ips, dest_ips


def compute_spectrum(ips: set[ipaddress.IPv4Address]) -> list[dict]:
    """Compute spectrum using MAAD Spectrum binary via stdin.
    
    Returns:
        List of {"alpha": float, "f": float} objects
    """
    if not ips or len(ips) < MIN_IPS_FOR_SPECTRUM:
        return []
    
    # Convert ipaddress objects to strings for stdin
    input_data = '\n'.join(str(ip) for ip in ips)
    
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
        
        # Parse CSV output: alpha,f (header may or may not be present)
        lines = result.stdout.strip().split('\n')
        if not lines:
            return []
        
        spectrum = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if len(parts) == 2:
                try:
                    spectrum.append({
                        "alpha": float(parts[0]),
                        "f": float(parts[1])
                    })
                except ValueError:
                    continue
        return spectrum
    except subprocess.TimeoutExpired:
        print(f"Spectrum timed out for {len(ips)} IPs")
        return []
    except FileNotFoundError:
        print(f"Spectrum binary not found at {SPECTRUM_BIN}")
        return []
    except Exception as e:
        print(f"Spectrum error: {e}")
        return []


def process_file(file_info: tuple) -> dict:
    """
    Process a single file and return results for IPv4.
    
    Note: IPv6 support is not yet available in MAAD binaries.
    
    Args:
        file_info: Tuple of (file_path, router, timestamp, file_exists)
        
    Returns:
        Dict with file_path, success, data, and raw_ips_sa/raw_ips_da (as ipaddress objects)
    """
    file_path, router, timestamp_unix, file_exists = file_info
    
    result = {
        'file_path': file_path,
        'router': router,
        'timestamp': timestamp_unix,
        'success': False,
        'data': None,
        'raw_ips_sa': None,
        'raw_ips_da': None,
        'error': None
    }
    
    if not file_exists:
        result['success'] = True
        result['data'] = {'spectrum_sa': [], 'spectrum_da': []}
        result['raw_ips_sa'] = set()
        result['raw_ips_da'] = set()
        return result
    
    print(f"[spectrum_stats] Processing {file_path}")
    
    try:
        source_ips, dest_ips = extract_ips(file_path)
        spectrum_sa = compute_spectrum(source_ips)
        spectrum_da = compute_spectrum(dest_ips)
        
        result['success'] = True
        result['data'] = {'spectrum_sa': spectrum_sa, 'spectrum_da': spectrum_da}
        result['raw_ips_sa'] = source_ips
        result['raw_ips_da'] = dest_ips
        
    except Exception as e:
        result['error'] = str(e)
        print(f"[spectrum_stats] Error processing {file_path}: {e}")
    
    return result


def compute_aggregates(results: list[dict], router: str, day_start: int) -> list[dict]:
    """
    Compute 30m, 1h, and 1d aggregates from 5m results for a single day.
    Tracks source and destination IPs separately.
    """
    aggregates = []
    
    # Separate buckets for source and destination IPs
    buckets_sa: dict[str, dict[int, set[ipaddress.IPv4Address]]] = {
        '30m': defaultdict(set),
        '1h': defaultdict(set),
        '1d': defaultdict(set),
    }
    buckets_da: dict[str, dict[int, set[ipaddress.IPv4Address]]] = {
        '30m': defaultdict(set),
        '1h': defaultdict(set),
        '1d': defaultdict(set),
    }
    
    for result in results:
        if not result['success'] or result['raw_ips_sa'] is None:
            continue
        
        timestamp = result['timestamp']
        ips_sa = result['raw_ips_sa']
        ips_da = result['raw_ips_da']
        
        dt = unix_to_timestamp(timestamp)
        
        bucket_30m = dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)
        bucket_30m_ts = timestamp_to_unix(bucket_30m)
        
        bucket_1h = dt.replace(minute=0, second=0, microsecond=0)
        bucket_1h_ts = timestamp_to_unix(bucket_1h)
        
        bucket_1d_ts = day_start
        
        for granularity, bucket_ts in [('30m', bucket_30m_ts), ('1h', bucket_1h_ts), ('1d', bucket_1d_ts)]:
            buckets_sa[granularity][bucket_ts].update(ips_sa)
            buckets_da[granularity][bucket_ts].update(ips_da)
    
    durations = {'30m': 1800, '1h': 3600, '1d': 86400}
    
    for granularity in ['30m', '1h', '1d']:
        for bucket_start in buckets_sa[granularity].keys():
            ips_sa = buckets_sa[granularity][bucket_start]
            ips_da = buckets_da[granularity][bucket_start]
            spectrum_sa = compute_spectrum(ips_sa)
            spectrum_da = compute_spectrum(ips_da)
            aggregates.append({
                'router': router,
                'granularity': granularity,
                'bucket_start': bucket_start,
                'bucket_end': bucket_start + durations[granularity],
                'spectrum_sa': spectrum_sa,
                'spectrum_da': spectrum_da,
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
        
        spectrum_sa = compute_spectrum(result['raw_ips_sa'])
        spectrum_da = compute_spectrum(result['raw_ips_da'])
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO spectrum_stats 
                (router, granularity, bucket_start, bucket_end, ip_version, spectrum_json_sa, spectrum_json_da)
                VALUES (?, '5m', ?, ?, 4, ?, ?)
            """, (result['router'], bucket_start, bucket_end,
                  json.dumps(spectrum_sa), json.dumps(spectrum_da)))
            inserted_5m += 1
        except Exception as e:
            print(f"[spectrum_stats] Error inserting {result['file_path']}: {e}")
    
    for agg in aggregates:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO spectrum_stats 
                (router, granularity, bucket_start, bucket_end, ip_version, spectrum_json_sa, spectrum_json_da)
                VALUES (?, ?, ?, ?, 4, ?, ?)
            """, (agg['router'], agg['granularity'], agg['bucket_start'], agg['bucket_end'],
                  json.dumps(agg['spectrum_sa']), json.dumps(agg['spectrum_da'])))
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
    
    # Handle stale days - reset days that have new files mixed with already-processed files
    handle_stale_days(conn, 'spectrum_stats')
    
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
