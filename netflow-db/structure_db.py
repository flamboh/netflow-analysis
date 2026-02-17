"""
Structure function statistics processor.

Computes IP structure function from unique source IPs at various granularities.
Uses day-parallel processing with parent-thread DB writes.
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

# Structure function binary path (Zig binary in burstify)
STRUCTURE_FUNCTION_BIN = get_optional_env(
    'STRUCTURE_FUNCTION_BIN',
    str(Path(__file__).parent.parent / 'burstify' / 'zig-out' / 'bin' / 'StructureFunction')
)
MIN_IPS_FOR_STRUCTURE = 100


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


def compute_structure_function(ips: set[ipaddress.IPv4Address]) -> list[dict]:
    """Compute structure function using Zig StructureFunction binary via stdin.
    
    Returns:
        List of {"q": float, "tau": float, "sd": float} objects
    """
    if not ips or len(ips) < MIN_IPS_FOR_STRUCTURE:
        return []
    
    # Convert ipaddress objects to strings for stdin
    input_data = '\n'.join(str(ip) for ip in ips)
    
    try:
        result = subprocess.run(
            [STRUCTURE_FUNCTION_BIN],
            input=input_data,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode != 0:
            print(f"StructureFunction error (returncode {result.returncode}): {result.stderr}")
            return []
        
        # Parse CSV output: q,tauTilde,sd (header may or may not be present)
        lines = result.stdout.strip().split('\n')
        if not lines:
            return []
        
        structure = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if len(parts) == 3:
                try:
                    structure.append({
                        "q": float(parts[0]),
                        "tau": float(parts[1]),
                        "sd": float(parts[2])
                    })
                except ValueError:
                    continue
        return structure
    except subprocess.TimeoutExpired:
        print(f"StructureFunction timed out for {len(ips)} IPs")
        return []
    except FileNotFoundError:
        print(f"StructureFunction binary not found at {STRUCTURE_FUNCTION_BIN}")
        return []
    except Exception as e:
        print(f"StructureFunction error: {e}")
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
        result['data'] = {'structure_sa': [], 'structure_da': []}
        result['raw_ips_sa'] = set()
        result['raw_ips_da'] = set()
        return result
    
    print(f"[structure_stats] Processing {file_path}")
    
    try:
        source_ips, dest_ips = extract_ips(file_path)
        structure_sa = compute_structure_function(source_ips)
        structure_da = compute_structure_function(dest_ips)
        
        result['success'] = True
        result['data'] = {'structure_sa': structure_sa, 'structure_da': structure_da}
        result['raw_ips_sa'] = source_ips
        result['raw_ips_da'] = dest_ips
        
    except Exception as e:
        result['error'] = str(e)
        print(f"[structure_stats] Error processing {file_path}: {e}")
    
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
            structure_sa = compute_structure_function(ips_sa)
            structure_da = compute_structure_function(ips_da)
            aggregates.append({
                'router': router,
                'granularity': granularity,
                'bucket_start': bucket_start,
                'bucket_end': bucket_start + durations[granularity],
                'structure_sa': structure_sa,
                'structure_da': structure_da,
            })
    
    return aggregates


def insert_results(conn: sqlite3.Connection, rows_5m: list[dict], rows_agg: list[dict]) -> tuple[int, int]:
    """Insert prepared 5m and aggregate rows into the database (no commit)."""
    cursor = conn.cursor()
    inserted_5m = 0
    inserted_agg = 0
    
    for row in rows_5m:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO structure_stats 
                (router, granularity, bucket_start, bucket_end, ip_version, structure_json_sa, structure_json_da)
                VALUES (?, '5m', ?, ?, 4, ?, ?)
            """, (row['router'], row['bucket_start'], row['bucket_end'],
                  row['structure_json_sa'], row['structure_json_da']))
            inserted_5m += 1
        except Exception as e:
            print(f"[structure_stats] Error inserting 5m row {row['router']} {row['bucket_start']}: {e}")
    
    for agg in rows_agg:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO structure_stats 
                (router, granularity, bucket_start, bucket_end, ip_version, structure_json_sa, structure_json_da)
                VALUES (?, ?, ?, ?, 4, ?, ?)
            """, (agg['router'], agg['granularity'], agg['bucket_start'], agg['bucket_end'],
                  agg['structure_json_sa'], agg['structure_json_da']))
            inserted_agg += 1
        except Exception as e:
            print(f"[structure_stats] Error inserting aggregate: {e}")
    
    return inserted_5m, inserted_agg


def process_day_worker(day_info: tuple) -> dict:
    """
    Process a complete day and return rows for parent-thread insertion.
    
    Args:
        day_info: Tuple of (router, day_start, day_files)
        
    Returns:
        Dict with processing statistics and prepared insert payloads
    """
    router, day_start, day_files = day_info
    day_dt = unix_to_timestamp(day_start)
    
    print(f"[structure_stats] Worker starting {router} {day_dt.strftime('%Y-%m-%d')} ({len(day_files)} files)")
    
    # Process all files for this day sequentially within the worker
    results = [process_file(f) for f in day_files]
    
    # Compute aggregates from accumulated data
    aggregates = compute_aggregates(results, router, day_start)
    
    rows_5m = []
    for result in results:
        if not result['success'] or result['data'] is None:
            continue
        bucket_start = result['timestamp']
        data = result['data']
        rows_5m.append({
            'router': result['router'],
            'bucket_start': bucket_start,
            'bucket_end': bucket_start + 300,
            'structure_json_sa': json.dumps(data['structure_sa']),
            'structure_json_da': json.dumps(data['structure_da']),
        })

    rows_agg = []
    for agg in aggregates:
        rows_agg.append({
            'router': agg['router'],
            'granularity': agg['granularity'],
            'bucket_start': agg['bucket_start'],
            'bucket_end': agg['bucket_end'],
            'structure_json_sa': json.dumps(agg['structure_sa']),
            'structure_json_da': json.dumps(agg['structure_da']),
        })

    mark_results = [{'file_path': r['file_path'], 'success': r['success']} for r in results]
    errors = len([r for r in results if not r['success']])
    inserted_5m = len(rows_5m)
    inserted_agg = len(rows_agg)
    
    print(f"[structure_stats] Worker complete {router} {day_dt.strftime('%Y-%m-%d')}: "
          f"{inserted_5m} 5m prepared, {inserted_agg} agg prepared, {errors} errors")
    
    return {
        'router': router,
        'day': day_start,
        'processed': inserted_5m,
        'aggregates': inserted_agg,
        'errors': errors,
        'rows_5m': rows_5m,
        'rows_agg': rows_agg,
        'mark_results': mark_results,
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
    
    # Process days in parallel - parent thread owns all database writes
    with Pool(processes=MAX_WORKERS) as pool:
        for result in pool.imap_unordered(process_day_worker, day_tasks, chunksize=1):
            day_dt = unix_to_timestamp(result['day']).strftime('%Y-%m-%d')
            print(f"[structure_stats] Parent writing {result['router']} {day_dt}")
            try:
                conn.execute("BEGIN")
                inserted_5m, inserted_agg = insert_results(conn, result['rows_5m'], result['rows_agg'])
                batch_mark_processed(conn, 'structure_stats', result['mark_results'], commit=False)
                conn.commit()

                stats['processed'] += inserted_5m
                stats['aggregates'] += inserted_agg
                stats['errors'] += result['errors']
                print(f"[structure_stats] Parent commit complete {result['router']} {day_dt}: "
                      f"{inserted_5m} 5m, {inserted_agg} agg, {result['errors']} errors")
            except Exception as e:
                conn.rollback()
                failed_count = len(result['mark_results'])
                stats['errors'] += failed_count
                print(f"[structure_stats] Parent transaction failed for {result['router']} {day_dt}: {e}")
                print(f"[structure_stats] Rolled back {result['router']} {day_dt}; counted {failed_count} errors")
            finally:
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
