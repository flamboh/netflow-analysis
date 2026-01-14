"""
Structure function statistics processor.

Computes IP structure function from unique source IPs at various granularities.
"""

import sqlite3
import subprocess
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


def init_structure_stats_table(conn: sqlite3.Connection) -> None:
    """Create the structure_stats table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS structure_stats (
            router TEXT NOT NULL,
            granularity TEXT NOT NULL CHECK (granularity IN ('5m','30m','1h','1d')),
            bucket_start INTEGER NOT NULL,
            bucket_end   INTEGER NOT NULL,
            structure_data TEXT NOT NULL DEFAULT '{}',
            ip_count INTEGER NOT NULL DEFAULT 0,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (router, granularity, bucket_start)
        ) WITHOUT ROWID;
    """)
    conn.commit()


def extract_ips(file_path: str) -> list[str]:
    """Extract unique source IPv4 addresses from a netflow file."""
    command = ["nfdump", "-r", file_path, "-q", "-o", "fmt:%sa", "-n", "0", "ipv4"]
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            ips = set()
            for line in result.stdout.strip().split("\n"):
                if line.strip():
                    ips.add(line.strip())
            return list(ips)
    except Exception:
        pass
    
    return []


def compute_structure_function(ips: list[str]) -> dict:
    """Compute structure function using MAAD StructureFunction binary."""
    if not ips or len(ips) < 10:
        return {}
    
    structure_path = os.path.join(MAAD_PATH, "StructureFunction")
    if not os.path.exists(structure_path):
        return {}
    
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
            structure = {}
            for line in result.stdout.strip().split("\n"):
                if "," in line:
                    try:
                        scale, value = line.strip().split(",")
                        structure[scale] = value
                    except ValueError:
                        continue
            return structure
    except Exception:
        pass
    finally:
        os.unlink(temp_path)
    
    return {}


def process_file_worker(task: tuple) -> dict:
    """Worker function to process a single file (no DB access)."""
    file_path, router, timestamp_unix, file_exists = task
    
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
        result['data'] = {'structure': {}, 'ip_count': 0}
        result['raw_ips'] = []
        return result
    
    print(f"[structure_stats] Processing {file_path}")
    
    try:
        ips = extract_ips(file_path)
        structure = compute_structure_function(ips)
        
        result['success'] = True
        result['data'] = {'structure': structure, 'ip_count': len(ips)}
        result['raw_ips'] = ips
        
    except Exception as e:
        result['error'] = str(e)
        print(f"[structure_stats] Error processing {file_path}: {e}")
    
    return result


def compute_aggregates(results: list[dict], router: str, day_start: int) -> list[dict]:
    """
    Compute 30m, 1h, and 1d aggregates from 5m results for a single day.
    """
    aggregates = []
    
    buckets = {
        '30m': defaultdict(set),
        '1h': defaultdict(set),
        '1d': defaultdict(set),
    }
    
    for result in results:
        if not result['success'] or result['raw_ips'] is None:
            continue
        
        timestamp = result['timestamp']
        ips = set(result['raw_ips'])
        
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
            structure = compute_structure_function(list(ips))
            aggregates.append({
                'router': router,
                'granularity': granularity,
                'bucket_start': bucket_start,
                'bucket_end': bucket_start + durations[granularity],
                'structure': structure,
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
                INSERT OR REPLACE INTO structure_stats 
                (router, granularity, bucket_start, bucket_end, structure_data, ip_count)
                VALUES (?, '5m', ?, ?, ?, ?)
            """, (result['router'], bucket_start, bucket_end,
                  json.dumps(data['structure']), data['ip_count']))
            inserted_5m += 1
        except Exception as e:
            print(f"[structure_stats] Error inserting {result['file_path']}: {e}")
    
    for agg in aggregates:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO structure_stats 
                (router, granularity, bucket_start, bucket_end, structure_data, ip_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (agg['router'], agg['granularity'], agg['bucket_start'], agg['bucket_end'],
                  json.dumps(agg['structure']), agg['ip_count']))
            inserted_agg += 1
        except Exception as e:
            print(f"[structure_stats] Error inserting aggregate: {e}")
    
    conn.commit()
    return inserted_5m, inserted_agg


def process_pending_files(conn: sqlite3.Connection, limit: int = None) -> dict:
    """
    Process all pending files for the structure_stats table using parallel processing.
    Processes complete days only, computing aggregates after each day.
    """
    init_structure_stats_table(conn)
    
    pending = get_files_needing_processing(conn, 'structure_stats', limit)
    stats = {'processed': 0, 'errors': 0, 'aggregates': 0, 'days': 0}
    
    if not pending:
        print("[structure_stats] No pending files to process")
        return stats
    
    days = group_files_by_day(pending)
    print(f"[structure_stats] Processing {len(pending)} files across {len(days)} complete days with {MAX_WORKERS} workers...")
    
    for (router, day_start), day_files in sorted(days.items()):
        day_dt = unix_to_timestamp(day_start)
        print(f"[structure_stats] Processing {router} {day_dt.strftime('%Y-%m-%d')} ({len(day_files)} files)")
        
        with Pool(processes=MAX_WORKERS) as pool:
            results = pool.map(process_file_worker, day_files)
        
        aggregates = compute_aggregates(results, router, day_start)
        inserted_5m, inserted_agg = insert_results(conn, results, aggregates)
        batch_mark_processed(conn, 'structure_stats', results)
        
        errors = len([r for r in results if not r['success']])
        stats['processed'] += inserted_5m
        stats['aggregates'] += inserted_agg
        stats['errors'] += errors
        stats['days'] += 1
        
        print(f"[structure_stats] Day complete: {inserted_5m} 5m records, {inserted_agg} aggregates, {errors} errors")
    
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
