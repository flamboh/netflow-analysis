#!/usr/bin/env python3
"""
Unified NetFlow data processing pipeline.

This is the main entry point for cron-based processing. It orchestrates:
1. File discovery - scan filesystem for new NetFlow files
2. Gap detection - identify missing timestamps before the data horizon
3. Processing - run all processors (flow, ip, protocol, spectrum, structure)
4. Zero-fill - insert placeholder rows for gaps

Usage:
    python pipeline.py [--discover-only] [--process-only] [--limit N] [--tables TABLE1,TABLE2]
    
Options:
    --discover-only   Only run discovery phase, skip processing
    --process-only    Skip discovery, only process pending files
    --limit N         Limit processing to N files per table
    --tables          Comma-separated list of tables to process (default: all)
                      Valid: flow_stats,ip_stats,protocol_stats,spectrum_stats,structure_stats
"""

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from common import (
    DATABASE_PATH,
    AVAILABLE_ROUTERS,
    get_db_connection,
    init_processed_files_table,
)
from discovery import (
    sync_processed_files_table,
    compute_data_horizon,
    get_pending_files,
)

# Import processor modules
import flow_db
import ip_db
import protocol_db
import spectrum_db
import structure_db


# Mapping of table names to processor modules
PROCESSORS = {
    'flow_stats': flow_db,
    'ip_stats': ip_db,
    'protocol_stats': protocol_db,
    'spectrum_stats': spectrum_db,
    'structure_stats': structure_db,
}


def setup_logging(log_dir: Path = None):
    """Setup logging to file and stdout."""
    if log_dir is None:
        log_dir = Path(__file__).parent / 'logs' / 'pipeline'
    
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    log_file = log_dir / f'pipeline-{timestamp}.log'
    
    # Simple logging - print to both stdout and file
    class Logger:
        def __init__(self, log_path):
            self.terminal = sys.stdout
            self.log = open(log_path, 'w')
        
        def write(self, message):
            self.terminal.write(message)
            self.log.write(message)
        
        def flush(self):
            self.terminal.flush()
            self.log.flush()
    
    sys.stdout = Logger(log_file)
    print(f"Logging to {log_file}")
    return log_file


def run_discovery(conn: sqlite3.Connection) -> dict:
    """
    Run the file discovery phase.
    
    Scans the filesystem for NetFlow files and identifies gaps.
    
    Args:
        conn: Database connection
        
    Returns:
        Discovery statistics
    """
    print("\n" + "=" * 60)
    print("PHASE 1: FILE DISCOVERY")
    print("=" * 60)
    
    stats = sync_processed_files_table(conn, include_gaps=True)
    
    horizon = compute_data_horizon(conn)
    print(f"\nData horizon: {horizon}")
    
    # Count pending files
    pending = get_pending_files(conn)
    print(f"Total pending files: {len(pending)}")
    
    return stats


def run_processing(
    conn: sqlite3.Connection,
    tables: list[str] = None,
    limit: int = None
) -> dict:
    """
    Run the processing phase for specified tables.
    
    Args:
        conn: Database connection
        tables: List of table names to process (default: all)
        limit: Optional limit on files per table
        
    Returns:
        Processing statistics per table
    """
    print("\n" + "=" * 60)
    print("PHASE 2: DATA PROCESSING")
    print("=" * 60)
    
    if tables is None:
        tables = list(PROCESSORS.keys())
    
    all_stats = {}
    
    for table_name in tables:
        if table_name not in PROCESSORS:
            print(f"Warning: Unknown table '{table_name}', skipping")
            continue
        
        processor = PROCESSORS[table_name]
        
        print(f"\n--- Processing {table_name} ---")
        
        # Initialize the table
        if hasattr(processor, f'init_{table_name}_table'):
            init_func = getattr(processor, f'init_{table_name}_table')
            init_func(conn)
        elif table_name == 'flow_stats':
            processor.init_netflow_stats_table(conn)
        elif table_name == 'ip_stats':
            processor.init_ip_stats_table(conn)
        elif table_name == 'protocol_stats':
            processor.init_protocol_stats_table(conn)
        elif table_name == 'spectrum_stats':
            processor.init_spectrum_stats_table(conn)
        elif table_name == 'structure_stats':
            processor.init_structure_stats_table(conn)
        
        # Process pending files
        stats = processor.process_pending_files(conn, limit)
        all_stats[table_name] = stats
        
        print(f"{table_name}: {stats['processed']} processed, {stats['errors']} errors")
    
    return all_stats


def print_summary(discovery_stats: dict, processing_stats: dict):
    """Print a summary of the pipeline run."""
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    
    if discovery_stats:
        print("\nDiscovery:")
        print(f"  Files discovered: {discovery_stats.get('discovered', 0)}")
        print(f"  New files: {discovery_stats.get('new_files', 0)}")
        print(f"  Gaps identified: {discovery_stats.get('gaps', 0)}")
    
    if processing_stats:
        print("\nProcessing:")
        total_processed = 0
        total_errors = 0
        for table, stats in processing_stats.items():
            print(f"  {table}: {stats['processed']} processed, {stats['errors']} errors")
            total_processed += stats['processed']
            total_errors += stats['errors']
        print(f"  TOTAL: {total_processed} processed, {total_errors} errors")


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(
        description='NetFlow data processing pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--discover-only',
        action='store_true',
        help='Only run discovery phase, skip processing'
    )
    parser.add_argument(
        '--process-only',
        action='store_true',
        help='Skip discovery, only process pending files'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit processing to N files per table'
    )
    parser.add_argument(
        '--tables',
        type=str,
        default=None,
        help='Comma-separated list of tables to process'
    )
    parser.add_argument(
        '--no-log',
        action='store_true',
        help='Disable logging to file'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    if not args.no_log:
        setup_logging()
    
    print("=" * 60)
    print(f"NetFlow Pipeline - {datetime.now()}")
    print("=" * 60)
    print(f"Database: {DATABASE_PATH}")
    print(f"Routers: {', '.join(AVAILABLE_ROUTERS)}")
    
    # Parse tables argument
    tables = None
    if args.tables:
        tables = [t.strip() for t in args.tables.split(',')]
        print(f"Tables: {', '.join(tables)}")
    else:
        print("Tables: all")
    
    if args.limit:
        print(f"Limit: {args.limit} files per table")
    
    start_time = datetime.now()
    discovery_stats = {}
    processing_stats = {}
    
    with get_db_connection() as conn:
        # Ensure processed_files table exists
        init_processed_files_table(conn)
        
        # Phase 1: Discovery
        if not args.process_only:
            discovery_stats = run_discovery(conn)
        
        # Phase 2: Processing
        if not args.discover_only:
            processing_stats = run_processing(conn, tables, args.limit)
    
    # Print summary
    print_summary(discovery_stats, processing_stats)
    
    elapsed = datetime.now() - start_time
    print(f"\nTotal time: {elapsed}")
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
