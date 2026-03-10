#!/usr/bin/env python3
"""
Unified NetFlow data processing pipeline.

This is the main entry point for cron-based processing. It orchestrates:
1. File discovery - scan filesystem for new NetFlow files
2. Gap detection - identify missing timestamps before the data horizon
3. Processing - run all processors (flow, ip, protocol, spectrum, structure)
4. Zero-fill - insert placeholder rows for gaps

Usage:
        python pipeline.py
        python pipeline.py --discover-only
        python pipeline.py --process-only
        python pipeline.py --tables flow_stats,ip_stats
        python pipeline.py --limit 500
        python pipeline.py --discovery-window-days 30 --reprocess-window-days 14
        python pipeline.py --no-log
        python pipeline.py --dataset uoregon
        python pipeline.py --dataset uoregon --discover-only
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(log_dir: Path | None = None):
    """Setup logging to file and stdout."""
    if log_dir is None:
        log_dir = Path(__file__).parent / 'logs' / 'pipeline'

    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    log_file = log_dir / f'pipeline-{timestamp}.log'

    class Logger:
        def __init__(self, log_path: Path):
            self.terminal = sys.stdout
            self.log = open(log_path, 'w')

        def write(self, message: str):
            self.terminal.write(message)
            self.log.write(message)

        def flush(self):
            self.terminal.flush()
            self.log.flush()

    sys.stdout = Logger(log_file)
    print(f"Logging to {log_file}")
    return log_file


def format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as HH:MM:SS."""
    total_seconds = int(round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def print_summary(discovery_stats: dict, processing_stats: dict, max_workers: int):
    """Print a summary of the pipeline run."""
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)

    if discovery_stats:
        print("\nDiscovery:")
        print(f"  Files discovered: {discovery_stats.get('discovered', 0)}")
        print(f"  New files: {discovery_stats.get('new_files', 0)}")
        print(f"  Gaps identified: {discovery_stats.get('gaps', 0)}")
        print(f"  Elapsed: {format_elapsed(discovery_stats.get('elapsed_seconds', 0.0))}")

    if processing_stats:
        print("\nProcessing:")
        total_processed = 0
        total_errors = 0
        total_attempted = 0
        total_elapsed_seconds = 0.0
        for table, stats in processing_stats.items():
            attempted = stats.get('attempted', 0)
            elapsed_seconds = stats.get('elapsed_seconds', 0.0)
            avg_per_file = (
                f"{elapsed_seconds / attempted:.2f}s/file with {max_workers} workers"
                if attempted
                else f"n/a with {max_workers} workers"
            )
            print(
                f"  {table}: {format_elapsed(elapsed_seconds)} total, "
                f"{attempted} attempted files, {avg_per_file}, "
                f"{stats['processed']} processed, {stats['errors']} errors"
            )
            total_processed += stats['processed']
            total_errors += stats['errors']
            total_attempted += attempted
            total_elapsed_seconds += elapsed_seconds
        total_avg_per_file = (
            f"{total_elapsed_seconds / total_attempted:.2f}s/file with {max_workers} workers"
            if total_attempted
            else f"n/a with {max_workers} workers"
        )
        print(
            f"  TOTAL: {format_elapsed(total_elapsed_seconds)} total, "
            f"{total_attempted} attempted files, {total_avg_per_file}, "
            f"{total_processed} processed, {total_errors} errors"
        )


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(
        description='NetFlow data processing pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--dataset',
        type=str,
        default=None,
        help='Dataset id from datasets.json (default: NETFLOW_DATASET or DEFAULT_DATASET)',
    )
    parser.add_argument(
        '--discover-only',
        action='store_true',
        help='Only run discovery phase, skip processing',
    )
    parser.add_argument(
        '--process-only',
        action='store_true',
        help='Skip discovery, only process pending files',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit processing to N files per table',
    )
    parser.add_argument(
        '--tables',
        type=str,
        default=None,
        help='Comma-separated list of tables to process',
    )
    parser.add_argument(
        '--no-log',
        action='store_true',
        help='Disable logging to file',
    )
    parser.add_argument(
        '--reprocess-window-days',
        type=int,
        default=30,
        help='Reprocessing window in days (default: 30, 0 = unlimited)',
    )
    parser.add_argument(
        '--discovery-window-days',
        type=int,
        default=None,
        help='Discovery scan window in days (default: matches reprocessing window, 0 = unlimited)',
    )

    args = parser.parse_args()
    if args.dataset:
        os.environ['NETFLOW_DATASET'] = args.dataset

    from common import (
        ACTIVE_DATASET,
        AVAILABLE_ROUTERS,
        DATABASE_PATH,
        MAX_WORKERS,
        get_db_connection,
        init_processed_files_table,
    )
    from discovery import compute_data_horizon, get_pending_files, sync_processed_files_table
    import flow_db
    import ip_db
    import protocol_db
    import spectrum_db
    import structure_db

    processors = {
        'flow_stats': flow_db,
        'ip_stats': ip_db,
        'protocol_stats': protocol_db,
        'spectrum_stats': spectrum_db,
        'structure_stats': structure_db,
    }

    def run_discovery(
        conn,
        reprocess_window_days: int = 30,
        discovery_window_days: int = 30,
    ) -> dict:
        print("\n" + "=" * 60)
        print("PHASE 1: FILE DISCOVERY")
        print("=" * 60)

        phase_start = datetime.now()
        stats = sync_processed_files_table(
            conn,
            include_gaps=True,
            reprocess_window_days=reprocess_window_days,
            discovery_window_days=discovery_window_days,
        )
        stats['elapsed_seconds'] = (datetime.now() - phase_start).total_seconds()

        horizon = compute_data_horizon(conn)
        print(f"\nData horizon: {horizon}")

        pending = get_pending_files(conn, reprocess_window_days=reprocess_window_days)
        print(f"Total pending files: {len(pending)}")

        return stats

    def run_processing(
        conn,
        tables: list[str] | None = None,
        limit: int | None = None,
        reprocess_window_days: int = 30,
    ) -> dict:
        print("\n" + "=" * 60)
        print("PHASE 2: DATA PROCESSING")
        print("=" * 60)

        if tables is None:
            tables = list(processors.keys())

        all_stats: dict[str, dict] = {}

        for table_name in tables:
            if table_name not in processors:
                print(f"Warning: Unknown table '{table_name}', skipping")
                continue

            processor = processors[table_name]
            print(f"\n--- Processing {table_name} ---")

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

            phase_start = datetime.now()
            stats = processor.process_pending_files(conn, limit, reprocess_window_days)
            stats['elapsed_seconds'] = (datetime.now() - phase_start).total_seconds()
            all_stats[table_name] = stats

            print(f"{table_name}: {stats['processed']} processed, {stats['errors']} errors")

        return all_stats

    discovery_window_days = (
        args.reprocess_window_days
        if args.discovery_window_days is None
        else args.discovery_window_days
    )

    if not args.no_log:
        setup_logging()

    print("=" * 60)
    print(f"NetFlow Pipeline - {datetime.now()}")
    print("=" * 60)
    print(f"Dataset: {ACTIVE_DATASET['dataset_id']}")
    print(f"Database: {DATABASE_PATH}")
    print(f"Routers: {', '.join(AVAILABLE_ROUTERS)}")

    tables = None
    if args.tables:
        tables = [t.strip() for t in args.tables.split(',')]
        print(f"Tables: {', '.join(tables)}")
    else:
        print("Tables: all")

    if args.limit:
        print(f"Limit: {args.limit} files per table")
    if args.reprocess_window_days == 0:
        print("Reprocessing window: unlimited")
    else:
        print(f"Reprocessing window: last {args.reprocess_window_days} days")
    if discovery_window_days == 0:
        print("Discovery window: unlimited")
    else:
        print(f"Discovery window: last {discovery_window_days} days")

    start_time = datetime.now()
    discovery_stats: dict = {}
    processing_stats: dict = {}

    with get_db_connection() as conn:
        init_processed_files_table(conn)

        if not args.process_only:
            discovery_stats = run_discovery(
                conn,
                args.reprocess_window_days,
                discovery_window_days,
            )

        if not args.discover_only:
            processing_stats = run_processing(conn, tables, args.limit, args.reprocess_window_days)

    print_summary(discovery_stats, processing_stats, MAX_WORKERS)

    elapsed = datetime.now() - start_time
    print(f"\nTotal time: {elapsed}")
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
