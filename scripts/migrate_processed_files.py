#!/usr/bin/env python3
"""
Migration script for the processed_files table.

This script:
1. Deletes all data after Oct 1, 2025 from all tables
2. Backfills processed_files from known-good netflow_stats data (Feb 11 - Oct 1, 2025)
3. Marks those files as fully processed across all 5 tables

Usage:
    python migrate_processed_files.py --dry-run     # Show what would be changed
    python migrate_processed_files.py               # Run migration
    python migrate_processed_files.py --verify-only # Just verify current state
"""

import argparse
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

from common import DATABASE_PATH, init_processed_files_table

# Migration timestamps
KNOWN_GOOD_START = 1738368000  # Feb 1, 2025 (matches DATA_START_DATE in common.py)
KNOWN_GOOD_END = 1759276800    # Oct 1, 2025 (for backfill range)
DELETE_AFTER = 1759302000      # Oct 1, 2025 (for deletion cutoff)

TABLES_TO_CLEAN = [
    ('netflow_stats', 'timestamp'),
    ('ip_stats', 'bucket_start'),
    ('protocol_stats', 'bucket_start'),
    ('spectrum_stats', 'bucket_start'),
    ('structure_stats', 'bucket_start'),
]


def get_counts(conn: sqlite3.Connection) -> dict:
    """Get current row counts for all tables."""
    cursor = conn.cursor()
    counts = {}
    
    for table, _ in TABLES_TO_CLEAN:
        try:
            row = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            counts[table] = row[0] if row else 0
        except sqlite3.OperationalError:
            counts[table] = 0
    
    # processed_files
    try:
        row = cursor.execute("SELECT COUNT(*) FROM processed_files").fetchone()
        counts['processed_files'] = row[0] if row else 0
    except sqlite3.OperationalError:
        counts['processed_files'] = 0
    
    return counts


def get_delete_counts(conn: sqlite3.Connection) -> dict:
    """Get counts of rows that would be deleted."""
    cursor = conn.cursor()
    counts = {}
    
    for table, timestamp_col in TABLES_TO_CLEAN:
        try:
            row = cursor.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {timestamp_col} >= ?",
                (DELETE_AFTER,)
            ).fetchone()
            counts[table] = row[0] if row else 0
        except sqlite3.OperationalError:
            counts[table] = 0
    
    return counts


def get_backfill_count(conn: sqlite3.Connection) -> int:
    """Get count of rows that would be backfilled to processed_files."""
    cursor = conn.cursor()
    try:
        row = cursor.execute("""
            SELECT COUNT(*) FROM netflow_stats
            WHERE timestamp >= ? AND timestamp < ?
        """, (KNOWN_GOOD_START, KNOWN_GOOD_END)).fetchone()
        return row[0] if row else 0
    except sqlite3.OperationalError:
        return 0


def create_backup(db_path: str) -> str:
    """Create a backup of the database."""
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    backup_path = f"{db_path}.backup-{timestamp}"
    
    print(f"Creating backup: {backup_path}")
    result = subprocess.run(
        ['sqlite3', db_path],
        input=f'.backup {backup_path}\n',
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Backup failed: {result.stderr}")
    
    return backup_path


def run_dry_run(conn: sqlite3.Connection):
    """Show what the migration would do without making changes."""
    print("\n" + "=" * 60)
    print("DRY RUN - No changes will be made")
    print("=" * 60)
    
    # Current counts
    print("\nCurrent row counts:")
    counts = get_counts(conn)
    for table, count in counts.items():
        print(f"  {table}: {count:,}")
    
    # Delete counts
    print(f"\nRows to delete (timestamp >= {DELETE_AFTER}):")
    delete_counts = get_delete_counts(conn)
    for table, count in delete_counts.items():
        print(f"  {table}: {count:,}")
    
    # Backfill count
    backfill_count = get_backfill_count(conn)
    print(f"\nRows to backfill to processed_files: {backfill_count:,}")
    print(f"  (from netflow_stats where {KNOWN_GOOD_START} <= timestamp < {KNOWN_GOOD_END})")
    
    # Expected final counts
    print("\nExpected counts after migration:")
    for table, count in counts.items():
        if table in delete_counts:
            final = count - delete_counts[table]
        elif table == 'processed_files':
            final = backfill_count
        else:
            final = count
        print(f"  {table}: {final:,}")


def run_migration(conn: sqlite3.Connection):
    """Execute the migration."""
    print("\n" + "=" * 60)
    print("RUNNING MIGRATION")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Phase 1: Delete post-Oct 1 data
    print("\nPhase 1: Deleting data after Oct 1, 2025...")
    for table, timestamp_col in TABLES_TO_CLEAN:
        cursor.execute(
            f"DELETE FROM {table} WHERE {timestamp_col} >= ?",
            (DELETE_AFTER,)
        )
        deleted = cursor.rowcount
        print(f"  {table}: {deleted:,} rows deleted")
    
    conn.commit()
    
    # Phase 2: Ensure processed_files table exists
    print("\nPhase 2: Ensuring processed_files table exists...")
    init_processed_files_table(conn)
    
    # Phase 3: Backfill processed_files
    print("\nPhase 3: Backfilling processed_files from netflow_stats...")
    cursor.execute("""
        INSERT INTO processed_files 
          (file_path, router, timestamp, file_exists, discovered_at, processed_at,
           flow_stats_status, ip_stats_status, protocol_stats_status, 
           spectrum_stats_status, structure_stats_status)
        SELECT 
          file_path, router, timestamp, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
          1, 1, 1, 1, 1
        FROM netflow_stats
        WHERE timestamp >= ? AND timestamp < ?
        ON CONFLICT(file_path) DO NOTHING
    """, (KNOWN_GOOD_START, KNOWN_GOOD_END))
    
    inserted = cursor.rowcount
    print(f"  Inserted {inserted:,} rows into processed_files")
    
    conn.commit()
    
    # Phase 4: Verification
    print("\nPhase 4: Verification...")
    counts = get_counts(conn)
    for table, count in counts.items():
        print(f"  {table}: {count:,}")
    
    print("\nMigration complete!")


def run_verify(conn: sqlite3.Connection):
    """Verify the current state of the database."""
    print("\n" + "=" * 60)
    print("DATABASE VERIFICATION")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Row counts
    print("\nRow counts:")
    counts = get_counts(conn)
    for table, count in counts.items():
        print(f"  {table}: {count:,}")
    
    # Date ranges
    print("\nDate ranges:")
    for table, timestamp_col in TABLES_TO_CLEAN:
        try:
            row = cursor.execute(f"""
                SELECT MIN({timestamp_col}), MAX({timestamp_col}) FROM {table}
            """).fetchone()
            if row and row[0]:
                min_dt = datetime.fromtimestamp(row[0])
                max_dt = datetime.fromtimestamp(row[1])
                print(f"  {table}: {min_dt} to {max_dt}")
            else:
                print(f"  {table}: (empty)")
        except sqlite3.OperationalError as e:
            print(f"  {table}: (error: {e})")
    
    # processed_files stats
    try:
        row = cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN processed_at IS NOT NULL THEN 1 ELSE 0 END) as processed,
                SUM(CASE WHEN file_exists = 0 THEN 1 ELSE 0 END) as gaps
            FROM processed_files
        """).fetchone()
        if row:
            print(f"\nprocessed_files status:")
            print(f"  Total entries: {row[0]:,}")
            print(f"  Fully processed: {row[1]:,}")
            print(f"  Gap placeholders: {row[2]:,}")
    except sqlite3.OperationalError:
        print("\nprocessed_files table does not exist")


def main():
    parser = argparse.ArgumentParser(
        description='Migration script for processed_files table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without making changes'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify current state, do not migrate'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Skip creating a backup (not recommended)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"Database Migration - {datetime.now()}")
    print("=" * 60)
    print(f"Database: {DATABASE_PATH}")
    print(f"Known-good range: {KNOWN_GOOD_START} to {KNOWN_GOOD_END}")
    print(f"Delete after: {DELETE_AFTER}")
    
    conn = sqlite3.connect(DATABASE_PATH)
    
    try:
        if args.verify_only:
            run_verify(conn)
        elif args.dry_run:
            run_dry_run(conn)
        else:
            # Create backup before migration
            if not args.no_backup:
                backup_path = create_backup(DATABASE_PATH)
                print(f"Backup created: {backup_path}")
            
            run_migration(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
