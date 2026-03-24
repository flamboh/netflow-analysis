#!/usr/bin/env python3
"""
Repair mirrored-root duplicates in NetFlow SQLite databases.

Keeps a preferred root for each logical capture slot (router + timestamp),
deletes duplicate rows from netflow_stats and processed_files for the other root,
and optionally adds unique indexes on (router, timestamp).

Example:
    python tools/netflow-db/repair_mirrored_root_duplicates.py \
        --db data/uoregon/netflow.sqlite \
        --preferred-prefix /research/obo/netflow_datasets/uoregon/ \
        --remove-prefix /research/tango_cis/uonet-in/ \
        --start-date 2026-03-02
"""

import argparse
import sqlite3
from pathlib import Path


def build_prefix_pattern(prefix: str) -> str:
    return prefix.rstrip('/') + '/%'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True, help='Path to SQLite database')
    parser.add_argument('--preferred-prefix', required=True, help='Path prefix to keep')
    parser.add_argument('--remove-prefix', required=True, help='Path prefix to remove')
    parser.add_argument('--start-date', required=True, help='Inclusive local date YYYY-MM-DD')
    parser.add_argument('--end-date', default=None, help='Inclusive local date YYYY-MM-DD')
    parser.add_argument('--dry-run', action='store_true', help='Report changes without writing')
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    preferred_pattern = build_prefix_pattern(args.preferred_prefix)
    remove_pattern = build_prefix_pattern(args.remove_prefix)
    end_date = args.end_date or '9999-12-31'

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    duplicate_slots = conn.execute(
        """
        SELECT router, timestamp
        FROM processed_files
        WHERE date(datetime(timestamp, 'unixepoch', 'localtime')) BETWEEN ? AND ?
        GROUP BY router, timestamp
        HAVING
            SUM(CASE WHEN file_path LIKE ? THEN 1 ELSE 0 END) > 0
            AND SUM(CASE WHEN file_path LIKE ? THEN 1 ELSE 0 END) > 0
        """,
        (args.start_date, end_date, preferred_pattern, remove_pattern),
    ).fetchall()

    duplicate_count = len(duplicate_slots)
    print(f"Logical duplicate slots matched: {duplicate_count}")
    if duplicate_count == 0:
        return

    delete_netflow = conn.execute(
        """
        SELECT COUNT(*)
        FROM netflow_stats
        WHERE date(datetime(timestamp, 'unixepoch', 'localtime')) BETWEEN ? AND ?
          AND file_path LIKE ?
          AND EXISTS (
              SELECT 1
              FROM netflow_stats preferred
              WHERE preferred.router = netflow_stats.router
                AND preferred.timestamp = netflow_stats.timestamp
                AND preferred.file_path LIKE ?
          )
        """,
        (args.start_date, end_date, remove_pattern, preferred_pattern),
    ).fetchone()[0]
    delete_processed = conn.execute(
        """
        SELECT COUNT(*)
        FROM processed_files
        WHERE date(datetime(timestamp, 'unixepoch', 'localtime')) BETWEEN ? AND ?
          AND file_path LIKE ?
          AND EXISTS (
              SELECT 1
              FROM processed_files preferred
              WHERE preferred.router = processed_files.router
                AND preferred.timestamp = processed_files.timestamp
                AND preferred.file_path LIKE ?
          )
        """,
        (args.start_date, end_date, remove_pattern, preferred_pattern),
    ).fetchone()[0]

    print(f"netflow_stats rows to delete: {delete_netflow}")
    print(f"processed_files rows to delete: {delete_processed}")

    if args.dry_run:
        return

    with conn:
        conn.execute(
            """
            DELETE FROM netflow_stats
            WHERE date(datetime(timestamp, 'unixepoch', 'localtime')) BETWEEN ? AND ?
              AND file_path LIKE ?
              AND EXISTS (
                  SELECT 1
                  FROM netflow_stats preferred
                  WHERE preferred.router = netflow_stats.router
                    AND preferred.timestamp = netflow_stats.timestamp
                    AND preferred.file_path LIKE ?
              )
            """,
            (args.start_date, end_date, remove_pattern, preferred_pattern),
        )
        conn.execute(
            """
            DELETE FROM processed_files
            WHERE date(datetime(timestamp, 'unixepoch', 'localtime')) BETWEEN ? AND ?
              AND file_path LIKE ?
              AND EXISTS (
                  SELECT 1
                  FROM processed_files preferred
                  WHERE preferred.router = processed_files.router
                    AND preferred.timestamp = processed_files.timestamp
                    AND preferred.file_path LIKE ?
              )
            """,
            (args.start_date, end_date, remove_pattern, preferred_pattern),
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_files_router_timestamp_unique
            ON processed_files(router, timestamp)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_netflow_router_timestamp_unique
            ON netflow_stats(router, timestamp)
            """
        )

    print("Repair complete")


if __name__ == '__main__':
    main()
