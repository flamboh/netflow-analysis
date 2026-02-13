#!/usr/bin/env python3
"""
Migration script to rename router identifiers from dash to underscore and
drop deprecated tables.

This script:
1) Detects routers containing '-' across active tables
2) Updates router columns and file_path references to use '_'
3) Drops deprecated tables

Usage:
  python migrate_router_names.py --dry-run
  python migrate_router_names.py
  python migrate_router_names.py --verify-only
"""

import argparse
import sqlite3
import subprocess
from datetime import datetime

from common import DATABASE_PATH


ROUTER_TABLES = [
    "netflow_stats",
    "processed_files",
    "ip_stats",
    "protocol_stats",
    "spectrum_stats",
    "structure_stats",
]

FILE_PATH_TABLES = [
    "netflow_stats",
    "processed_files",
]

DROP_TABLES = [
    "spectrum_results",
    "structure_results",
    "old_ip_stats",
    "old_protocol_stats",
    "hll_ip_stats",
    "maad_metadata",
]


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def get_router_mappings(conn: sqlite3.Connection) -> dict[str, str]:
    routers = set()
    for table in ROUTER_TABLES:
        if not table_exists(conn, table):
            continue
        rows = conn.execute(
            f"SELECT DISTINCT router FROM {table} WHERE router LIKE '%-%'"
        ).fetchall()
        routers.update(row[0] for row in rows if row and row[0])
    return {router: router.replace("-", "_") for router in sorted(routers)}


def create_backup(db_path: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.backup-{timestamp}"

    result = subprocess.run(
        ["sqlite3", db_path],
        input=f".backup {backup_path}\n",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Backup failed: {result.stderr}")
    return backup_path


def count_router_updates(conn: sqlite3.Connection, mappings: dict[str, str]) -> dict:
    counts = {}
    for table in ROUTER_TABLES:
        if not table_exists(conn, table):
            continue
        total = 0
        for old in mappings:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE router = ?",
                (old,),
            ).fetchone()
            total += row[0] if row else 0
        counts[table] = total
    return counts


def count_file_path_updates(conn: sqlite3.Connection, mappings: dict[str, str]) -> dict:
    counts = {}
    for table in FILE_PATH_TABLES:
        if not table_exists(conn, table):
            continue
        total = 0
        for old in mappings:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE file_path LIKE ?",
                (f"%{old}%",),
            ).fetchone()
            total += row[0] if row else 0
        counts[table] = total
    return counts


def run_dry_run(conn: sqlite3.Connection):
    print("\n" + "=" * 60)
    print("DRY RUN - No changes will be made")
    print("=" * 60)

    mappings = get_router_mappings(conn)
    if not mappings:
        print("\nNo routers with '-' found in active tables.")
        return

    print("\nRouter mappings:")
    for old, new in mappings.items():
        print(f"  {old} -> {new}")

    print("\nRouter column updates (rows affected):")
    router_counts = count_router_updates(conn, mappings)
    for table, count in router_counts.items():
        print(f"  {table}: {count:,}")

    print("\nFile path updates (rows affected):")
    file_counts = count_file_path_updates(conn, mappings)
    for table, count in file_counts.items():
        print(f"  {table}: {count:,}")

    print("\nTables to drop:")
    for table in DROP_TABLES:
        exists = table_exists(conn, table)
        print(f"  {table}: {'exists' if exists else 'missing'}")


def run_migration(conn: sqlite3.Connection):
    print("\n" + "=" * 60)
    print("RUNNING MIGRATION")
    print("=" * 60)

    mappings = get_router_mappings(conn)
    if not mappings:
        print("\nNo routers with '-' found. Skipping rename step.")
    else:
        print("\nUpdating router columns...")
        for table in ROUTER_TABLES:
            if not table_exists(conn, table):
                continue
            total = 0
            for old, new in mappings.items():
                cursor = conn.execute(
                    f"UPDATE {table} SET router = ? WHERE router = ?",
                    (new, old),
                )
                total += cursor.rowcount if cursor.rowcount != -1 else 0
            print(f"  {table}: {total:,} rows updated")

        print("\nUpdating file_path columns...")
        for table in FILE_PATH_TABLES:
            if not table_exists(conn, table):
                continue
            total = 0
            for old, new in mappings.items():
                cursor = conn.execute(
                    f"UPDATE {table} SET file_path = REPLACE(file_path, ?, ?) "
                    "WHERE file_path LIKE ?",
                    (old, new, f"%{old}%"),
                )
                total += cursor.rowcount if cursor.rowcount != -1 else 0
            print(f"  {table}: {total:,} rows updated")

    print("\nDropping deprecated tables...")
    for table in DROP_TABLES:
        if table_exists(conn, table):
            conn.execute(f"DROP TABLE {table}")
            print(f"  Dropped {table}")
        else:
            print(f"  Skipped {table} (missing)")

    conn.commit()
    print("\nMigration complete!")


def run_verify(conn: sqlite3.Connection):
    print("\n" + "=" * 60)
    print("DATABASE VERIFICATION")
    print("=" * 60)

    mappings = get_router_mappings(conn)
    if mappings:
        print("\nRouters still containing '-' (should be empty):")
        for router in mappings:
            print(f"  {router}")
    else:
        print("\nNo routers with '-' found in active tables.")

    if mappings:
        print("\nFile paths still referencing old routers:")
        for table in FILE_PATH_TABLES:
            if not table_exists(conn, table):
                continue
            for old in mappings:
                row = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE file_path LIKE ?",
                    (f"%{old}%",),
                ).fetchone()
                count = row[0] if row else 0
                if count:
                    print(f"  {table}: {old} -> {count:,} rows")

    print("\nDeprecated tables present:")
    for table in DROP_TABLES:
        exists = table_exists(conn, table)
        print(f"  {table}: {'exists' if exists else 'missing'}")


def main():
    parser = argparse.ArgumentParser(
        description="Rename routers from dash to underscore and drop deprecated tables"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print(f"Database Migration - {datetime.now()}")
    print("=" * 60)
    print(f"Database: {DATABASE_PATH}")

    conn = sqlite3.connect(DATABASE_PATH)
    try:
        if args.verify_only:
            run_verify(conn)
            return
        if args.dry_run:
            run_dry_run(conn)
            return

        if not args.no_backup:
            backup_path = create_backup(DATABASE_PATH)
            print(f"Backup created: {backup_path}")

        conn.execute("BEGIN")
        run_migration(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
