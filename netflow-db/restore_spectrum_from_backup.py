#!/usr/bin/env python3
"""
Restore spectrum_stats rows from backup for selected bucket months.

Workflow:
1. Delete suspect rows from the current DB:
   - processed_at in a specific month (default: 2026-02)
   - bucket month in target months
2. Reset processed_files status for matching 5m buckets.
3. Restore selected bucket months from backup, but only where rows are missing
   or payload differs.
4. Re-mark restored 5m rows as processed and recompute processed_at when all
   table statuses are present.
"""

import argparse
import sqlite3
from dataclasses import dataclass

from common import DATABASE_PATH


@dataclass
class Counts:
    suspect_rows: int = 0
    suspect_5m_rows: int = 0
    pf_rows_reset: int = 0
    backup_rows_in_scope: int = 0
    backup_rows_missing_current: int = 0
    backup_rows_different_current: int = 0
    restored_5m_pf_rows: int = 0
    hyphen_router_rows_after: int = 0


def month_clause(months: list[str]) -> str:
    quoted = ",".join(f"'{m}'" for m in months)
    return f"({quoted})"


def gather_counts(conn: sqlite3.Connection, backup_path: str, target_months: list[str], processed_month: str) -> Counts:
    cursor = conn.cursor()
    months_sql = month_clause(target_months)
    like_value = f"{processed_month}%"
    counts = Counts()

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM spectrum_stats
        WHERE processed_at LIKE ?
          AND substr(datetime(bucket_start, 'unixepoch'), 1, 7) IN {months_sql}
        """,
        (like_value,),
    )
    counts.suspect_rows = cursor.fetchone()[0]

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM spectrum_stats
        WHERE processed_at LIKE ?
          AND granularity = '5m'
          AND ip_version = 4
          AND substr(datetime(bucket_start, 'unixepoch'), 1, 7) IN {months_sql}
        """,
        (like_value,),
    )
    counts.suspect_5m_rows = cursor.fetchone()[0]

    cursor.execute(f"ATTACH DATABASE ? AS old", (backup_path,))
    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM old.spectrum_stats
        WHERE substr(datetime(bucket_start, 'unixepoch'), 1, 7) IN {months_sql}
        """,
    )
    counts.backup_rows_in_scope = cursor.fetchone()[0]

    cursor.execute(
        f"""
        WITH restore_source AS (
            SELECT
                REPLACE(router, '-', '_') AS router,
                granularity,
                bucket_start,
                ip_version,
                bucket_end,
                spectrum_json_sa,
                spectrum_json_da,
                processed_at
            FROM old.spectrum_stats
            WHERE substr(datetime(bucket_start, 'unixepoch'), 1, 7) IN {months_sql}
        )
        SELECT COUNT(*)
        FROM restore_source rs
        LEFT JOIN spectrum_stats cur
          ON cur.router = rs.router
         AND cur.granularity = rs.granularity
         AND cur.bucket_start = rs.bucket_start
         AND cur.ip_version = rs.ip_version
        WHERE cur.router IS NULL
        """
    )
    counts.backup_rows_missing_current = cursor.fetchone()[0]

    cursor.execute(
        f"""
        WITH restore_source AS (
            SELECT
                REPLACE(router, '-', '_') AS router,
                granularity,
                bucket_start,
                ip_version,
                bucket_end,
                spectrum_json_sa,
                spectrum_json_da,
                processed_at
            FROM old.spectrum_stats
            WHERE substr(datetime(bucket_start, 'unixepoch'), 1, 7) IN {months_sql}
        )
        SELECT COUNT(*)
        FROM restore_source rs
        JOIN spectrum_stats cur
          ON cur.router = rs.router
         AND cur.granularity = rs.granularity
         AND cur.bucket_start = rs.bucket_start
         AND cur.ip_version = rs.ip_version
        WHERE cur.bucket_end != rs.bucket_end
           OR cur.spectrum_json_sa != rs.spectrum_json_sa
           OR cur.spectrum_json_da != rs.spectrum_json_da
           OR IFNULL(cur.processed_at, '') != IFNULL(rs.processed_at, '')
        """
    )
    counts.backup_rows_different_current = cursor.fetchone()[0]
    cursor.execute("DETACH DATABASE old")

    return counts


def apply_restore(conn: sqlite3.Connection, backup_path: str, target_months: list[str], processed_month: str) -> Counts:
    counts = gather_counts(conn, backup_path, target_months, processed_month)
    cursor = conn.cursor()
    months_sql = month_clause(target_months)
    like_value = f"{processed_month}%"

    cursor.execute(f"ATTACH DATABASE ? AS old", (backup_path,))
    cursor.execute("BEGIN IMMEDIATE")

    try:
        cursor.execute(
            f"""
            CREATE TEMP TABLE suspect_5m AS
            SELECT router, bucket_start
            FROM spectrum_stats
            WHERE processed_at LIKE ?
              AND granularity = '5m'
              AND ip_version = 4
              AND substr(datetime(bucket_start, 'unixepoch'), 1, 7) IN {months_sql}
            """,
            (like_value,),
        )

        cursor.execute(
            f"""
            UPDATE processed_files
            SET spectrum_stats_status = NULL,
                processed_at = NULL
            WHERE EXISTS (
                SELECT 1
                FROM suspect_5m s
                WHERE s.router = processed_files.router
                  AND s.bucket_start = processed_files.timestamp
            )
            """
        )
        counts.pf_rows_reset = cursor.rowcount

        cursor.execute(
            f"""
            DELETE FROM spectrum_stats
            WHERE processed_at LIKE ?
              AND substr(datetime(bucket_start, 'unixepoch'), 1, 7) IN {months_sql}
            """,
            (like_value,),
        )

        cursor.execute(
            f"""
            CREATE TEMP TABLE restore_source AS
            SELECT
                REPLACE(router, '-', '_') AS router,
                granularity,
                bucket_start,
                ip_version,
                bucket_end,
                spectrum_json_sa,
                spectrum_json_da,
                processed_at
            FROM old.spectrum_stats
            WHERE substr(datetime(bucket_start, 'unixepoch'), 1, 7) IN {months_sql}
            """
        )
        cursor.execute(
            """
            CREATE INDEX idx_restore_source_pk
            ON restore_source(router, granularity, bucket_start, ip_version)
            """
        )

        if counts.backup_rows_different_current > 0:
            cursor.execute(
                """
                CREATE TEMP TABLE restore_diff AS
                SELECT
                    cur.rowid AS rowid,
                    rs.bucket_end AS bucket_end,
                    rs.spectrum_json_sa AS spectrum_json_sa,
                    rs.spectrum_json_da AS spectrum_json_da,
                    rs.processed_at AS processed_at
                FROM spectrum_stats cur
                JOIN restore_source rs
                  ON rs.router = cur.router
                 AND rs.granularity = cur.granularity
                 AND rs.bucket_start = cur.bucket_start
                 AND rs.ip_version = cur.ip_version
                WHERE cur.bucket_end != rs.bucket_end
                   OR cur.spectrum_json_sa != rs.spectrum_json_sa
                   OR cur.spectrum_json_da != rs.spectrum_json_da
                   OR IFNULL(cur.processed_at, '') != IFNULL(rs.processed_at, '')
                """
            )
            cursor.execute("CREATE INDEX idx_restore_diff_rowid ON restore_diff(rowid)")
            cursor.execute(
                """
                UPDATE spectrum_stats
                SET
                    bucket_end = (SELECT d.bucket_end FROM restore_diff d WHERE d.rowid = spectrum_stats.rowid),
                    spectrum_json_sa = (SELECT d.spectrum_json_sa FROM restore_diff d WHERE d.rowid = spectrum_stats.rowid),
                    spectrum_json_da = (SELECT d.spectrum_json_da FROM restore_diff d WHERE d.rowid = spectrum_stats.rowid),
                    processed_at = (SELECT d.processed_at FROM restore_diff d WHERE d.rowid = spectrum_stats.rowid)
                WHERE rowid IN (SELECT rowid FROM restore_diff)
                """
            )

        cursor.execute(
            """
            INSERT INTO spectrum_stats (
                router,
                granularity,
                bucket_start,
                bucket_end,
                ip_version,
                spectrum_json_sa,
                spectrum_json_da,
                processed_at
            )
            SELECT
                rs.router,
                rs.granularity,
                rs.bucket_start,
                rs.bucket_end,
                rs.ip_version,
                rs.spectrum_json_sa,
                rs.spectrum_json_da,
                rs.processed_at
            FROM restore_source rs
            WHERE NOT EXISTS (
                SELECT 1
                FROM spectrum_stats cur
                WHERE cur.router = rs.router
                  AND cur.granularity = rs.granularity
                  AND cur.bucket_start = rs.bucket_start
                  AND cur.ip_version = rs.ip_version
            )
            """
        )

        cursor.execute(
            """
            UPDATE processed_files
            SET spectrum_stats_status = 1
            WHERE EXISTS (
                SELECT 1
                FROM restore_source rs
                WHERE rs.granularity = '5m'
                  AND rs.ip_version = 4
                  AND rs.router = processed_files.router
                  AND rs.bucket_start = processed_files.timestamp
            )
            """
        )
        counts.restored_5m_pf_rows = cursor.rowcount

        cursor.execute(
            """
            UPDATE processed_files
            SET processed_at = CURRENT_TIMESTAMP
            WHERE flow_stats_status IS NOT NULL
              AND ip_stats_status IS NOT NULL
              AND protocol_stats_status IS NOT NULL
              AND spectrum_stats_status IS NOT NULL
              AND structure_stats_status IS NOT NULL
            """
        )

        cursor.execute("COMMIT")
    except Exception:
        cursor.execute("ROLLBACK")
        cursor.execute("DETACH DATABASE old")
        raise

    cursor.execute(
        """
        SELECT COUNT(*) FROM spectrum_stats
        WHERE router LIKE '%-%'
        """
    )
    counts.hyphen_router_rows_after = cursor.fetchone()[0]

    cursor.execute("DETACH DATABASE old")
    return counts


def print_counts(title: str, counts: Counts) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    print(f"suspect_rows: {counts.suspect_rows}")
    print(f"suspect_5m_rows: {counts.suspect_5m_rows}")
    print(f"processed_files_rows_reset: {counts.pf_rows_reset}")
    print(f"backup_rows_in_scope: {counts.backup_rows_in_scope}")
    print(f"backup_rows_missing_current: {counts.backup_rows_missing_current}")
    print(f"backup_rows_different_current: {counts.backup_rows_different_current}")
    print(f"restored_5m_processed_files_rows: {counts.restored_5m_pf_rows}")
    print(f"hyphen_router_rows_after: {counts.hyphen_router_rows_after}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore spectrum_stats rows from backup")
    parser.add_argument(
        "--backup-path",
        default="netflow-db/flowStats.db.backup-20260203-115729",
        help="Path to backup SQLite DB",
    )
    parser.add_argument(
        "--processed-month",
        default="2026-02",
        help="Processed month prefix to purge first, e.g. 2026-02",
    )
    parser.add_argument(
        "--bucket-months",
        default="2025-10,2025-11,2025-12,2026-01",
        help="Comma-separated bucket months to purge/restore",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply purge+restore. Without this flag, runs dry-run only.",
    )
    args = parser.parse_args()

    target_months = [m.strip() for m in args.bucket_months.split(",") if m.strip()]
    if not target_months:
        raise ValueError("No bucket months provided")

    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA temp_store=MEMORY")
        dry_counts = gather_counts(conn, args.backup_path, target_months, args.processed_month)
        print_counts("Dry Run", dry_counts)

        if not args.apply:
            return

        applied_counts = apply_restore(conn, args.backup_path, target_months, args.processed_month)
        print_counts("Applied", applied_counts)


if __name__ == "__main__":
    main()
