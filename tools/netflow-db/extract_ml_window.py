#!/usr/bin/env python3
"""
Extract a fixed NetFlow training window into a slim SQLite DB and Parquet files.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


TABLE_CONFIG = {
    "netflow_stats": {"time_column": "timestamp", "extra_where": "", "extra_params": ()},
    "ip_stats": {"time_column": "bucket_start", "extra_where": "AND granularity = ?", "extra_params": ("5m",)},
    "protocol_stats": {"time_column": "bucket_start", "extra_where": "AND granularity = ?", "extra_params": ("5m",)},
    "spectrum_stats": {"time_column": "bucket_start", "extra_where": "AND granularity = ?", "extra_params": ("5m",)},
    "structure_stats": {"time_column": "bucket_start", "extra_where": "AND granularity = ?", "extra_params": ("5m",)},
}

DEFAULT_START = "2025-03-30"
DEFAULT_END_INCLUSIVE = "2025-06-07"
DEFAULT_OUTPUT_DIR = "data/uoregon/ml-2025-03-30-to-2025-06-07"
SQLITE_FILENAME = "netflow_window.sqlite"


def resolve_default_source_db() -> str:
    try:
        from common import get_dataset_db_path
    except Exception as error:
        raise SystemExit(f"Could not resolve default source DB: {error}") from error

    return str(get_dataset_db_path("uoregon"))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(
        description="Extract a fixed NetFlow window for ML workflows.",
    )
    parser.add_argument(
        "--router",
        help="Only extract rows for a single router.",
    )
    parser.add_argument(
        "--source-db",
        default=None,
        help="Path to the source SQLite database.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for the extracted SQLite, Parquet, and manifest outputs.",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        help="Inclusive window start date in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-inclusive",
        default=DEFAULT_END_INCLUSIVE,
        help="Inclusive window end date in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Fetch/insert batch size.",
    )
    parser.add_argument(
        "--skip-sqlite",
        action="store_true",
        help="Skip writing the extracted SQLite database.",
    )
    parser.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Skip writing Parquet files.",
    )
    parser.add_argument(
        "--all-granularities",
        action="store_true",
        help="Include non-5m rows from derived tables.",
    )

    if not argv:
        parser.print_help()
        raise SystemExit(0)

    args = parser.parse_args(argv)
    if args.source_db is None:
        args.source_db = resolve_default_source_db()
    return args


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def compute_window(start_date: str, end_inclusive: str) -> tuple[datetime, datetime, int, int]:
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_inclusive)
    if end_dt < start_dt:
        raise SystemExit("--end-inclusive must be on or after --start")

    end_exclusive_dt = end_dt + timedelta(days=1)
    return start_dt, end_exclusive_dt, int(start_dt.timestamp()), int(end_exclusive_dt.timestamp())


def ensure_output_dir(path_str: str) -> Path:
    output_dir = Path(path_str).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def build_table_filters(
    *,
    router: str | None,
    all_granularities: bool,
    config: dict[str, Any],
) -> tuple[str, tuple[Any, ...]]:
    clauses: list[str] = []
    params: list[Any] = []

    if not all_granularities and config["extra_where"]:
        clauses.append(str(config["extra_where"]).removeprefix("AND ").strip())
        params.extend(config["extra_params"])

    if router is not None:
        clauses.append("router = ?")
        params.append(router)

    extra_where = ""
    if clauses:
        extra_where = "AND " + " AND ".join(clauses)

    return extra_where, tuple(params)


def get_table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if not rows:
        raise RuntimeError(f"Table not found or has no columns: {table}")
    return [str(row["name"]) for row in rows]


def create_sqlite_table(source_conn: sqlite3.Connection, dest_conn: sqlite3.Connection, table: str) -> None:
    schema_rows = source_conn.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE tbl_name = ?
          AND type IN ('table', 'index')
          AND sql IS NOT NULL
        ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END, name
        """,
        (table,),
    ).fetchall()

    if not schema_rows:
        raise RuntimeError(f"Missing schema for table: {table}")

    for row in schema_rows:
        dest_conn.execute(str(row["sql"]))
    dest_conn.commit()


def iter_table_batches(
    conn: sqlite3.Connection,
    table: str,
    time_column: str,
    start_ts: int,
    end_ts: int,
    batch_size: int,
    extra_where: str = "",
    extra_params: tuple[Any, ...] = (),
):
    cursor = conn.execute(
        f"""
        SELECT *
        FROM {table}
        WHERE {time_column} >= ? AND {time_column} < ?
        {extra_where}
        """,
        (start_ts, end_ts, *extra_params),
    )

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows


def copy_table_to_sqlite(
    source_conn: sqlite3.Connection,
    dest_conn: sqlite3.Connection,
    table: str,
    time_column: str,
    start_ts: int,
    end_ts: int,
    batch_size: int,
    extra_where: str = "",
    extra_params: tuple[Any, ...] = (),
) -> int:
    columns = get_table_columns(source_conn, table)
    column_list = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    inserted = 0

    for rows in iter_table_batches(
        source_conn,
        table,
        time_column,
        start_ts,
        end_ts,
        batch_size,
        extra_where=extra_where,
        extra_params=extra_params,
    ):
        payload = [tuple(row[column] for column in columns) for row in rows]
        dest_conn.executemany(
            f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})",
            payload,
        )
        inserted += len(payload)

    dest_conn.commit()
    return inserted


def export_table_to_parquet(
    source_conn: sqlite3.Connection,
    output_path: Path,
    table: str,
    time_column: str,
    start_ts: int,
    end_ts: int,
    batch_size: int,
    extra_where: str = "",
    extra_params: tuple[Any, ...] = (),
) -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as error:
        raise SystemExit(
            "Parquet export requires pyarrow. Install it in the active environment and rerun."
        ) from error
    except ImportError as error:
        raise SystemExit(
            "PyArrow is installed but could not load its native runtime dependencies. "
            f"Original error: {error}"
        ) from error

    writer = None
    written = 0

    try:
        for rows in iter_table_batches(
            source_conn,
            table,
            time_column,
            start_ts,
            end_ts,
            batch_size,
            extra_where=extra_where,
            extra_params=extra_params,
        ):
            records = [dict(row) for row in rows]
            batch_table = pa.Table.from_pylist(records)
            if writer is None:
                writer = pq.ParquetWriter(output_path, batch_table.schema)
            writer.write_table(batch_table)
            written += len(records)

        if writer is None:
            empty_columns = get_table_columns(source_conn, table)
            empty_table = pa.table({column: [] for column in empty_columns})
            pq.write_table(empty_table, output_path)
            return 0

        return written
    finally:
        if writer is not None:
            writer.close()


def collect_table_summary(
    conn: sqlite3.Connection,
    table: str,
    time_column: str,
    start_ts: int,
    end_ts: int,
    extra_where: str = "",
    extra_params: tuple[Any, ...] = (),
) -> dict[str, Any]:
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS row_count,
               MIN({time_column}) AS min_time,
               MAX({time_column}) AS max_time
        FROM {table}
        WHERE {time_column} >= ? AND {time_column} < ?
        {extra_where}
        """,
        (start_ts, end_ts, *extra_params),
    ).fetchone()

    return {
        "row_count": int(row["row_count"]),
        "min_time": row["min_time"],
        "max_time": row["max_time"],
    }


def build_manifest(
    source_db: Path,
    output_dir: Path,
    start_dt: datetime,
    end_exclusive_dt: datetime,
    start_ts: int,
    end_ts: int,
    tables: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "source_db": str(source_db),
        "output_dir": str(output_dir),
        "start_date": start_dt.strftime("%Y-%m-%d"),
        "end_inclusive_date": (end_exclusive_dt - timedelta(days=1)).strftime("%Y-%m-%d"),
        "end_exclusive_date": end_exclusive_dt.strftime("%Y-%m-%d"),
        "start_ts": start_ts,
        "end_exclusive_ts": end_ts,
        "tables": tables,
    }


def write_manifest(output_dir: Path, manifest: dict[str, Any]) -> Path:
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest_path


def main() -> None:
    args = parse_args()
    if args.skip_sqlite and args.skip_parquet:
        raise SystemExit("At least one output must be enabled.")

    source_db = Path(args.source_db).expanduser().resolve()
    if not source_db.exists():
        raise SystemExit(f"Source DB not found: {source_db}")

    output_dir = ensure_output_dir(args.output_dir)
    parquet_dir = output_dir / "parquet"
    if not args.skip_parquet:
        parquet_dir.mkdir(parents=True, exist_ok=True)

    start_dt, end_exclusive_dt, start_ts, end_ts = compute_window(args.start, args.end_inclusive)

    manifest_tables: dict[str, dict[str, Any]] = {}
    sqlite_output_path = output_dir / SQLITE_FILENAME

    with connect_db(source_db) as source_conn:
        dest_conn = None
        try:
            if not args.skip_sqlite:
                if sqlite_output_path.exists():
                    sqlite_output_path.unlink()
                dest_conn = connect_db(sqlite_output_path)

            for table, config in TABLE_CONFIG.items():
                time_column = str(config["time_column"])
                extra_where, extra_params = build_table_filters(
                    router=args.router,
                    all_granularities=args.all_granularities,
                    config=config,
                )
                granularity_filter = None
                if not args.all_granularities and config["extra_params"]:
                    granularity_filter = config["extra_params"][0]
                summary = collect_table_summary(
                    source_conn,
                    table,
                    time_column,
                    start_ts,
                    end_ts,
                    extra_where=extra_where,
                    extra_params=extra_params,
                )

                table_manifest = {
                    "time_column": time_column,
                    "router_filter": args.router,
                    "granularity_filter": granularity_filter,
                    "source_row_count": summary["row_count"],
                    "source_min_time": summary["min_time"],
                    "source_max_time": summary["max_time"],
                }

                if dest_conn is not None:
                    create_sqlite_table(source_conn, dest_conn, table)
                    sqlite_rows = copy_table_to_sqlite(
                        source_conn,
                        dest_conn,
                        table,
                        time_column,
                        start_ts,
                        end_ts,
                        args.batch_size,
                        extra_where=extra_where,
                        extra_params=extra_params,
                    )
                    table_manifest["sqlite_row_count"] = sqlite_rows

                if not args.skip_parquet:
                    parquet_path = parquet_dir / f"{table}.parquet"
                    parquet_rows = export_table_to_parquet(
                        source_conn,
                        parquet_path,
                        table,
                        time_column,
                        start_ts,
                        end_ts,
                        args.batch_size,
                        extra_where=extra_where,
                        extra_params=extra_params,
                    )
                    table_manifest["parquet_row_count"] = parquet_rows
                    table_manifest["parquet_path"] = str(parquet_path)

                manifest_tables[table] = table_manifest
                print(
                    f"[extract] {table}: source={table_manifest['source_row_count']}"
                    + (
                        f", sqlite={table_manifest['sqlite_row_count']}"
                        if "sqlite_row_count" in table_manifest
                        else ""
                    )
                    + (
                        f", parquet={table_manifest['parquet_row_count']}"
                        if "parquet_row_count" in table_manifest
                        else ""
                    )
                )
        finally:
            if dest_conn is not None:
                dest_conn.close()

    manifest = build_manifest(
        source_db=source_db,
        output_dir=output_dir,
        start_dt=start_dt,
        end_exclusive_dt=end_exclusive_dt,
        start_ts=start_ts,
        end_ts=end_ts,
        tables=manifest_tables,
    )
    manifest_path = write_manifest(output_dir, manifest)

    print(f"[extract] Source DB: {source_db}")
    if not args.skip_sqlite:
        print(f"[extract] SQLite output: {sqlite_output_path}")
    if not args.skip_parquet:
        print(f"[extract] Parquet output dir: {parquet_dir}")
    print(f"[extract] Manifest: {manifest_path}")
    print(
        "[extract] Window: "
        f"{start_dt.strftime('%Y-%m-%d')} <= ts < {end_exclusive_dt.strftime('%Y-%m-%d')}"
    )


if __name__ == "__main__":
    main()
