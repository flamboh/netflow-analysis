"""
Input tracking table for pipeline v2.

Tracks logical bucket-level inputs independently of legacy file-only assumptions.
"""

from __future__ import annotations

import sqlite3


VALID_STATUS_TABLES = {
    'netflow_stats_v2',
    'ip_stats_v2',
    'protocol_stats_v2',
    'structure_stats_v2',
    'spectrum_stats_v2',
    'dimension_stats_v2',
}


def init_processed_inputs_v2_table(conn: sqlite3.Connection) -> None:
    """Create the processed_inputs_v2 table if it does not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_inputs_v2 (
            input_kind TEXT NOT NULL CHECK (input_kind IN ('nfcapd', 'csv')),
            input_locator TEXT NOT NULL,
            source_id TEXT NOT NULL,
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            netflow_stats_v2_status INTEGER,
            ip_stats_v2_status INTEGER,
            protocol_stats_v2_status INTEGER,
            structure_stats_v2_status INTEGER,
            spectrum_stats_v2_status INTEGER,
            dimension_stats_v2_status INTEGER,
            PRIMARY KEY (input_kind, input_locator, source_id, bucket_start)
        ) WITHOUT ROWID
        """
    )
    for column in ('structure_stats_v2_status', 'spectrum_stats_v2_status', 'dimension_stats_v2_status'):
        ensure_column(conn, 'processed_inputs_v2', column, 'INTEGER')
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_processed_inputs_v2_source_bucket
        ON processed_inputs_v2(source_id, bucket_start)
        """
    )
    conn.commit()


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> None:
    """Add a column to an existing table when needed."""
    columns = {
        row[1]
        for row in conn.execute(f'PRAGMA table_info({table_name})').fetchall()
    }
    if column_name not in columns:
        conn.execute(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}')


def upsert_input_bucket(
    conn: sqlite3.Connection,
    *,
    input_kind: str,
    input_locator: str,
    source_id: str,
    bucket_start: int,
    bucket_end: int,
) -> None:
    """Insert or replace an input bucket record without committing."""
    init_processed_inputs_v2_table(conn)
    conn.execute(
        """
        INSERT INTO processed_inputs_v2 (
            input_kind, input_locator, source_id, bucket_start, bucket_end
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(input_kind, input_locator, source_id, bucket_start)
        DO UPDATE SET bucket_end = excluded.bucket_end
        """,
        (input_kind, input_locator, source_id, bucket_start, bucket_end),
    )


def get_pending_inputs(conn: sqlite3.Connection, table_name: str) -> list[dict]:
    """Return bucket inputs that have not been marked processed for the given v2 table."""
    status_column = get_status_column(table_name)
    rows = conn.execute(
        f"""
        SELECT input_kind, input_locator, source_id, bucket_start, bucket_end
        FROM processed_inputs_v2
        WHERE {status_column} IS NULL
        ORDER BY bucket_start, source_id, input_locator
        """
    ).fetchall()
    return [
        {
            'input_kind': row[0],
            'input_locator': row[1],
            'source_id': row[2],
            'bucket_start': row[3],
            'bucket_end': row[4],
        }
        for row in rows
    ]


def mark_input_bucket_processed(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    input_kind: str,
    input_locator: str,
    source_id: str,
    bucket_start: int,
    success: bool,
) -> None:
    """Mark the input bucket processed for one v2 table without committing."""
    status_column = get_status_column(table_name)
    conn.execute(
        f"""
        UPDATE processed_inputs_v2
        SET {status_column} = ?
        WHERE input_kind = ? AND input_locator = ? AND source_id = ? AND bucket_start = ?
        """,
        (1 if success else 0, input_kind, input_locator, source_id, bucket_start),
    )


def get_status_column(table_name: str) -> str:
    """Return the processed_inputs_v2 status column for the given table."""
    if table_name not in VALID_STATUS_TABLES:
        raise ValueError(f'Unsupported v2 status table: {table_name}')
    return f'{table_name}_status'
