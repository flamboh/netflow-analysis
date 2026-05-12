"""Dataset metadata table shared by pipeline output and the web app."""

from __future__ import annotations

import sqlite3
from typing import Any


def init_datasets_table(conn: sqlite3.Connection) -> None:
    """Create the dataset metadata table if it does not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS datasets (
            id TEXT PRIMARY KEY NOT NULL,
            label TEXT NOT NULL,
            default_start_date TEXT NOT NULL,
            source_mode TEXT NOT NULL DEFAULT 'static',
            discovery_mode TEXT NOT NULL DEFAULT 'static',
            sort_order INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()


def upsert_dataset_metadata(conn: sqlite3.Connection, dataset: dict[str, Any]) -> None:
    """Insert or update one dataset metadata row."""
    init_datasets_table(conn)
    dataset_id = str(dataset['dataset_id']).strip()
    label = str(dataset.get('label') or dataset_id)
    default_start_date = str(dataset.get('default_start_date') or '2025-02-01').strip()
    source_mode = str(dataset.get('source_mode') or 'static').strip()
    discovery_mode = str(dataset.get('discovery_mode') or 'static').strip()
    sort_order = int(dataset.get('sort_order') or 0)

    conn.execute(
        """
        INSERT INTO datasets (
            id,
            label,
            default_start_date,
            source_mode,
            discovery_mode,
            sort_order
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            label = excluded.label,
            default_start_date = excluded.default_start_date,
            source_mode = excluded.source_mode,
            discovery_mode = excluded.discovery_mode,
            sort_order = excluded.sort_order
        """,
        (dataset_id, label, default_start_date, source_mode, discovery_mode, sort_order),
    )
    conn.commit()
