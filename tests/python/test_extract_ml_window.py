import importlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest


def load_module():
    module = importlib.import_module('extract_ml_window')
    return importlib.reload(module)


def make_source_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE netflow_stats (timestamp INTEGER NOT NULL, router TEXT NOT NULL, flows INTEGER NOT NULL)'
    )
    conn.execute('CREATE INDEX idx_netflow_timestamp ON netflow_stats(timestamp)')
    conn.executemany(
        'INSERT INTO netflow_stats (timestamp, router, flows) VALUES (?, ?, ?)',
        [
            (100, 'r1', 10),
            (200, 'r1', 20),
            (400, 'r2', 40),
        ],
    )
    conn.commit()
    return conn


def test_compute_window_rejects_reversed_dates() -> None:
    module = load_module()
    with pytest.raises(SystemExit, match='--end-inclusive must be on or after --start'):
        module.compute_window('2025-03-02', '2025-03-01')


def test_copy_table_to_sqlite_preserves_schema_and_rows(tmp_path: Path) -> None:
    module = load_module()
    source_path = tmp_path / 'source.sqlite'
    dest_path = tmp_path / 'dest.sqlite'
    source_conn = make_source_db(source_path)
    dest_conn = module.connect_db(dest_path)

    module.create_sqlite_table(source_conn, dest_conn, 'netflow_stats')
    inserted = module.copy_table_to_sqlite(
        source_conn,
        dest_conn,
        'netflow_stats',
        'timestamp',
        150,
        500,
        2,
    )

    assert inserted == 2
    rows = dest_conn.execute(
        'SELECT timestamp, router, flows FROM netflow_stats ORDER BY timestamp'
    ).fetchall()
    assert [tuple(row) for row in rows] == [(200, 'r1', 20), (400, 'r2', 40)]


def test_collect_table_summary_and_manifest(tmp_path: Path) -> None:
    module = load_module()
    source_conn = make_source_db(tmp_path / 'summary.sqlite')

    summary = module.collect_table_summary(source_conn, 'netflow_stats', 'timestamp', 50, 250)
    manifest = module.build_manifest(
        source_db=tmp_path / 'summary.sqlite',
        output_dir=tmp_path / 'out',
        start_dt=datetime(2025, 3, 30),
        end_exclusive_dt=datetime(2025, 6, 8),
        start_ts=123,
        end_ts=456,
        tables={'netflow_stats': summary},
    )
    manifest_path = module.write_manifest(tmp_path, manifest)

    assert summary == {'row_count': 2, 'min_time': 100, 'max_time': 200}
    assert manifest['end_inclusive_date'] == '2025-06-07'
    assert manifest_path.read_text().endswith('\n')
    assert '"row_count": 2' in manifest_path.read_text()
