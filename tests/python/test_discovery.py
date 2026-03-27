import importlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest


def load_modules():
    common = importlib.import_module('common')
    discovery = importlib.import_module('discovery')
    return importlib.reload(common), importlib.reload(discovery)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2025, 3, 20, 14, 0, 0, tzinfo=tz)


def test_get_discovery_start_dt_respects_window(monkeypatch: pytest.MonkeyPatch) -> None:
    _, discovery = load_modules()
    monkeypatch.setattr(discovery, 'datetime', FixedDateTime)
    monkeypatch.setattr(discovery, 'DATA_START_DATE', datetime(2025, 3, 1))

    assert discovery.get_discovery_start_dt(0) == datetime(2025, 3, 1)
    assert discovery.get_discovery_start_dt(3) == datetime(2025, 3, 17)


def test_identify_gaps_returns_missing_intervals() -> None:
    common, discovery = load_modules()
    conn = sqlite3.connect(':memory:')
    common.init_processed_files_table(conn)

    start = datetime(2025, 3, 1, 0, 0)
    rows = [
        ('a', 'r1', common.timestamp_to_unix(start), 1),
        ('b', 'r1', common.timestamp_to_unix(datetime(2025, 3, 1, 0, 10)), 1),
    ]
    conn.executemany(
        'INSERT INTO processed_files (file_path, router, timestamp, file_exists) VALUES (?, ?, ?, ?)',
        rows,
    )

    gaps = discovery.identify_gaps(conn, 'r1', start, datetime(2025, 3, 1, 0, 15))

    assert gaps == [datetime(2025, 3, 1, 0, 5)]


def test_sync_processed_files_table_inserts_discoveries_and_gaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    common, discovery = load_modules()
    conn = sqlite3.connect(':memory:')
    common.init_processed_files_table(conn)

    monkeypatch.setattr(discovery, 'AVAILABLE_ROUTERS', ['r1'])
    monkeypatch.setattr(discovery, 'DATA_START_DATE', datetime(2025, 3, 1, 0, 0))
    monkeypatch.setattr(
        discovery,
        'scan_filesystem',
        lambda discovery_window_days=0: iter(
            [
                ('/captures/r1/2025/03/01/nfcapd.202503010000', 'r1', datetime(2025, 3, 1, 0, 0)),
                ('/captures/r1/2025/03/01/nfcapd.202503010010', 'r1', datetime(2025, 3, 1, 0, 10)),
            ]
        ),
    )
    monkeypatch.setattr(
        discovery,
        'construct_file_path',
        lambda router, timestamp: f"/captures/{router}/{timestamp.strftime('%Y/%m/%d')}/nfcapd.{timestamp.strftime('%Y%m%d%H%M')}",
    )

    stats = discovery.sync_processed_files_table(
        conn,
        include_gaps=True,
        reprocess_window_days=0,
        discovery_window_days=0,
    )

    assert stats == {'discovered': 2, 'new_files': 2, 'gaps': 1}
    rows = conn.execute(
        'SELECT file_path, timestamp, file_exists FROM processed_files ORDER BY timestamp'
    ).fetchall()
    assert rows == [
        ('/captures/r1/2025/03/01/nfcapd.202503010000', common.timestamp_to_unix(datetime(2025, 3, 1, 0, 0)), 1),
        ('/captures/r1/2025/03/01/nfcapd.202503010005', common.timestamp_to_unix(datetime(2025, 3, 1, 0, 5)), 0),
        ('/captures/r1/2025/03/01/nfcapd.202503010010', common.timestamp_to_unix(datetime(2025, 3, 1, 0, 10)), 1),
    ]


def test_scan_filesystem_skips_unparseable_and_pre_start_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, discovery = load_modules()
    root = tmp_path / 'captures' / 'r1' / '2025' / '03' / '01'
    root.mkdir(parents=True)
    (root / 'nfcapd.202503010000').write_text('')
    (root / 'nfcapd.invalid').write_text('')

    monkeypatch.setattr(discovery, 'NETFLOW_DATA_PATH', str(tmp_path / 'captures'))
    monkeypatch.setattr(discovery, 'AVAILABLE_ROUTERS', ['r1'])
    monkeypatch.setattr(discovery, 'DATA_START_DATE', datetime(2025, 3, 1, 0, 5))
    monkeypatch.setattr(
        discovery,
        'iter_scan_days',
        lambda discovery_window_days=0: iter([datetime(2025, 3, 1)]),
    )

    rows = list(discovery.scan_filesystem())

    assert rows == []


def test_get_stale_days_uses_local_day_boundaries() -> None:
    common, discovery = load_modules()
    conn = sqlite3.connect(':memory:')
    common.init_processed_files_table(conn)

    same_local_day = [
        (
            '/captures/r1/2025/03/05/nfcapd.202503050045',
            'r1',
            common.timestamp_to_unix(datetime(2025, 3, 5, 0, 45)),
            1,
        ),
        (
            '/captures/r1/2025/03/05/nfcapd.202503052355',
            'r1',
            common.timestamp_to_unix(datetime(2025, 3, 5, 23, 55)),
            None,
        ),
    ]
    conn.executemany(
        'INSERT INTO processed_files (file_path, router, timestamp, flow_stats_status) VALUES (?, ?, ?, ?)',
        same_local_day,
    )

    assert discovery.get_stale_days(conn, 'flow_stats') == {
        ('r1', common.timestamp_to_unix(datetime(2025, 3, 5, 0, 0)))
    }


def test_sync_processed_files_table_updates_mirrored_path_without_duplication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    common, discovery = load_modules()
    conn = sqlite3.connect(':memory:')
    common.init_processed_files_table(conn)

    ts = common.timestamp_to_unix(datetime(2025, 3, 2, 0, 0))
    conn.execute(
        'INSERT INTO processed_files (file_path, router, timestamp, file_exists) VALUES (?, ?, ?, ?)',
        ('/old-root/r1/2025/03/02/nfcapd.202503020000', 'r1', ts, 1),
    )

    monkeypatch.setattr(discovery, 'AVAILABLE_ROUTERS', ['r1'])
    monkeypatch.setattr(discovery, 'DATA_START_DATE', datetime(2025, 3, 1, 0, 0))
    monkeypatch.setattr(
        discovery,
        'scan_filesystem',
        lambda discovery_window_days=0: iter(
            [('/new-root/r1/2025/03/02/nfcapd.202503020000', 'r1', datetime(2025, 3, 2, 0, 0))]
        ),
    )

    stats = discovery.sync_processed_files_table(
        conn,
        include_gaps=False,
        reprocess_window_days=0,
        discovery_window_days=0,
    )

    row = conn.execute(
        'SELECT file_path, router, timestamp, file_exists FROM processed_files'
    ).fetchone()
    assert stats == {'discovered': 1, 'new_files': 0, 'gaps': 0}
    assert row == ('/new-root/r1/2025/03/02/nfcapd.202503020000', 'r1', ts, 1)
