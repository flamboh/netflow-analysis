"""
Local-only uoregon v2 validation.

Run explicitly; default pytest discovery does not collect this file:

RUN_LOCAL_NETFLOW_V2=1 \
  ./scripts/run-with-nix-if-available.sh uv run --extra test pytest \
  tests/local_only/uoregon_v2_validation.py
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest


V1_DB = Path(os.environ.get('LOCAL_UOREGON_V1_DB', 'data/uoregon/netflow.sqlite'))
V2_DB = Path(os.environ.get('LOCAL_UOREGON_V2_DB', 'data/uoregon-v2/netflow.sqlite'))
SOURCE_ID = os.environ.get('LOCAL_UOREGON_SOURCE_ID', 'oh_ir1_gw')
WINDOW_START = int(os.environ.get('LOCAL_UOREGON_WINDOW_START', '1741075200'))
WINDOW_END = int(os.environ.get('LOCAL_UOREGON_WINDOW_END', '1741075500'))
GRANULARITIES = tuple(
    granularity.strip()
    for granularity in os.environ.get('LOCAL_UOREGON_GRANULARITIES', '5m').split(',')
    if granularity.strip()
)


def require_local_validation() -> None:
    if os.environ.get('RUN_LOCAL_NETFLOW_V2') != '1':
        pytest.skip('local-only validation disabled; set RUN_LOCAL_NETFLOW_V2=1')
    if not V1_DB.exists():
        pytest.skip(f'v1 database not found: {V1_DB}')
    if not V2_DB.exists():
        pytest.skip(f'v2 database not found: {V2_DB}')


@pytest.fixture
def conn() -> sqlite3.Connection:
    require_local_validation()
    connection = sqlite3.connect(':memory:')
    connection.execute('ATTACH DATABASE ? AS v1', (str(V1_DB),))
    connection.execute('ATTACH DATABASE ? AS v2', (str(V2_DB),))
    try:
        yield connection
    finally:
        connection.close()


def test_v2_netflow_5m_totals_match_v1(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT
            v1.timestamp,
            v1.flows,
            v1.packets,
            v1.bytes,
            COALESCE(SUM(v2.flows), 0),
            COALESCE(SUM(v2.packets), 0),
            COALESCE(SUM(v2.bytes), 0)
        FROM v1.netflow_stats v1
        LEFT JOIN v2.netflow_stats_v2 v2
          ON v2.source_id = v1.router
         AND v2.bucket_start = v1.timestamp
        WHERE v1.router = ?
          AND v1.timestamp >= ?
          AND v1.timestamp < ?
        GROUP BY v1.timestamp
        ORDER BY v1.timestamp
        """,
        (SOURCE_ID, WINDOW_START, WINDOW_END),
    ).fetchall()

    assert rows
    assert [(row[0], row[1], row[2], row[3]) for row in rows] == [
        (row[0], row[4], row[5], row[6]) for row in rows
    ]


def test_v2_ip_stats_match_v1(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        f"""
        SELECT
            v1.granularity,
            v1.bucket_start,
            v1.sa_ipv4_count,
            v1.da_ipv4_count,
            v1.sa_ipv6_count,
            v1.da_ipv6_count,
            v2.sa_ipv4_count,
            v2.da_ipv4_count,
            v2.sa_ipv6_count,
            v2.da_ipv6_count
        FROM v1.ip_stats v1
        JOIN v2.ip_stats_v2 v2
          ON v2.source_id = v1.router
         AND v2.granularity = v1.granularity
         AND v2.bucket_start = v1.bucket_start
        WHERE v1.router = ?
          AND v1.bucket_start >= ?
          AND v1.bucket_start < ?
          AND v1.granularity IN ({granularity_placeholders()})
        ORDER BY v1.granularity, v1.bucket_start
        """,
        (SOURCE_ID, WINDOW_START, WINDOW_END, *GRANULARITIES),
    ).fetchall()

    assert rows
    assert [row[:6] for row in rows] == [
        (row[0], row[1], row[6], row[7], row[8], row[9]) for row in rows
    ]


def test_v2_protocol_stats_match_v1(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        f"""
        SELECT
            v1.granularity,
            v1.bucket_start,
            v1.unique_protocols_count_ipv4,
            v1.unique_protocols_count_ipv6,
            v1.protocols_list_ipv4,
            v1.protocols_list_ipv6,
            v2.unique_protocols_count_ipv4,
            v2.unique_protocols_count_ipv6,
            v2.protocols_list_ipv4,
            v2.protocols_list_ipv6
        FROM v1.protocol_stats v1
        JOIN v2.protocol_stats_v2 v2
          ON v2.source_id = v1.router
         AND v2.granularity = v1.granularity
         AND v2.bucket_start = v1.bucket_start
        WHERE v1.router = ?
          AND v1.bucket_start >= ?
          AND v1.bucket_start < ?
          AND v1.granularity IN ({granularity_placeholders()})
        ORDER BY v1.granularity, v1.bucket_start
        """,
        (SOURCE_ID, WINDOW_START, WINDOW_END, *GRANULARITIES),
    ).fetchall()

    assert rows
    assert [row[:6] for row in rows] == [
        (row[0], row[1], row[6], row[7], row[8], row[9]) for row in rows
    ]


def granularity_placeholders() -> str:
    return ','.join('?' for _ in GRANULARITIES)
