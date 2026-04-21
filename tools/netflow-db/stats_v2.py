"""
5-minute aggregate tables for pipeline v2.

This slice writes netflow, IP, and protocol summaries over normalized rows.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from normalized_rows_v2 import NormalizedRow


def init_netflow_stats_v2_table(conn: sqlite3.Connection) -> None:
    """Create the netflow_stats_v2 table if it does not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS netflow_stats_v2 (
            source_id TEXT NOT NULL,
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            ip_version INTEGER NOT NULL CHECK (ip_version IN (4, 6)),
            flows INTEGER NOT NULL,
            flows_tcp INTEGER NOT NULL,
            flows_udp INTEGER NOT NULL,
            flows_icmp INTEGER NOT NULL,
            flows_other INTEGER NOT NULL,
            packets INTEGER NOT NULL,
            packets_tcp INTEGER NOT NULL,
            packets_udp INTEGER NOT NULL,
            packets_icmp INTEGER NOT NULL,
            packets_other INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            bytes_tcp INTEGER NOT NULL,
            bytes_udp INTEGER NOT NULL,
            bytes_icmp INTEGER NOT NULL,
            bytes_other INTEGER NOT NULL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_id, bucket_start, ip_version)
        ) WITHOUT ROWID
        """
    )
    conn.commit()


def init_ip_stats_v2_table(conn: sqlite3.Connection) -> None:
    """Create the ip_stats_v2 table if it does not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ip_stats_v2 (
            source_id TEXT NOT NULL,
            granularity TEXT NOT NULL CHECK (granularity IN ('5m', '30m', '1h', '1d')),
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            sa_ipv4_count INTEGER NOT NULL,
            da_ipv4_count INTEGER NOT NULL,
            sa_ipv6_count INTEGER NOT NULL,
            da_ipv6_count INTEGER NOT NULL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_id, granularity, bucket_start)
        ) WITHOUT ROWID
        """
    )
    conn.commit()


def init_protocol_stats_v2_table(conn: sqlite3.Connection) -> None:
    """Create the protocol_stats_v2 table if it does not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS protocol_stats_v2 (
            source_id TEXT NOT NULL,
            granularity TEXT NOT NULL CHECK (granularity IN ('5m', '30m', '1h', '1d')),
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            unique_protocols_count_ipv4 INTEGER NOT NULL,
            unique_protocols_count_ipv6 INTEGER NOT NULL,
            protocols_list_ipv4 TEXT NOT NULL,
            protocols_list_ipv6 TEXT NOT NULL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_id, granularity, bucket_start)
        ) WITHOUT ROWID
        """
    )
    conn.commit()


def build_netflow_stats_v2_rows(rows: list[NormalizedRow]) -> list[dict]:
    """Aggregate normalized rows into netflow_stats_v2 rows."""
    buckets: dict[tuple[str, int, int, int], dict] = {}
    for row in rows:
        key = (row.source_id, row.bucket_start, row.bucket_end, row.ip_version)
        bucket = buckets.setdefault(
            key,
            {
                'source_id': row.source_id,
                'bucket_start': row.bucket_start,
                'bucket_end': row.bucket_end,
                'ip_version': row.ip_version,
                'flows': 0,
                'flows_tcp': 0,
                'flows_udp': 0,
                'flows_icmp': 0,
                'flows_other': 0,
                'packets': 0,
                'packets_tcp': 0,
                'packets_udp': 0,
                'packets_icmp': 0,
                'packets_other': 0,
                'bytes': 0,
                'bytes_tcp': 0,
                'bytes_udp': 0,
                'bytes_icmp': 0,
                'bytes_other': 0,
            },
        )
        bucket['flows'] += 1
        bucket['packets'] += row.packets
        bucket['bytes'] += row.bytes
        flow_key, packets_key, bytes_key = protocol_metric_keys(row.protocol)
        bucket[flow_key] += 1
        bucket[packets_key] += row.packets
        bucket[bytes_key] += row.bytes
    return sorted(buckets.values(), key=lambda row: (row['bucket_start'], row['source_id'], row['ip_version']))


def build_ip_stats_v2_rows(rows: list[NormalizedRow]) -> list[dict]:
    """Aggregate normalized rows into ip_stats_v2 rows."""
    buckets = defaultdict(
        lambda: {
            'sa_ipv4': set(),
            'da_ipv4': set(),
            'sa_ipv6': set(),
            'da_ipv6': set(),
        }
    )
    bucket_bounds: dict[tuple[str, int], int] = {}
    for row in rows:
        key = (row.source_id, row.bucket_start)
        bucket_bounds[key] = row.bucket_end
        bucket = buckets[key]
        if row.ip_version == 4:
            bucket['sa_ipv4'].add(row.src_ip)
            bucket['da_ipv4'].add(row.dst_ip)
        else:
            bucket['sa_ipv6'].add(row.src_ip)
            bucket['da_ipv6'].add(row.dst_ip)
    result = []
    for (source_id, bucket_start), bucket in sorted(buckets.items()):
        result.append(
            {
                'source_id': source_id,
                'granularity': '5m',
                'bucket_start': bucket_start,
                'bucket_end': bucket_bounds[(source_id, bucket_start)],
                'sa_ipv4_count': len(bucket['sa_ipv4']),
                'da_ipv4_count': len(bucket['da_ipv4']),
                'sa_ipv6_count': len(bucket['sa_ipv6']),
                'da_ipv6_count': len(bucket['da_ipv6']),
            }
        )
    return result


def build_protocol_stats_v2_rows(rows: list[NormalizedRow]) -> list[dict]:
    """Aggregate normalized rows into protocol_stats_v2 rows."""
    buckets = defaultdict(lambda: {'ipv4': set(), 'ipv6': set()})
    bucket_bounds: dict[tuple[str, int], int] = {}
    for row in rows:
        key = (row.source_id, row.bucket_start)
        bucket_bounds[key] = row.bucket_end
        family_key = 'ipv4' if row.ip_version == 4 else 'ipv6'
        buckets[key][family_key].add(str(row.protocol))
    result = []
    for (source_id, bucket_start), bucket in sorted(buckets.items()):
        result.append(
            {
                'source_id': source_id,
                'granularity': '5m',
                'bucket_start': bucket_start,
                'bucket_end': bucket_bounds[(source_id, bucket_start)],
                'unique_protocols_count_ipv4': len(bucket['ipv4']),
                'unique_protocols_count_ipv6': len(bucket['ipv6']),
                'protocols_list_ipv4': ','.join(sorted(bucket['ipv4'])),
                'protocols_list_ipv6': ','.join(sorted(bucket['ipv6'])),
            }
        )
    return result


def insert_netflow_stats_v2_rows(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Insert or replace netflow_stats_v2 rows."""
    conn.executemany(
        """
        INSERT OR REPLACE INTO netflow_stats_v2 (
            source_id, bucket_start, bucket_end, ip_version,
            flows, flows_tcp, flows_udp, flows_icmp, flows_other,
            packets, packets_tcp, packets_udp, packets_icmp, packets_other,
            bytes, bytes_tcp, bytes_udp, bytes_icmp, bytes_other
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row['source_id'],
                row['bucket_start'],
                row['bucket_end'],
                row['ip_version'],
                row['flows'],
                row['flows_tcp'],
                row['flows_udp'],
                row['flows_icmp'],
                row['flows_other'],
                row['packets'],
                row['packets_tcp'],
                row['packets_udp'],
                row['packets_icmp'],
                row['packets_other'],
                row['bytes'],
                row['bytes_tcp'],
                row['bytes_udp'],
                row['bytes_icmp'],
                row['bytes_other'],
            )
            for row in rows
        ],
    )
    conn.commit()


def insert_ip_stats_v2_rows(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Insert or replace ip_stats_v2 rows."""
    conn.executemany(
        """
        INSERT OR REPLACE INTO ip_stats_v2 (
            source_id, granularity, bucket_start, bucket_end,
            sa_ipv4_count, da_ipv4_count, sa_ipv6_count, da_ipv6_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row['source_id'],
                row['granularity'],
                row['bucket_start'],
                row['bucket_end'],
                row['sa_ipv4_count'],
                row['da_ipv4_count'],
                row['sa_ipv6_count'],
                row['da_ipv6_count'],
            )
            for row in rows
        ],
    )
    conn.commit()


def insert_protocol_stats_v2_rows(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Insert or replace protocol_stats_v2 rows."""
    conn.executemany(
        """
        INSERT OR REPLACE INTO protocol_stats_v2 (
            source_id, granularity, bucket_start, bucket_end,
            unique_protocols_count_ipv4, unique_protocols_count_ipv6,
            protocols_list_ipv4, protocols_list_ipv6
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row['source_id'],
                row['granularity'],
                row['bucket_start'],
                row['bucket_end'],
                row['unique_protocols_count_ipv4'],
                row['unique_protocols_count_ipv6'],
                row['protocols_list_ipv4'],
                row['protocols_list_ipv6'],
            )
            for row in rows
        ],
    )
    conn.commit()


def protocol_metric_keys(protocol: int) -> tuple[str, str, str]:
    """Return the netflow metric keys for a protocol number."""
    if protocol == 6:
        return ('flows_tcp', 'packets_tcp', 'bytes_tcp')
    if protocol == 17:
        return ('flows_udp', 'packets_udp', 'bytes_udp')
    if protocol in {1, 58}:
        return ('flows_icmp', 'packets_icmp', 'bytes_icmp')
    return ('flows_other', 'packets_other', 'bytes_other')
