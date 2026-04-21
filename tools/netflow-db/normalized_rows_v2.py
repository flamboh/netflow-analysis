"""
Normalized row utilities for pipeline v2.

Adapters map raw input rows into a shared, bucketed row contract that powers
the v2 aggregate tables.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from csv_ingest_v2 import CsvSourceConfig, CsvSourceConfigError, parse_timestamp, resolve_bucket_start, resolve_source_id


NFDUMP_CSV_FORMAT = 'csv:%trr,%ter,%tsr,%sa,%da,%sp,%dp,%pr,%pkt,%byt,%stos,%dtos'


@dataclass(frozen=True)
class NormalizedRow:
    source_id: str
    bucket_start: int
    bucket_end: int
    time_received: int | None
    time_end: int | None
    time_start: int | None
    src_ip: str
    dst_ip: str
    ip_version: int
    src_port: int
    dst_port: int
    protocol: int
    packets: int
    bytes: int
    src_tos: int
    dst_tos: int


def build_nfdump_csv_command(file_path: str, ip_version: int) -> list[str]:
    """Build the fixed-order nfdump CSV command for a capture file."""
    if ip_version == 4:
        family_filter = ['ipv4']
    elif ip_version == 6:
        family_filter = ['ipv6', '-6']
    else:
        raise ValueError('ip_version must be 4 or 6')

    return ['nfdump', '-r', file_path, '-q', '-o', NFDUMP_CSV_FORMAT, *family_filter]


def normalize_nfdump_csv_values(values: Sequence[str], source_id: str) -> NormalizedRow:
    """Normalize a fixed-order nfdump CSV row."""
    if len(values) != 12:
        raise CsvSourceConfigError(f'nfdump CSV row must contain 12 values, got {len(values)}.')

    row = {
        'tr': values[0],
        'te': values[1],
        'ts': values[2],
        'sa': values[3],
        'da': values[4],
        'sp': normalize_nfdump_port(values[5]),
        'dp': normalize_nfdump_port(values[6]),
        'pr': values[7],
        'pkt': values[8],
        'byt': values[9],
        'stos': values[10],
        'dtos': values[11],
    }
    config = CsvSourceConfig(
        delimiter=',',
        has_header=False,
        timestamp_format='unix',
        columns={
            'time_received': 'tr',
            'time_end': 'te',
            'time_start': 'ts',
            'src_ip': 'sa',
            'dst_ip': 'da',
            'src_port': 'sp',
            'dst_port': 'dp',
            'protocol': 'pr',
            'packets': 'pkt',
            'bytes': 'byt',
            'src_tos': 'stos',
            'dst_tos': 'dtos',
        },
        source_id_value=source_id,
        source_id_column=None,
    )
    return normalize_csv_row(row, config)


def normalize_nfdump_port(raw_value: str) -> str:
    """Map nfdump ICMP type/code pseudo-ports to no transport port."""
    raw_text = str(raw_value).strip()
    if '.' in raw_text:
        return '0'
    return raw_text


def normalize_csv_row(row: Mapping[str, Any], config: CsvSourceConfig) -> NormalizedRow:
    """Normalize a mapped CSV row into the shared v2 row contract."""
    source_id = resolve_source_id(row, config)
    bucket_start = resolve_bucket_start(row, config)
    bucket_end = bucket_start + 300

    src_ip = require_value(row, config.columns['src_ip'])
    dst_ip = require_value(row, config.columns['dst_ip'])
    ip_version = infer_ip_version(src_ip, dst_ip)

    return NormalizedRow(
        source_id=source_id,
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        time_received=extract_timestamp(row, config, 'time_received'),
        time_end=extract_timestamp(row, config, 'time_end'),
        time_start=extract_timestamp(row, config, 'time_start'),
        src_ip=src_ip,
        dst_ip=dst_ip,
        ip_version=ip_version,
        src_port=extract_int(row, config, 'src_port'),
        dst_port=extract_int(row, config, 'dst_port'),
        protocol=extract_int(row, config, 'protocol'),
        packets=extract_int(row, config, 'packets'),
        bytes=extract_int(row, config, 'bytes'),
        src_tos=extract_int(row, config, 'src_tos'),
        dst_tos=extract_int(row, config, 'dst_tos'),
    )


def infer_ip_version(src_ip: str, dst_ip: str) -> int:
    """Infer a shared IP family from the parsed source and destination addresses."""
    src = ipaddress.ip_address(src_ip)
    dst = ipaddress.ip_address(dst_ip)
    if src.version != dst.version:
        raise CsvSourceConfigError(
            f'Mixed IP versions in one row are not supported: {src_ip} -> {dst_ip}.'
        )
    return src.version


def require_value(row: Mapping[str, Any], column_name: str) -> str:
    """Load a required string value from the row."""
    raw = row.get(column_name)
    if raw in (None, ''):
        raise CsvSourceConfigError(f"CSV row is missing required value for column '{column_name}'.")
    return str(raw).strip()


def extract_timestamp(row: Mapping[str, Any], config: CsvSourceConfig, logical_key: str) -> int | None:
    """Extract an optional timestamp field using the config mapping."""
    column_name = config.columns.get(logical_key)
    if column_name is None:
        return None
    raw = row.get(column_name)
    if raw in (None, ''):
        return None
    return parse_timestamp(raw, config.timestamp_format)


def extract_int(row: Mapping[str, Any], config: CsvSourceConfig, logical_key: str) -> int:
    """Extract an optional integer field, defaulting to 0 when absent."""
    column_name = config.columns.get(logical_key)
    if column_name is None:
        return 0
    raw = row.get(column_name)
    if raw in (None, ''):
        return 0
    try:
        return int(str(raw).strip())
    except ValueError as error:
        raise CsvSourceConfigError(
            f"Invalid integer value '{raw}' for column '{column_name}'."
        ) from error
