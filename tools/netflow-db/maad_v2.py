"""
MAAD integration helpers for pipeline v2.

Consumes the MAAD JSON stdout contract:
MAAD --input - --output - --format json --structure --spectrum --dimensions
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MIN_MAAD_ADDRS = 2


class MaadV2Error(ValueError):
    """Raised when MAAD output or configuration is invalid."""


@dataclass(frozen=True)
class MaadJsonResult:
    schema_version: int
    metadata: dict[str, Any]
    structure: list[dict[str, Any]]
    spectrum: list[dict[str, Any]]
    dimensions: list[dict[str, Any]]


def parse_maad_json(output: str) -> MaadJsonResult:
    """Parse the MAAD JSON stdout contract."""
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise MaadV2Error('MAAD output was not valid JSON.') from error

    if not isinstance(payload, dict):
        raise MaadV2Error('MAAD JSON output must be an object.')

    schema_version = payload.get('schemaVersion')
    if schema_version != 1:
        raise MaadV2Error(f'Unsupported MAAD schemaVersion: {schema_version!r}.')

    metadata = payload.get('metadata')
    structure = payload.get('structure')
    spectrum = payload.get('spectrum')
    dimensions = payload.get('dimensions')

    if not isinstance(metadata, dict):
        raise MaadV2Error('MAAD metadata must be an object.')
    for key, value in {
        'structure': structure,
        'spectrum': spectrum,
        'dimensions': dimensions,
    }.items():
        if not isinstance(value, list):
            raise MaadV2Error(f'MAAD {key} must be a list.')

    return MaadJsonResult(
        schema_version=schema_version,
        metadata=metadata,
        structure=structure,
        spectrum=spectrum,
        dimensions=dimensions,
    )


def run_maad_json(maad_bin: str | Path, addresses: set[str]) -> MaadJsonResult:
    """Run MAAD against a set of IPv4 addresses and parse its JSON output."""
    if len(addresses) < MIN_MAAD_ADDRS:
        return empty_maad_result(len(addresses))

    input_payload = '\n'.join(sorted(addresses))
    result = subprocess.run(
        [
            str(maad_bin),
            '--input',
            '-',
            '--output',
            '-',
            '--format',
            'json',
            '--structure',
            '--spectrum',
            '--dimensions',
        ],
        input=input_payload,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f'MAAD failed: {result.stderr.strip()}')
    return parse_maad_json(result.stdout)


def empty_maad_result(total_addrs: int = 0) -> MaadJsonResult:
    """Return an empty MAAD result for too-small buckets."""
    return MaadJsonResult(
        schema_version=1,
        metadata={
            'input': '-',
            'minPrefixLength': None,
            'maxPrefixLength': None,
            'totalAddrs': total_addrs,
        },
        structure=[],
        spectrum=[],
        dimensions=[],
    )


def init_maad_v2_tables(conn: sqlite3.Connection) -> None:
    """Create all MAAD v2 output tables."""
    init_structure_stats_v2_table(conn)
    init_spectrum_stats_v2_table(conn)
    init_dimension_stats_v2_table(conn)


def init_structure_stats_v2_table(conn: sqlite3.Connection) -> None:
    """Create structure_stats_v2."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS structure_stats_v2 (
            source_id TEXT NOT NULL,
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            ip_version INTEGER NOT NULL CHECK (ip_version IN (4, 6)),
            structure_json_sa TEXT NOT NULL,
            structure_json_da TEXT NOT NULL,
            metadata_json_sa TEXT NOT NULL,
            metadata_json_da TEXT NOT NULL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_id, bucket_start, ip_version)
        ) WITHOUT ROWID
        """
    )
    conn.commit()


def init_spectrum_stats_v2_table(conn: sqlite3.Connection) -> None:
    """Create spectrum_stats_v2."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spectrum_stats_v2 (
            source_id TEXT NOT NULL,
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            ip_version INTEGER NOT NULL CHECK (ip_version IN (4, 6)),
            spectrum_json_sa TEXT NOT NULL,
            spectrum_json_da TEXT NOT NULL,
            metadata_json_sa TEXT NOT NULL,
            metadata_json_da TEXT NOT NULL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_id, bucket_start, ip_version)
        ) WITHOUT ROWID
        """
    )
    conn.commit()


def init_dimension_stats_v2_table(conn: sqlite3.Connection) -> None:
    """Create dimension_stats_v2."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dimension_stats_v2 (
            source_id TEXT NOT NULL,
            bucket_start INTEGER NOT NULL,
            bucket_end INTEGER NOT NULL,
            ip_version INTEGER NOT NULL CHECK (ip_version IN (4, 6)),
            dimensions_json_sa TEXT NOT NULL,
            dimensions_json_da TEXT NOT NULL,
            metadata_json_sa TEXT NOT NULL,
            metadata_json_da TEXT NOT NULL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_id, bucket_start, ip_version)
        ) WITHOUT ROWID
        """
    )
    conn.commit()


def build_maad_v2_rows(
    *,
    source_id: str,
    bucket_start: int,
    bucket_end: int,
    ip_version: int,
    source_result: MaadJsonResult,
    destination_result: MaadJsonResult,
) -> dict[str, dict]:
    """Build insert payloads for MAAD v2 tables."""
    base = {
        'source_id': source_id,
        'bucket_start': bucket_start,
        'bucket_end': bucket_end,
        'ip_version': ip_version,
        'metadata_json_sa': json.dumps(source_result.metadata, sort_keys=True),
        'metadata_json_da': json.dumps(destination_result.metadata, sort_keys=True),
    }
    return {
        'structure': {
            **base,
            'structure_json_sa': json.dumps(source_result.structure, sort_keys=True),
            'structure_json_da': json.dumps(destination_result.structure, sort_keys=True),
        },
        'spectrum': {
            **base,
            'spectrum_json_sa': json.dumps(source_result.spectrum, sort_keys=True),
            'spectrum_json_da': json.dumps(destination_result.spectrum, sort_keys=True),
        },
        'dimensions': {
            **base,
            'dimensions_json_sa': json.dumps(source_result.dimensions, sort_keys=True),
            'dimensions_json_da': json.dumps(destination_result.dimensions, sort_keys=True),
        },
    }


def insert_maad_v2_rows(conn: sqlite3.Connection, rows: dict[str, dict]) -> None:
    """Insert one bucket's MAAD v2 table rows."""
    insert_structure_stats_v2_row(conn, rows['structure'])
    insert_spectrum_stats_v2_row(conn, rows['spectrum'])
    insert_dimension_stats_v2_row(conn, rows['dimensions'])


def insert_structure_stats_v2_row(conn: sqlite3.Connection, row: dict) -> None:
    """Insert one structure_stats_v2 row."""
    conn.execute(
        """
        INSERT OR REPLACE INTO structure_stats_v2 (
            source_id, bucket_start, bucket_end, ip_version,
            structure_json_sa, structure_json_da, metadata_json_sa, metadata_json_da
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row['source_id'],
            row['bucket_start'],
            row['bucket_end'],
            row['ip_version'],
            row['structure_json_sa'],
            row['structure_json_da'],
            row['metadata_json_sa'],
            row['metadata_json_da'],
        ),
    )
    conn.commit()


def insert_spectrum_stats_v2_row(conn: sqlite3.Connection, row: dict) -> None:
    """Insert one spectrum_stats_v2 row."""
    conn.execute(
        """
        INSERT OR REPLACE INTO spectrum_stats_v2 (
            source_id, bucket_start, bucket_end, ip_version,
            spectrum_json_sa, spectrum_json_da, metadata_json_sa, metadata_json_da
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row['source_id'],
            row['bucket_start'],
            row['bucket_end'],
            row['ip_version'],
            row['spectrum_json_sa'],
            row['spectrum_json_da'],
            row['metadata_json_sa'],
            row['metadata_json_da'],
        ),
    )
    conn.commit()


def insert_dimension_stats_v2_row(conn: sqlite3.Connection, row: dict) -> None:
    """Insert one dimension_stats_v2 row."""
    conn.execute(
        """
        INSERT OR REPLACE INTO dimension_stats_v2 (
            source_id, bucket_start, bucket_end, ip_version,
            dimensions_json_sa, dimensions_json_da, metadata_json_sa, metadata_json_da
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row['source_id'],
            row['bucket_start'],
            row['bucket_end'],
            row['ip_version'],
            row['dimensions_json_sa'],
            row['dimensions_json_da'],
            row['metadata_json_sa'],
            row['metadata_json_da'],
        ),
    )
    conn.commit()
