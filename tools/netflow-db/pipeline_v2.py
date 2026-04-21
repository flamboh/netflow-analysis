#!/usr/bin/env python3
"""
Pipeline v2 entrypoint.

Processes explicit csv and nfcapd inputs into:
- processed_inputs_v2
- netflow_stats_v2
- ip_stats_v2
- protocol_stats_v2
- structure_stats_v2
- spectrum_stats_v2
- dimension_stats_v2
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import subprocess
from dataclasses import dataclass, field
from multiprocessing import Pool
from pathlib import Path
from typing import Iterable

from csv_ingest_v2 import load_csv_source_config
from maad_v2 import (
    MaadJsonResult,
    build_maad_v2_rows,
    init_maad_v2_tables,
    insert_maad_v2_rows,
    run_maad_json,
)
from normalized_rows_v2 import NormalizedRow, build_nfdump_csv_command, normalize_csv_row, normalize_nfdump_csv_values
from processed_inputs_v2 import (
    init_processed_inputs_v2_table,
    mark_input_bucket_processed,
    upsert_input_bucket,
)
from stats_v2 import (
    init_ip_stats_v2_table,
    init_netflow_stats_v2_table,
    init_protocol_stats_v2_table,
    insert_ip_stats_v2_rows,
    insert_netflow_stats_v2_rows,
    insert_protocol_stats_v2_rows,
    protocol_metric_keys,
)


DEFAULT_MAAD_BIN = Path(__file__).resolve().parents[2] / 'vendor' / 'maad' / 'MAAD'
DEFAULT_MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '8'))

STATS_TABLE_NAMES = ('netflow_stats_v2', 'ip_stats_v2', 'protocol_stats_v2')
MAAD_TABLE_NAMES = ('structure_stats_v2', 'spectrum_stats_v2', 'dimension_stats_v2')


@dataclass
class BucketAccumulator:
    """Per-source 5-minute accumulator for v2 stats and MAAD inputs."""

    source_id: str
    bucket_start: int
    bucket_end: int
    netflow_by_version: dict[int, dict] = field(default_factory=dict)
    source_ipv4: set[str] = field(default_factory=set)
    destination_ipv4: set[str] = field(default_factory=set)
    source_ipv6: set[str] = field(default_factory=set)
    destination_ipv6: set[str] = field(default_factory=set)
    protocols_ipv4: set[str] = field(default_factory=set)
    protocols_ipv6: set[str] = field(default_factory=set)
    maad_source_ipv4: set[str] = field(default_factory=set)
    maad_destination_ipv4: set[str] = field(default_factory=set)

    def add(self, row: NormalizedRow) -> None:
        """Accumulate one normalized row."""
        netflow = self.netflow_by_version.setdefault(row.ip_version, new_netflow_bucket(row))
        netflow['flows'] += 1
        netflow['packets'] += row.packets
        netflow['bytes'] += row.bytes
        flow_key, packets_key, bytes_key = protocol_metric_keys(row.protocol)
        netflow[flow_key] += 1
        netflow[packets_key] += row.packets
        netflow[bytes_key] += row.bytes

        if row.ip_version == 4:
            self.source_ipv4.add(row.src_ip)
            self.destination_ipv4.add(row.dst_ip)
            self.protocols_ipv4.add(str(row.protocol))
            self.maad_source_ipv4.add(row.src_ip)
            self.maad_destination_ipv4.add(row.dst_ip)
        else:
            self.source_ipv6.add(row.src_ip)
            self.destination_ipv6.add(row.dst_ip)
            self.protocols_ipv6.add(str(row.protocol))

    def netflow_rows(self) -> list[dict]:
        """Return netflow_stats_v2 rows for this bucket."""
        return [self.netflow_by_version[ip_version] for ip_version in sorted(self.netflow_by_version)]

    def ip_row(self) -> dict:
        """Return one ip_stats_v2 row for this bucket."""
        return {
            'source_id': self.source_id,
            'granularity': '5m',
            'bucket_start': self.bucket_start,
            'bucket_end': self.bucket_end,
            'sa_ipv4_count': len(self.source_ipv4),
            'da_ipv4_count': len(self.destination_ipv4),
            'sa_ipv6_count': len(self.source_ipv6),
            'da_ipv6_count': len(self.destination_ipv6),
        }

    def protocol_row(self) -> dict:
        """Return one protocol_stats_v2 row for this bucket."""
        return {
            'source_id': self.source_id,
            'granularity': '5m',
            'bucket_start': self.bucket_start,
            'bucket_end': self.bucket_end,
            'unique_protocols_count_ipv4': len(self.protocols_ipv4),
            'unique_protocols_count_ipv6': len(self.protocols_ipv6),
            'protocols_list_ipv4': ','.join(sorted(self.protocols_ipv4, key=int)),
            'protocols_list_ipv6': ','.join(sorted(self.protocols_ipv6, key=int)),
        }


def load_pipeline_v2_config(path: str | Path) -> dict:
    """Load the minimal v2 pipeline config file."""
    with open(path, 'r', encoding='utf-8') as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError('pipeline_v2 config must be a json object')
    inputs = payload.get('inputs')
    if not isinstance(inputs, list):
        raise ValueError("pipeline_v2 config must include an 'inputs' list")
    return payload


def process_input_specs(
    conn: sqlite3.Connection,
    input_specs: list[dict],
    *,
    maad_bin: str | Path = DEFAULT_MAAD_BIN,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> None:
    """Process explicit input specs into the v2 aggregate tables."""
    init_processed_inputs_v2_table(conn)
    init_netflow_stats_v2_table(conn)
    init_ip_stats_v2_table(conn)
    init_protocol_stats_v2_table(conn)
    init_maad_v2_tables(conn)

    tasks = [(spec, str(maad_bin)) for spec in input_specs]
    if max_workers > 1 and len(tasks) > 1:
        with Pool(processes=max_workers) as pool:
            payloads = pool.imap_unordered(process_input_spec_worker, tasks, chunksize=1)
            for payload in payloads:
                write_input_payload(conn, payload)
        return

    for task in tasks:
        write_input_payload(conn, process_input_spec_worker(task))


def process_input_spec_worker(task: tuple[dict, str]) -> dict:
    """Worker entrypoint for processing one input spec without DB access."""
    spec, maad_bin = task
    return build_input_payload(spec, maad_bin)


def build_input_payload(spec: dict, maad_bin: str | Path) -> dict:
    """Build all DB insert payloads for one input spec."""
    input_kind = str(spec['input_kind'])
    input_locator = str(spec['path'])
    buckets = accumulate_input_buckets(iter_input_rows(spec))
    bucket_values = [buckets[key] for key in sorted(buckets)]

    return {
        'processed_buckets': [
            {
                'input_kind': input_kind,
                'input_locator': input_locator,
                'source_id': source_id,
                'bucket_start': bucket_start,
                'bucket_end': bucket_end,
            }
            for source_id, bucket_start, bucket_end in sorted(buckets)
        ],
        'netflow_rows': [row for bucket in bucket_values for row in bucket.netflow_rows()],
        'ip_rows': [bucket.ip_row() for bucket in bucket_values],
        'protocol_rows': [bucket.protocol_row() for bucket in bucket_values],
        'maad_rows': build_maad_payload_rows(buckets, maad_bin),
    }


def write_input_payload(conn: sqlite3.Connection, payload: dict) -> None:
    """Persist a worker payload. SQLite writes remain in the parent process."""
    processed_buckets = payload['processed_buckets']
    if not processed_buckets:
        return

    for bucket in processed_buckets:
        upsert_input_bucket(conn, **bucket)

    insert_netflow_stats_v2_rows(conn, payload['netflow_rows'])
    insert_ip_stats_v2_rows(conn, payload['ip_rows'])
    insert_protocol_stats_v2_rows(conn, payload['protocol_rows'])

    for rows in payload['maad_rows']:
        insert_maad_v2_rows(conn, rows)

    for bucket in processed_buckets:
        for table_name in (*STATS_TABLE_NAMES, *MAAD_TABLE_NAMES):
            mark_input_bucket_processed(
                conn,
                table_name=table_name,
                input_kind=bucket['input_kind'],
                input_locator=bucket['input_locator'],
                source_id=bucket['source_id'],
                bucket_start=bucket['bucket_start'],
                success=True,
            )


def iter_input_rows(spec: dict) -> Iterable[NormalizedRow]:
    """Yield normalized rows for one explicit input spec."""
    input_kind = str(spec['input_kind'])
    input_path = str(spec['path'])
    if input_kind == 'csv':
        mapping_path = str(spec['mapping_path'])
        yield from iter_csv_rows(input_path, mapping_path)
        return
    if input_kind == 'nfcapd':
        source_id = str(spec['source_id'])
        yield from iter_nfdump_rows(input_path, source_id)
        return
    raise ValueError(f'Unsupported input_kind: {input_kind}')


def accumulate_input_buckets(rows: Iterable[NormalizedRow]) -> dict[tuple[str, int, int], BucketAccumulator]:
    """Accumulate normalized rows by source and 5-minute bucket."""
    buckets: dict[tuple[str, int, int], BucketAccumulator] = {}
    for row in rows:
        key = (row.source_id, row.bucket_start, row.bucket_end)
        bucket = buckets.setdefault(
            key,
            BucketAccumulator(
                source_id=row.source_id,
                bucket_start=row.bucket_start,
                bucket_end=row.bucket_end,
            ),
        )
        bucket.add(row)
    return buckets


def new_netflow_bucket(row: NormalizedRow) -> dict:
    """Create an empty netflow_stats_v2 row accumulator."""
    return {
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
    }


def build_maad_payload_rows(
    buckets: dict[tuple[str, int, int], BucketAccumulator],
    maad_bin: str | Path,
) -> list[dict[str, dict]]:
    """Run MAAD for each v2 bucket and return structure/spectrum/dimension rows."""
    rows = []
    for bucket in buckets.values():
        source_result = run_maad_json(maad_bin, bucket.maad_source_ipv4)
        destination_result = run_maad_json(maad_bin, bucket.maad_destination_ipv4)
        rows.append(
            build_maad_v2_rows(
                source_id=bucket.source_id,
                granularity='5m',
                bucket_start=bucket.bucket_start,
                bucket_end=bucket.bucket_end,
                ip_version=4,
                source_result=source_result,
                destination_result=destination_result,
            )
        )
    return rows


def iter_csv_rows(path: str, mapping_path: str) -> Iterable[NormalizedRow]:
    """Yield normalized rows from an external CSV input."""
    config = load_csv_source_config(mapping_path)
    with open(path, 'r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle, delimiter=config.delimiter)
        for row in reader:
            yield normalize_csv_row(row, config)


def iter_nfdump_rows(path: str, source_id: str) -> Iterable[NormalizedRow]:
    """Yield normalized rows from an nfcapd file via nfdump CSV output."""
    for ip_version in (4, 6):
        command = build_nfdump_csv_command(path, ip_version)
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise RuntimeError(
                f"nfdump failed for {path} family {ip_version}: {result.stderr.strip()}"
            )
        for values in csv.reader(result.stdout.splitlines()):
            if not values:
                continue
            if looks_like_nfdump_header(values):
                continue
            yield normalize_nfdump_csv_values(values, source_id)


def looks_like_nfdump_header(values: list[str]) -> bool:
    """Return true when the csv row looks like a textual header."""
    try:
        float(values[0])
        return False
    except ValueError:
        return True


def unique_input_buckets(rows: list[NormalizedRow]) -> list[tuple[str, int, int]]:
    """Return unique (source_id, bucket_start, bucket_end) tuples for the input."""
    return sorted({(row.source_id, row.bucket_start, row.bucket_end) for row in rows})


def main() -> None:
    """Run the minimal pipeline v2 entrypoint."""
    parser = argparse.ArgumentParser(description='Pipeline v2 processor')
    parser.add_argument('--config', required=True, help='Path to the pipeline_v2 json config.')
    args = parser.parse_args()

    config = load_pipeline_v2_config(args.config)
    db_path = Path(config['database_path'])
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        process_input_specs(
            conn,
            config['inputs'],
            maad_bin=config.get('maad_bin', DEFAULT_MAAD_BIN),
            max_workers=int(config.get('max_workers', DEFAULT_MAX_WORKERS)),
        )


if __name__ == '__main__':
    main()
