#!/usr/bin/env python3
"""
Minimal pipeline entrypoint for non-MAAD v2 work.

Processes explicit csv and nfcapd inputs into:
- processed_inputs_v2
- netflow_stats_v2
- ip_stats_v2
- protocol_stats_v2
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import subprocess
from dataclasses import dataclass, field
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
    build_ip_stats_v2_rows,
    build_netflow_stats_v2_rows,
    build_protocol_stats_v2_rows,
    init_ip_stats_v2_table,
    init_netflow_stats_v2_table,
    init_protocol_stats_v2_table,
    insert_ip_stats_v2_rows,
    insert_netflow_stats_v2_rows,
    insert_protocol_stats_v2_rows,
)


DEFAULT_MAAD_BIN = Path(__file__).resolve().parents[2] / 'vendor' / 'maad' / 'MAAD'


@dataclass
class BucketAccumulator:
    """Per-source 5-minute accumulator for v2 stats and MAAD inputs."""

    source_id: str
    bucket_start: int
    bucket_end: int
    netflow_rows: list[NormalizedRow] = field(default_factory=list)
    maad_source_ipv4: set[str] = field(default_factory=set)
    maad_destination_ipv4: set[str] = field(default_factory=set)

    def add(self, row: NormalizedRow) -> None:
        """Accumulate one normalized row."""
        self.netflow_rows.append(row)
        if row.ip_version == 4:
            self.maad_source_ipv4.add(row.src_ip)
            self.maad_destination_ipv4.add(row.dst_ip)


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
    run_maad: bool = False,
    maad_bin: str | Path = DEFAULT_MAAD_BIN,
) -> None:
    """Process explicit input specs into the v2 aggregate tables."""
    init_processed_inputs_v2_table(conn)
    init_netflow_stats_v2_table(conn)
    init_ip_stats_v2_table(conn)
    init_protocol_stats_v2_table(conn)
    if run_maad:
        init_maad_v2_tables(conn)

    for spec in input_specs:
        input_kind = str(spec['input_kind'])
        input_locator = str(spec['path'])
        buckets = accumulate_input_buckets(iter_input_rows(spec))
        if not buckets:
            continue

        for source_id, bucket_start, bucket_end in sorted(buckets):
            upsert_input_bucket(
                conn,
                input_kind=input_kind,
                input_locator=input_locator,
                source_id=source_id,
                bucket_start=bucket_start,
                bucket_end=bucket_end,
            )

        rows = [row for bucket in buckets.values() for row in bucket.netflow_rows]
        insert_netflow_stats_v2_rows(conn, build_netflow_stats_v2_rows(rows))
        insert_ip_stats_v2_rows(conn, build_ip_stats_v2_rows(rows))
        insert_protocol_stats_v2_rows(conn, build_protocol_stats_v2_rows(rows))

        if run_maad:
            process_maad_buckets(conn, buckets, maad_bin)

        for source_id, bucket_start, _bucket_end in sorted(buckets):
            for table_name in ('netflow_stats_v2', 'ip_stats_v2', 'protocol_stats_v2'):
                mark_input_bucket_processed(
                    conn,
                    table_name=table_name,
                    input_kind=input_kind,
                    input_locator=input_locator,
                    source_id=source_id,
                    bucket_start=bucket_start,
                    success=True,
                )
            if run_maad:
                for table_name in ('structure_stats_v2', 'spectrum_stats_v2', 'dimension_stats_v2'):
                    mark_input_bucket_processed(
                        conn,
                        table_name=table_name,
                        input_kind=input_kind,
                        input_locator=input_locator,
                        source_id=source_id,
                        bucket_start=bucket_start,
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


def process_maad_buckets(
    conn: sqlite3.Connection,
    buckets: dict[tuple[str, int, int], BucketAccumulator],
    maad_bin: str | Path,
) -> None:
    """Run MAAD for each v2 bucket and persist structure/spectrum/dimensions."""
    for bucket in buckets.values():
        source_result = run_maad_json(maad_bin, bucket.maad_source_ipv4)
        destination_result = run_maad_json(maad_bin, bucket.maad_destination_ipv4)
        rows = build_maad_v2_rows(
            source_id=bucket.source_id,
            bucket_start=bucket.bucket_start,
            bucket_end=bucket.bucket_end,
            ip_version=4,
            source_result=source_result,
            destination_result=destination_result,
        )
        insert_maad_v2_rows(conn, rows)


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
        int(values[0])
        return False
    except ValueError:
        return True


def unique_input_buckets(rows: list[NormalizedRow]) -> list[tuple[str, int, int]]:
    """Return unique (source_id, bucket_start, bucket_end) tuples for the input."""
    return sorted({(row.source_id, row.bucket_start, row.bucket_end) for row in rows})


def main() -> None:
    """Run the minimal pipeline v2 entrypoint."""
    parser = argparse.ArgumentParser(description='Pipeline v2 non-MAAD processor')
    parser.add_argument('--config', required=True, help='Path to the pipeline_v2 json config.')
    args = parser.parse_args()

    config = load_pipeline_v2_config(args.config)
    db_path = Path(config['database_path'])
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        process_input_specs(
            conn,
            config['inputs'],
            run_maad=bool(config.get('run_maad', False)),
            maad_bin=config.get('maad_bin', DEFAULT_MAAD_BIN),
        )


if __name__ == '__main__':
    main()
