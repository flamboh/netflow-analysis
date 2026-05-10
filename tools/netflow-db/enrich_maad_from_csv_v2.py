#!/usr/bin/env python3
"""Enrich an existing pipeline v2 SQLite DB with MAAD rows from CSV inputs.

This pass is intentionally narrower than pipeline_v2: it reads only timestamp and
endpoint columns, keeps raw addresses in bounded in-memory sets, writes
structure/spectrum/dimension rows, and then drops the sets.
"""

from __future__ import annotations

import argparse
import sqlite3
import tarfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from csv_ingest_v2 import CsvSourceConfig, load_csv_source_config
from csv_inputs_v2 import discover_csv_specs, is_tar_archive
from csv_inputs_v2 import csv_discovery_sort_key
from maad_v2 import init_maad_v2_tables, insert_maad_v2_rows
from pipeline_v2 import (
    AGGREGATE_GRANULARITY_SECONDS,
    CSV_ARROW_BLOCK_BYTES,
    CSV_MAAD_BATCH_BUCKETS,
    CSV_STREAM_PROGRESS_ROWS,
    arrow_ipv4_pair_mask,
    floor_bucket_start,
    next_bucket_start,
    parse_arrow_minute_bucket,
    process_maad_tasks,
)
from processed_inputs_v2 import init_processed_inputs_v2_table, mark_input_bucket_status


@dataclass(slots=True)
class MaadAddressBucket:
    source_id: str
    granularity: str
    bucket_start: int
    bucket_end: int
    source_ipv4: set[str] = field(default_factory=set)
    destination_ipv4: set[str] = field(default_factory=set)


def enrich_database_from_config(
    conn: sqlite3.Connection,
    config_path: str | Path,
    *,
    maad_bin: str | Path,
    maad_backend: str,
    maad_workers: int,
) -> None:
    """Read a pipeline_v2 config and enrich its CSV tree inputs."""
    import json

    payload = json.loads(Path(config_path).read_text(encoding='utf-8'))
    for spec in payload['inputs']:
        if spec['input_kind'] != 'csv_tree':
            continue
        enrich_database_from_csv_tree(
            conn,
            root_path=spec['root_path'],
            mapping_path=spec['mapping_path'],
            maad_bin=maad_bin,
            maad_backend=maad_backend,
            maad_workers=maad_workers,
        )


def enrich_database_from_csv_tree(
    conn: sqlite3.Connection,
    *,
    root_path: str | Path,
    mapping_path: str | Path,
    maad_bin: str | Path,
    maad_backend: str,
    maad_workers: int,
) -> None:
    """Discover CSV specs under a tree and enrich matching DB inputs."""
    specs = discover_csv_specs(root_path, mapping_path)
    pending_locators = csv_locators_requiring_maad(conn)
    specs_by_locator = {str(spec['path']): spec for spec in specs}
    selected = []
    for locator in sorted(
        pending_locators,
        key=lambda value: csv_discovery_sort_key(Path(value).name),
    ):
        spec = specs_by_locator.get(locator)
        if spec is not None:
            selected.append(spec)
            continue
        if Path(locator).is_file():
            selected.append(
                {
                    'input_kind': 'csv',
                    'path': locator,
                    'mapping_path': str(mapping_path),
                }
            )
    print(f'[maad_enrich_v2] CSV inputs requiring MAAD: {len(selected)}/{len(specs)}')
    enrich_database_from_csv_specs(
        conn,
        selected,
        maad_bin=maad_bin,
        maad_backend=maad_backend,
        maad_workers=maad_workers,
    )


def enrich_database_from_csv_specs(
    conn: sqlite3.Connection,
    specs: list[dict],
    *,
    maad_bin: str | Path,
    maad_backend: str,
    maad_workers: int,
) -> None:
    """Enrich an existing DB from explicit CSV specs."""
    init_processed_inputs_v2_table(conn)
    init_maad_v2_tables(conn)
    aggregate_buckets: dict[tuple[str, str, int], MaadAddressBucket] = {}
    pending_processed_marks: list[dict] = []

    for spec in specs:
        enrich_database_from_csv_spec(
            conn,
            spec,
            maad_bin=maad_bin,
            maad_backend=maad_backend,
            maad_workers=maad_workers,
            aggregate_buckets=aggregate_buckets,
            pending_processed_marks=pending_processed_marks,
        )

    flush_aggregate_buckets(
        conn,
        aggregate_buckets,
        list(aggregate_buckets),
        maad_bin=maad_bin,
        maad_backend=maad_backend,
        maad_workers=maad_workers,
    )
    mark_ready_processed_inputs(conn, pending_processed_marks, float('inf'))


def enrich_database_from_csv_spec(
    conn: sqlite3.Connection,
    spec: dict,
    *,
    maad_bin: str | Path,
    maad_backend: str,
    maad_workers: int,
    aggregate_buckets: dict[tuple[str, str, int], MaadAddressBucket],
    pending_processed_marks: list[dict],
) -> None:
    """Read one CSV/archive and write MAAD rows for DB buckets that need them."""
    config = load_csv_source_config(spec['mapping_path'])
    if config.source_id_value is None:
        raise ValueError('MAAD CSV enrichment requires a constant source_id value.')

    input_locator = str(spec['path'])
    needed_buckets = pending_buckets_for_input(conn, input_locator)
    if not needed_buckets:
        print(f'[maad_enrich_v2] Skip CSV input with no pending buckets: {input_locator}')
        return

    active_buckets: dict[int, MaadAddressBucket] = {}
    max_bucket_start: int | None = None
    rows_seen = 0
    print(f'[maad_enrich_v2] CSV start: {input_locator}')

    for batch in iter_endpoint_arrow_batches(input_locator, config):
        rows_seen += batch.num_rows
        max_bucket_start = merge_endpoint_batch(
            active_buckets,
            batch,
            config,
            config.source_id_value,
            max_bucket_start,
            needed_buckets,
        )
        if max_bucket_start is None:
            continue

        cutoff = max_bucket_start - (config.out_of_order_lag_buckets * 300)
        if rows_seen % CSV_STREAM_PROGRESS_ROWS < batch.num_rows:
            print(
                f'[maad_enrich_v2] CSV rows={rows_seen} active_buckets={len(active_buckets)} '
                f'input={input_locator}'
            )
        ready_bucket_starts = [bucket_start for bucket_start in active_buckets if bucket_start < cutoff]
        if len(ready_bucket_starts) < CSV_MAAD_BATCH_BUCKETS:
            continue
        flush_5m_buckets(
            conn,
            active_buckets,
            ready_bucket_starts,
            input_locator=input_locator,
            maad_bin=maad_bin,
            maad_backend=maad_backend,
            maad_workers=maad_workers,
            aggregate_buckets=aggregate_buckets,
            pending_processed_marks=pending_processed_marks,
            aggregate_cutoff=cutoff,
        )

    flush_5m_buckets(
        conn,
        active_buckets,
        list(active_buckets),
        input_locator=input_locator,
        maad_bin=maad_bin,
        maad_backend=maad_backend,
        maad_workers=maad_workers,
        aggregate_buckets=aggregate_buckets,
        pending_processed_marks=pending_processed_marks,
        aggregate_cutoff=max_bucket_start if max_bucket_start is not None else 0,
    )
    print(f'[maad_enrich_v2] CSV complete: rows={rows_seen} input={input_locator}')


def iter_endpoint_arrow_batches(input_locator: str, config: CsvSourceConfig):
    """Yield PyArrow batches with only timestamp and endpoint columns."""
    import pyarrow as pa
    import pyarrow.csv as arrow_csv

    include_columns = [
        config.columns['time_end'],
        config.columns['src_ip'],
        config.columns['dst_ip'],
    ]
    read_options = arrow_csv.ReadOptions(
        column_names=config.fieldnames,
        block_size=CSV_ARROW_BLOCK_BYTES,
    )
    parse_options = arrow_csv.ParseOptions(
        delimiter=config.delimiter,
        invalid_row_handler=lambda _row: 'skip' if config.skip_bad_column_count else 'error',
    )
    convert_options = arrow_csv.ConvertOptions(
        include_columns=include_columns,
        column_types={column: pa.string() for column in include_columns},
    )

    input_path = Path(input_locator)
    if is_tar_archive(input_path):
        with tarfile.open(input_path, mode='r:*') as archive:
            for member in archive:
                if not member.isfile():
                    continue
                if config.archive_member_contains and config.archive_member_contains not in member.name:
                    continue
                extracted = archive.extractfile(member)
                if extracted is None:
                    continue
                with extracted:
                    yield from read_endpoint_arrow_batches(
                        extracted,
                        read_options,
                        parse_options,
                        convert_options,
                    )
        return

    with open(input_path, 'rb') as handle:
        yield from read_endpoint_arrow_batches(handle, read_options, parse_options, convert_options)


def read_endpoint_arrow_batches(handle, read_options, parse_options, convert_options):
    """Read Arrow CSV batches and close the reader."""
    import pyarrow.csv as arrow_csv

    reader = arrow_csv.open_csv(
        handle,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )
    try:
        while True:
            try:
                yield reader.read_next_batch()
            except StopIteration:
                break
    finally:
        reader.close()


def merge_endpoint_batch(
    active_buckets: dict[int, MaadAddressBucket],
    batch,
    config: CsvSourceConfig,
    source_id: str,
    max_bucket_start: int | None,
    needed_buckets: set[int],
) -> int | None:
    """Merge one endpoint-only Arrow batch into active 5m MAAD buckets."""
    import pyarrow as pa
    import pyarrow.compute as pc

    time_column = config.columns['time_end']
    src_column = config.columns['src_ip']
    dst_column = config.columns['dst_ip']
    mask = pc.match_substring_regex(
        batch[time_column],
        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
    )
    range_mask = needed_bucket_time_range_mask(batch, config, needed_buckets)
    if range_mask is not None:
        mask = pc.and_(mask, range_mask)
    batch = batch.filter(mask)
    if batch.num_rows == 0:
        return max_bucket_start

    mask = pc.not_equal(batch[src_column], '')
    mask = pc.and_(mask, pc.not_equal(batch[dst_column], ''))
    mask = pc.and_(mask, arrow_ipv4_pair_mask(batch, src_column, dst_column))
    batch = batch.filter(mask)
    if batch.num_rows == 0:
        return max_bucket_start

    minute = pc.utf8_slice_codeunits(batch[time_column], 0, 16)
    table = pa.table({'minute': minute, 'src_ip': batch[src_column], 'dst_ip': batch[dst_column]})
    for raw_minute in pc.unique(minute).to_pylist():
        bucket_start = parse_arrow_minute_bucket(raw_minute, config)
        max_bucket_start = bucket_start if max_bucket_start is None else max(max_bucket_start, bucket_start)
        if bucket_start not in needed_buckets:
            continue
        minute_table = table.filter(pc.equal(table['minute'], raw_minute))
        bucket = active_buckets.setdefault(
            bucket_start,
            MaadAddressBucket(
                source_id=source_id,
                granularity='5m',
                bucket_start=bucket_start,
                bucket_end=bucket_start + 300,
            ),
        )
        bucket.source_ipv4.update(pc.unique(minute_table['src_ip']).to_pylist())
        bucket.destination_ipv4.update(pc.unique(minute_table['dst_ip']).to_pylist())
    return max_bucket_start


def needed_bucket_time_range_mask(batch, config: CsvSourceConfig, needed_buckets: set[int]):
    """Return a cheap timestamp mask that excludes rows outside pending bucket bounds."""
    if config.timestamp_format != 'datetime' or config.datetime_format != '%Y-%m-%d %H:%M:%S':
        return None
    if not needed_buckets:
        return None

    import pyarrow as pa
    import pyarrow.compute as pc

    time_column = config.columns['time_end']
    min_bucket = min(needed_buckets)
    max_bucket = max(needed_buckets) + 300
    lower = bucket_start_to_csv_datetime(min_bucket, config)
    upper = bucket_start_to_csv_datetime(max_bucket, config)
    return pc.and_(
        pc.greater_equal(batch[time_column], pa.scalar(lower)),
        pc.less(batch[time_column], pa.scalar(upper)),
    )


def bucket_start_to_csv_datetime(bucket_start: int, config: CsvSourceConfig) -> str:
    """Format a bucket boundary for lexicographic CSV timestamp comparison."""
    return datetime.fromtimestamp(
        bucket_start,
        ZoneInfo(config.timestamp_timezone),
    ).strftime(config.datetime_format)


def flush_5m_buckets(
    conn: sqlite3.Connection,
    active_buckets: dict[int, MaadAddressBucket],
    bucket_starts: list[int],
    *,
    input_locator: str,
    maad_bin: str | Path,
    maad_backend: str,
    maad_workers: int,
    aggregate_buckets: dict[tuple[str, str, int], MaadAddressBucket],
    pending_processed_marks: list[dict],
    aggregate_cutoff: int,
) -> None:
    """Write 5m MAAD rows, merge into aggregate state, and release raw IP sets."""
    if not bucket_starts:
        return
    buckets = [active_buckets.pop(bucket_start) for bucket_start in sorted(bucket_starts)]
    write_maad_buckets(
        conn,
        buckets,
        maad_bin=maad_bin,
        maad_backend=maad_backend,
        maad_workers=maad_workers,
    )
    for bucket in buckets:
        merge_aggregate_buckets(aggregate_buckets, bucket)
        pending_processed_marks.append(
            {
                'input_kind': 'csv',
                'input_locator': input_locator,
                'source_id': bucket.source_id,
                'bucket_start': bucket.bucket_start,
            }
        )
    ready_aggregate_keys = [
        key for key, bucket in aggregate_buckets.items() if bucket.bucket_end <= aggregate_cutoff
    ]
    flush_aggregate_buckets(
        conn,
        aggregate_buckets,
        ready_aggregate_keys,
        maad_bin=maad_bin,
        maad_backend=maad_backend,
        maad_workers=maad_workers,
    )
    mark_ready_processed_inputs(conn, pending_processed_marks, aggregate_cutoff)


def merge_aggregate_buckets(
    aggregate_buckets: dict[tuple[str, str, int], MaadAddressBucket],
    bucket: MaadAddressBucket,
) -> None:
    """Merge one 5m bucket into open aggregate MAAD buckets."""
    for granularity, seconds in AGGREGATE_GRANULARITY_SECONDS:
        bucket_start = floor_bucket_start(bucket.bucket_start, seconds)
        key = (bucket.source_id, granularity, bucket_start)
        aggregate = aggregate_buckets.setdefault(
            key,
            MaadAddressBucket(
                source_id=bucket.source_id,
                granularity=granularity,
                bucket_start=bucket_start,
                bucket_end=next_bucket_start(bucket_start, seconds),
            ),
        )
        aggregate.source_ipv4.update(bucket.source_ipv4)
        aggregate.destination_ipv4.update(bucket.destination_ipv4)


def flush_aggregate_buckets(
    conn: sqlite3.Connection,
    aggregate_buckets: dict[tuple[str, str, int], MaadAddressBucket],
    keys: list[tuple[str, str, int]],
    *,
    maad_bin: str | Path,
    maad_backend: str,
    maad_workers: int,
) -> None:
    """Write aggregate MAAD rows and drop their raw IP sets."""
    if not keys:
        return
    buckets = [aggregate_buckets.pop(key) for key in sorted(keys)]
    write_maad_buckets(
        conn,
        buckets,
        maad_bin=maad_bin,
        maad_backend=maad_backend,
        maad_workers=maad_workers,
    )


def write_maad_buckets(
    conn: sqlite3.Connection,
    buckets: list[MaadAddressBucket],
    *,
    maad_bin: str | Path,
    maad_backend: str,
    maad_workers: int,
) -> None:
    """Compute and insert MAAD rows for address buckets."""
    tasks = [
        {
            'maad_bin': str(maad_bin),
            'maad_backend': maad_backend,
            'source_id': bucket.source_id,
            'granularity': bucket.granularity,
            'bucket_start': bucket.bucket_start,
            'bucket_end': bucket.bucket_end,
            'source_addresses': sorted(bucket.source_ipv4),
            'destination_addresses': sorted(bucket.destination_ipv4),
            'log_progress': bucket.granularity != '5m',
        }
        for bucket in buckets
    ]
    rows = process_maad_tasks(tasks, maad_workers)
    with conn:
        for row_group in rows:
            insert_maad_v2_rows(conn, row_group)


def pending_buckets_for_input(conn: sqlite3.Connection, input_locator: str) -> set[int]:
    """Return pending 5m bucket starts for a CSV locator."""
    rows = conn.execute(
        """
        SELECT bucket_start
        FROM processed_inputs_v2
        WHERE input_kind = 'csv'
          AND input_locator = ?
          AND status != 'processed'
        ORDER BY bucket_start
        """,
        (input_locator,),
    ).fetchall()
    return {int(row[0]) for row in rows}


def csv_locators_requiring_maad(conn: sqlite3.Connection) -> set[str]:
    """Return CSV locators with unprocessed DB bucket records."""
    init_processed_inputs_v2_table(conn)
    rows = conn.execute(
        """
        SELECT DISTINCT input_locator
        FROM processed_inputs_v2
        WHERE input_kind = 'csv' AND status != 'processed'
        ORDER BY input_locator
        """
    ).fetchall()
    return {str(row[0]) for row in rows}


def mark_ready_processed_inputs(
    conn: sqlite3.Connection,
    pending_processed_marks: list[dict],
    aggregate_cutoff: float,
) -> None:
    """Mark input buckets processed once enclosing aggregate MAAD rows are closed."""
    ready = [
        mark
        for mark in pending_processed_marks
        if all_aggregate_maad_closed(int(mark['bucket_start']), aggregate_cutoff)
    ]
    if not ready:
        return
    ready_ids = {
        (mark['input_locator'], mark['source_id'], mark['bucket_start'])
        for mark in ready
    }
    with conn:
        for mark in ready:
            mark_input_bucket_status(conn, **mark, status='processed')
    pending_processed_marks[:] = [
        mark
        for mark in pending_processed_marks
        if (mark['input_locator'], mark['source_id'], mark['bucket_start']) not in ready_ids
    ]


def all_aggregate_maad_closed(bucket_start: int, aggregate_cutoff: float) -> bool:
    """Return true when all aggregate buckets containing the 5m bucket were written."""
    return all(
        next_bucket_start(floor_bucket_start(bucket_start, seconds), seconds) <= aggregate_cutoff
        for _, seconds in AGGREGATE_GRANULARITY_SECONDS
    )


def main() -> None:
    parser = argparse.ArgumentParser(description='Enrich pipeline v2 DB MAAD rows from CSV inputs.')
    parser.add_argument('--database-path', required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--maad-bin')
    parser.add_argument('--maad-backend', choices=['python', 'subprocess'])
    parser.add_argument('--maad-workers', type=int)
    args = parser.parse_args()

    import json

    config_payload = json.loads(Path(args.config).read_text(encoding='utf-8'))
    maad_bin = args.maad_bin if args.maad_bin is not None else config_payload.get('maad_bin', './tools/netflow-db/maad_fast')
    maad_backend = args.maad_backend if args.maad_backend is not None else str(config_payload.get('maad_backend', 'subprocess'))
    maad_workers = args.maad_workers if args.maad_workers is not None else int(config_payload.get('maad_workers', 1))

    with sqlite3.connect(args.database_path) as conn:
        enrich_database_from_config(
            conn,
            args.config,
            maad_bin=maad_bin,
            maad_backend=maad_backend,
            maad_workers=maad_workers,
        )


if __name__ == '__main__':
    main()
