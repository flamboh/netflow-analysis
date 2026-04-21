"""
CSV ingest contract for pipeline v2.

Strictly validates user-provided CSV mapping json and resolves the bucket/source
dimensions needed by downstream aggregators.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


TIMESTAMP_KEYS = ('time_received', 'time_end', 'time_start')
REQUIRED_COLUMN_KEYS = ('src_ip', 'dst_ip')


class CsvSourceConfigError(ValueError):
    """Raised when a CSV source mapping is invalid or incomplete."""


@dataclass(frozen=True)
class CsvSourceConfig:
    delimiter: str
    has_header: bool
    timestamp_format: str
    columns: dict[str, str]
    source_id_value: str | None
    source_id_column: str | None


def load_csv_source_config(path: str | Path) -> CsvSourceConfig:
    """Load and validate a v2 CSV source mapping file."""
    config_path = Path(path)
    with open(config_path, 'r', encoding='utf-8') as handle:
        payload = json.load(handle)
    return validate_csv_source_config(payload)


def validate_csv_source_config(payload: Mapping[str, Any]) -> CsvSourceConfig:
    """Validate a raw mapping payload and return the normalized config."""
    columns_raw = payload.get('columns')
    if not isinstance(columns_raw, Mapping):
        raise CsvSourceConfigError("CSV config must include a 'columns' object.")

    columns: dict[str, str] = {}
    for key, value in columns_raw.items():
        if value in (None, ''):
            continue
        if not isinstance(key, str) or not isinstance(value, str):
            raise CsvSourceConfigError('CSV column mappings must be string-to-string entries.')
        columns[key] = value

    if not any(key in columns for key in TIMESTAMP_KEYS):
        raise CsvSourceConfigError(
            "CSV config must map at least one timestamp column: "
            "time_received, time_end, or time_start."
        )

    for key in REQUIRED_COLUMN_KEYS:
        if key not in columns:
            raise CsvSourceConfigError(f"CSV config is missing required column mapping '{key}'.")

    source_id = payload.get('source_id')
    source_id_value: str | None = None
    source_id_column: str | None = None
    if isinstance(source_id, Mapping):
        raw_value = source_id.get('value')
        raw_column = source_id.get('column')
        if raw_value not in (None, ''):
            if not isinstance(raw_value, str):
                raise CsvSourceConfigError("CSV config source_id.value must be a string.")
            source_id_value = raw_value
        if raw_column not in (None, ''):
            if not isinstance(raw_column, str):
                raise CsvSourceConfigError("CSV config source_id.column must be a string.")
            source_id_column = raw_column

    if source_id_value is None and source_id_column is None:
        raise CsvSourceConfigError(
            "CSV config must declare source_id via either source_id.value or source_id.column."
        )

    delimiter = payload.get('delimiter', ',')
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise CsvSourceConfigError("CSV config delimiter must be a single character string.")

    has_header = payload.get('has_header', True)
    if not isinstance(has_header, bool):
        raise CsvSourceConfigError('CSV config has_header must be true or false.')

    timestamp_format = payload.get('timestamp_format', 'unix')
    if timestamp_format not in {'unix', 'unix_ms'}:
        raise CsvSourceConfigError("CSV config timestamp_format must be 'unix' or 'unix_ms'.")

    return CsvSourceConfig(
        delimiter=delimiter,
        has_header=has_header,
        timestamp_format=timestamp_format,
        columns=columns,
        source_id_value=source_id_value,
        source_id_column=source_id_column,
    )


def resolve_source_id(row: Mapping[str, Any], config: CsvSourceConfig) -> str:
    """Resolve the row source_id from a constant or mapped source column."""
    if config.source_id_value is not None:
        return config.source_id_value
    assert config.source_id_column is not None
    raw_value = row.get(config.source_id_column)
    if raw_value in (None, ''):
        raise CsvSourceConfigError(
            f"CSV row is missing source_id column '{config.source_id_column}'."
        )
    return str(raw_value)


def resolve_bucket_start(
    row: Mapping[str, Any],
    config: CsvSourceConfig,
    bucket_seconds: int = 300,
) -> int:
    """Resolve a row timestamp using configured precedence and floor to a bucket."""
    for logical_key in TIMESTAMP_KEYS:
        column_name = config.columns.get(logical_key)
        if column_name is None:
            continue
        raw_value = row.get(column_name)
        if raw_value in (None, ''):
            continue
        unix_ts = parse_timestamp(raw_value, config.timestamp_format)
        return floor_unix_timestamp(unix_ts, bucket_seconds)

    raise CsvSourceConfigError(
        'CSV row did not contain any usable timestamp value for the configured precedence.'
    )


def parse_timestamp(raw_value: Any, timestamp_format: str) -> int:
    """Parse the configured timestamp value into unix seconds."""
    try:
        numeric = float(str(raw_value).strip())
    except ValueError as error:
        raise CsvSourceConfigError(f"Invalid timestamp value '{raw_value}'.") from error

    if timestamp_format == 'unix':
        return int(numeric)
    if timestamp_format == 'unix_ms':
        return int(numeric) // 1000
    raise CsvSourceConfigError(f"Unsupported timestamp_format '{timestamp_format}'.")


def floor_unix_timestamp(unix_ts: int, bucket_seconds: int) -> int:
    """Floor a unix timestamp to the start of its bucket."""
    if bucket_seconds <= 0:
        raise CsvSourceConfigError('bucket_seconds must be > 0.')
    return unix_ts - (unix_ts % bucket_seconds)
