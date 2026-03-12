#!/usr/bin/env python3
"""
Prepare external nfcapd datasets into the canonical dataset/source/date layout.

Supported first-pass flows:
- extract immutable archives from /research/obo/raw_datasets
- normalize week-long binary nfdump files into 5-minute nfcapd buckets
- write output under /research/obo/netflow_datasets/<dataset>/<source>/YYYY/MM/DD/
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

from common import get_dataset_root_path


TIMESTAMP_RE = re.compile(r"^nfcapd\.(\d{12})$")
DEFAULT_INTERVAL_SECONDS = 300


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a command and raise on failure."""
    return subprocess.run(command, capture_output=True, text=True, check=True)


def parse_nfdump_summary(file_path: Path) -> tuple[datetime, datetime]:
    """Return the first/last flow timestamps from `nfdump -I`."""
    result = run_command(["nfdump", "-I", "-r", str(file_path)])
    first = None
    last = None

    for line in result.stdout.splitlines():
        if line.startswith("First:"):
            first = int(line.split(":", 1)[1].strip())
        elif line.startswith("Last:"):
            last = int(line.split(":", 1)[1].strip())

    if first is None or last is None:
        raise ValueError(f"Could not parse first/last timestamps from {file_path}")

    return datetime.fromtimestamp(first), datetime.fromtimestamp(last)


def floor_bucket(dt: datetime, interval_seconds: int) -> datetime:
    """Floor a datetime to the interval boundary."""
    seconds = int(dt.timestamp())
    return datetime.fromtimestamp((seconds // interval_seconds) * interval_seconds)


def format_nfdump_time_range(start: datetime, end: datetime) -> str:
    """Format a time range for `nfdump -t`."""
    return f"{start.strftime('%Y/%m/%d.%H:%M:%S')}-{end.strftime('%Y/%m/%d.%H:%M:%S')}"


def iter_bucket_starts(
    first: datetime,
    last: datetime,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    max_buckets: int | None = None,
) -> Iterator[datetime]:
    """Yield bucket starts from the first to last observed flow time."""
    current = floor_bucket(first, interval_seconds)
    last_bucket = floor_bucket(last, interval_seconds)
    emitted = 0

    while current <= last_bucket:
        yield current
        emitted += 1
        if max_buckets is not None and emitted >= max_buckets:
            return
        current += timedelta(seconds=interval_seconds)


def canonical_bucket_path(dataset_root: Path, source: str, bucket_start: datetime) -> Path:
    """Return the canonical output file path for a 5-minute bucket."""
    return (
        dataset_root
        / source
        / bucket_start.strftime("%Y")
        / bucket_start.strftime("%m")
        / bucket_start.strftime("%d")
        / f"nfcapd.{bucket_start.strftime('%Y%m%d%H%M')}"
    )


def segment_large_file(
    source_file: Path,
    dataset_root: Path,
    source: str,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    max_buckets: int | None = None,
) -> dict[str, int]:
    """Slice a large binary file into canonical 5-minute bucket files."""
    first, last = parse_nfdump_summary(source_file)
    stats = {"written": 0, "skipped_existing": 0}

    print(f"[prepare] Segmenting {source_file}")
    print(f"[prepare] Flow range: {first} -> {last}")

    for bucket_start in iter_bucket_starts(first, last, interval_seconds, max_buckets=max_buckets):
        bucket_end = bucket_start + timedelta(seconds=interval_seconds - 1)
        output_path = canonical_bucket_path(dataset_root, source, bucket_start)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.exists():
            stats["skipped_existing"] += 1
            continue

        time_range = format_nfdump_time_range(bucket_start, bucket_end)
        run_command(
            [
                "nfdump",
                "-r",
                str(source_file),
                "-t",
                time_range,
                "-w",
                str(output_path),
            ]
        )
        stats["written"] += 1

    return stats


def extract_archive_member(archive_path: Path, member_name: str, destination: Path) -> Path:
    """Extract one file from a tar archive to a destination path."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:*") as archive:
        member = archive.getmember(member_name)
        extracted = archive.extractfile(member)
        if extracted is None:
            raise ValueError(f"Could not extract member '{member_name}' from {archive_path}")
        with open(destination, "wb") as output:
            shutil.copyfileobj(extracted, output)
    return destination


def prepare_archive(
    archive_path: Path,
    dataset: str,
    source: str,
    max_buckets: int | None = None,
) -> dict[str, int]:
    """Prepare one archive into canonical bucket files."""
    dataset_root = get_dataset_root_path(dataset)
    dataset_root.parent.mkdir(parents=True, exist_ok=True)
    stats = {"written": 0, "skipped_existing": 0, "members": 0}

    with tempfile.TemporaryDirectory(prefix=f"{dataset}-{source}-", dir=str(dataset_root.parent)) as temp_dir:
        temp_root = Path(temp_dir)
        with tarfile.open(archive_path, "r:*") as archive:
            regular_members = [member for member in archive.getmembers() if member.isfile()]

        if not regular_members:
            raise ValueError(f"No regular files found in archive {archive_path}")

        for member in regular_members:
            stats["members"] += 1
            member_name = Path(member.name).name
            extracted_path = extract_archive_member(archive_path, member.name, temp_root / member_name)

            match = TIMESTAMP_RE.match(member_name)
            if match:
                timestamp = datetime.strptime(match.group(1), "%Y%m%d%H%M")
                output_path = canonical_bucket_path(dataset_root, source, timestamp)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                if output_path.exists():
                    stats["skipped_existing"] += 1
                else:
                    shutil.move(str(extracted_path), output_path)
                    stats["written"] += 1
                continue

            segmented = segment_large_file(
                extracted_path,
                dataset_root=dataset_root,
                source=source,
                max_buckets=max_buckets,
            )
            stats["written"] += segmented["written"]
            stats["skipped_existing"] += segmented["skipped_existing"]

    return stats


def main():
    parser = argparse.ArgumentParser(description="Prepare external nfcapd datasets")
    parser.add_argument("--dataset", required=True, help="Dataset id from datasets.json")
    parser.add_argument("--source", default="default", help="Source id to write under the dataset")
    parser.add_argument("--archive", required=True, help="Path to a raw immutable archive")
    parser.add_argument(
        "--max-buckets",
        type=int,
        default=None,
        help="Optional cap on the number of 5-minute buckets to emit for testing",
    )
    args = parser.parse_args()

    archive_path = Path(args.archive).expanduser()
    if not archive_path.exists():
        raise SystemExit(f"Archive not found: {archive_path}")

    stats = prepare_archive(
        archive_path=archive_path,
        dataset=args.dataset,
        source=args.source,
        max_buckets=args.max_buckets,
    )

    print(f"[prepare] Archive: {archive_path}")
    print(f"[prepare] Dataset: {args.dataset}")
    print(f"[prepare] Source: {args.source}")
    print(f"[prepare] Members handled: {stats['members']}")
    print(f"[prepare] Bucket files written: {stats['written']}")
    print(f"[prepare] Existing bucket files skipped: {stats['skipped_existing']}")


if __name__ == "__main__":
    main()
