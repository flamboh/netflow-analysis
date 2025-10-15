#!/usr/bin/env python3
"""
### WHOLLY AI GENERATED CODE, USED FOR QUICK VERIFICATION OF IP UNIQUENESS ###
Utility script to check whether NetFlow IPs are unique across routers.

The script mirrors the nfdump query pattern in ip_db.py: for each router and
5-minute bucket it runs nfdump with the `fmt:%sa,%da` formatter to capture
unique source/destination addresses.  After aggregating IPv4 and (optionally)
IPv6 addresses it reports any overlaps between routers so operators can spot
non-unique IP usage.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


def load_env_file(env_path: str) -> None:
    """Populate os.environ with key/value pairs from a dotenv-style file."""
    env_file = Path(env_path)
    if not env_file.exists():
        print(f"ERROR: Environment file '{env_path}' not found.", file=sys.stderr)
        sys.exit(1)

    with env_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()


def get_required_env(key: str) -> str:
    """Return a required environment variable or exit with an error."""
    value = os.environ.get(key)
    if not value:
        print(
            f"ERROR: Required environment variable '{key}' is not set.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


def parse_timestamp(raw: str) -> datetime:
    """Parse a timestamp string in one of the accepted formats."""
    accepted_formats = ("%Y%m%d%H%M", "%Y-%m-%d %H:%M")
    for fmt in accepted_formats:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    print(
        f"ERROR: '{raw}' is not a valid timestamp. "
        "Use 'YYYYMMDDHHMM' or 'YYYY-MM-DD HH:MM'.",
        file=sys.stderr,
    )
    sys.exit(1)


def iter_buckets(start: datetime, end: datetime) -> Iterable[datetime]:
    """Yield 5-minute bucket timestamps inclusive of the start and end."""
    current = start
    while current <= end:
        yield current
        current += timedelta(minutes=5)


def build_nfcapd_path(base: Path, router: str, bucket: datetime) -> Path:
    """Construct the absolute path to the nfcapd file for a router and bucket."""
    timestamp = bucket.strftime("%Y%m%d%H%M")
    return (
        base
        / router
        / bucket.strftime("%Y")
        / bucket.strftime("%m")
        / bucket.strftime("%d")
        / f"nfcapd.{timestamp}"
    )


def run_nfdump(file_path: Path, extra_args: List[str], timeout: int) -> List[str]:
    """Execute nfdump and return parsed lines (source,destination) from stdout."""
    base_cmd = [
        "nfdump",
        "-r",
        str(file_path),
        "-q",
        "-o",
        "fmt:%sa,%da",
        "-n",
        "0",
    ]

    try:
        result = subprocess.run(
            base_cmd + extra_args,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError:
        print(
            "ERROR: nfdump executable not found on PATH. "
            "Install nfdump or adjust PATH before running this script.",
            file=sys.stderr,
        )
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print(f"ERROR: nfdump timed out while processing {file_path}", file=sys.stderr)
        return []

    if result.returncode != 0:
        print(
            f"WARNING: nfdump returned code {result.returncode} for {file_path}.\n"
            f"stderr: {result.stderr.strip()}",
            file=sys.stderr,
        )
        return []

    lines = []
    for raw_line in result.stdout.splitlines():
        raw_line = raw_line.strip()
        if not raw_line or "," not in raw_line:
            continue
        lines.append(raw_line)
    return lines


def collect_file_ips(file_path: Path, include_ipv6: bool, timeout: int) -> Dict[str, Set[str]]:
    """Collect IPv4/IPv6 source/destination IPs from a single nfcapd file."""
    results: Dict[str, Set[str]] = {
        "sa_ipv4": set(),
        "da_ipv4": set(),
        "sa_ipv6": set(),
        "da_ipv6": set(),
    }

    if not file_path.exists():
        return results

    ipv4_lines = run_nfdump(file_path, ["ipv4"], timeout)
    for line in ipv4_lines:
        try:
            src, dst = line.split(",", 1)
            results["sa_ipv4"].add(src)
            results["da_ipv4"].add(dst)
        except ValueError:
            continue

    if include_ipv6:
        ipv6_lines = run_nfdump(file_path, ["ipv6", "-6"], timeout)
        for line in ipv6_lines:
            try:
                src, dst = line.split(",", 1)
                results["sa_ipv6"].add(src)
                results["da_ipv6"].add(dst)
            except ValueError:
                continue

    return results


def collect_router_ips(
    router: str,
    buckets: Iterable[datetime],
    base_path: Path,
    include_ipv6: bool,
    timeout: int,
    verbose: bool,
) -> Tuple[Dict[str, Set[str]], List[Path]]:
    """Aggregate IP sets for a router across all buckets."""
    aggregate: Dict[str, Set[str]] = {
        "sa_ipv4": set(),
        "da_ipv4": set(),
        "sa_ipv6": set(),
        "da_ipv6": set(),
    }
    missing_files: List[Path] = []

    for bucket in buckets:
        file_path = build_nfcapd_path(base_path, router, bucket)
        if not file_path.exists():
            missing_files.append(file_path)
            if verbose:
                print(f"INFO: Skipping missing file {file_path}")
            continue

        file_results = collect_file_ips(file_path, include_ipv6, timeout)
        for key, values in file_results.items():
            aggregate[key].update(values)

    return aggregate, missing_files


def compute_overlaps(
    router_data: Dict[str, Dict[str, Set[str]]]
) -> Dict[Tuple[str, str], Dict[str, Set[str]]]:
    """Compute pairwise overlaps between routers for each address category."""
    overlaps: Dict[Tuple[str, str], Dict[str, Set[str]]] = {}
    keys = ("sa_ipv4", "da_ipv4", "sa_ipv6", "da_ipv6")

    for left, right in combinations(router_data.keys(), 2):
        pair_key = (left, right)
        overlaps[pair_key] = {}
        for key in keys:
            overlaps[pair_key][key] = router_data[left][key] & router_data[right][key]

    return overlaps


def render_summary(
    router_data: Dict[str, Dict[str, Set[str]]],
    overlaps: Dict[Tuple[str, str], Dict[str, Set[str]]],
    sample_size: int,
) -> None:
    """Pretty-print collection and overlap statistics."""
    print("=== Router IP Summary ===")
    for router, buckets in router_data.items():
        ipv4_total = len(buckets["sa_ipv4"] | buckets["da_ipv4"])
        ipv6_total = len(buckets["sa_ipv6"] | buckets["da_ipv6"])
        print(
            f"{router}: "
            f"IPv4 sources={len(buckets['sa_ipv4'])}, "
            f"IPv4 destinations={len(buckets['da_ipv4'])}, "
            f"IPv4 combined={ipv4_total}; "
            f"IPv6 sources={len(buckets['sa_ipv6'])}, "
            f"IPv6 destinations={len(buckets['da_ipv6'])}, "
            f"IPv6 combined={ipv6_total}"
        )

    print("\n=== Pairwise Overlaps ===")
    any_overlap = False
    for (left, right), pair_data in overlaps.items():
        overlap_counts = {
            key: len(values) for key, values in pair_data.items() if values
        }
        if not overlap_counts:
            print(f"{left} ↔ {right}: no overlaps detected")
            continue

        any_overlap = True
        print(f"{left} ↔ {right}:")
        for key, values in pair_data.items():
            if not values:
                continue
            sample = sorted(values)[:sample_size]
            sample_preview = ", ".join(sample)
            print(
                f"  {key}: {len(values)} shared "
                f"({sample_preview}{'...' if len(values) > sample_size else ''})"
            )

    if not any_overlap:
        print("All routers have unique IPs for the provided window.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect NetFlow captures and verify whether unique IPs are shared "
            "across routers."
        )
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start timestamp (YYYYMMDDHHMM or 'YYYY-MM-DD HH:MM').",
    )
    parser.add_argument(
        "--end",
        help=(
            "Optional end timestamp (inclusive). Defaults to start if omitted. "
            "Must align to 5-minute boundaries."
        ),
    )
    parser.add_argument(
        "--routers",
        nargs="+",
        help="Optional subset of routers to inspect. Defaults to AVAILABLE_ROUTERS.",
    )
    parser.add_argument(
        "--skip-ipv6",
        action="store_true",
        help="Skip IPv6 processing to speed up the check.",
    )
    parser.add_argument(
        "--env-path",
        default="../.env",
        help="Path to the environment file (defaults to ../.env).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="nfdump timeout per file in seconds (default: 300).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Number of overlapping IPs to display as a sample (default: 10).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (e.g., missing file notices).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    load_env_file(args.env_path)
    netflow_data_path = Path(get_required_env("NETFLOW_DATA_PATH"))
    available_routers = get_required_env("AVAILABLE_ROUTERS").split(",")
    routers = args.routers if args.routers else available_routers

    unknown = sorted(set(routers) - set(available_routers))
    if unknown:
        print(
            f"ERROR: Router(s) {', '.join(unknown)} not found in AVAILABLE_ROUTERS.",
            file=sys.stderr,
        )
        sys.exit(1)

    start_dt = parse_timestamp(args.start)
    if args.end:
        end_dt = parse_timestamp(args.end)
    else:
        end_dt = start_dt

    if end_dt < start_dt:
        print("ERROR: --end must be after --start.", file=sys.stderr)
        sys.exit(1)

    buckets = list(iter_buckets(start_dt, end_dt))
    if not buckets:
        print("ERROR: No buckets generated for the provided window.", file=sys.stderr)
        sys.exit(1)

    router_data: Dict[str, Dict[str, Set[str]]] = {}
    missing_summary: Dict[str, List[Path]] = {}
    include_ipv6 = not args.skip_ipv6

    for router in routers:
        aggregate, missing_files = collect_router_ips(
            router=router,
            buckets=buckets,
            base_path=netflow_data_path,
            include_ipv6=include_ipv6,
            timeout=args.timeout,
            verbose=args.verbose,
        )
        router_data[router] = aggregate
        if missing_files:
            missing_summary[router] = missing_files

    overlaps = compute_overlaps(router_data)
    render_summary(router_data, overlaps, args.sample_size)

    if missing_summary:
        print("\n=== Missing Files ===")
        for router, files in missing_summary.items():
            print(f"{router}: {len(files)} files missing")
            if args.verbose:
                for file_path in files:
                    print(f"  {file_path}")


if __name__ == "__main__":
    main()

