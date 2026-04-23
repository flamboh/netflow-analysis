#!/usr/bin/env python3
"""Scrape UGR16 weekly download URLs for a specific asset kind."""

from __future__ import annotations

import argparse
import re
import sys
from urllib.parse import urljoin
from urllib.request import urlopen


def fetch(base_url: str, path: str) -> str:
    with urlopen(urljoin(base_url, path), timeout=60) as response:
        return response.read().decode("utf-8", "replace")


def strip_html_comments(html: str) -> str:
    return re.sub(r"<!--.*?-->", "", html, flags=re.S)


def iter_week_pages(base_url: str) -> list[str]:
    index_html = strip_html_comments(fetch(base_url, "index.php"))
    month_pages = sorted(set(re.findall(r'href="([a-z]+\.php)#INI"', index_html)))
    week_pages: set[str] = set()
    for month_page in month_pages:
        month_html = strip_html_comments(fetch(base_url, month_page))
        week_pages.update(re.findall(r'href="([a-z]+_week\d+\.php)#INI"', month_html))
    return sorted(week_pages)


def iter_hidden_values(base_url: str) -> list[str]:
    values: list[str] = []
    for week_page in iter_week_pages(base_url):
        week_html = strip_html_comments(fetch(base_url, week_page))
        values.extend(
            re.findall(
                r'<input id="[^"]*hidden" type="hidden" value="([^"]+)"',
                week_html,
            )
        )
    return values


def is_nfcapd_path(path: str) -> bool:
    return "nfcapd" in path.lower()


def is_csv_path(path: str) -> bool:
    filename = path.rsplit("/", 1)[-1].lower()
    return re.fullmatch(r"[a-z]+_week\d+_csv\.tar\.gz", filename) is not None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="UGR16 dataset base URL")
    parser.add_argument(
        "--kind",
        required=True,
        choices=("nfcapd", "csv"),
        help="Asset kind to extract",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    matcher = is_nfcapd_path if args.kind == "nfcapd" else is_csv_path
    seen: set[str] = set()

    for path in iter_hidden_values(args.base_url):
        if not matcher(path):
            continue
        url = urljoin(args.base_url, path)
        if url in seen:
            continue
        seen.add(url)
        print(url)

    return 0


if __name__ == "__main__":
    sys.exit(main())
