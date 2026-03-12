#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://nesg.ugr.es/nesg-ugr16/"
DEST_DIR="${RAW_DATASETS_PATH:-/research/obo/raw_datasets}"
URL_LIST_NAME="ugr16-nfcapd-urls.txt"
URLS_ONLY=0
DOWNLOADER=""

usage() {
  cat <<'EOF'
Usage: download_ugr16_nfcapd.sh [options]

Download only the UGR16 weekly nfcapd archives from the NESG dataset site.

Options:
  --dest DIR          Destination directory for archives and URL manifest.
                      Default: \$RAW_DATASETS_PATH or /research/obo/raw_datasets
  --urls-only         Only generate the URL manifest; do not download files.
  --downloader NAME   Force a downloader: aria2c, wget, or curl.
  -h, --help          Show this help text.

Examples:
  scripts/download_ugr16_nfcapd.sh
  scripts/download_ugr16_nfcapd.sh --urls-only
  scripts/download_ugr16_nfcapd.sh --dest /research/obo/raw_datasets
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dest)
      DEST_DIR="$2"
      shift 2
      ;;
    --urls-only)
      URLS_ONLY=1
      shift
      ;;
    --downloader)
      DOWNLOADER="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to scrape the dataset pages." >&2
  exit 1
fi

mkdir -p "$DEST_DIR"
URL_LIST_PATH="$DEST_DIR/$URL_LIST_NAME"
TMP_URL_LIST="$(mktemp)"
trap 'rm -f "$TMP_URL_LIST"' EXIT

python3 - "$BASE_URL" > "$TMP_URL_LIST" <<'PY'
import re
import sys
from urllib.parse import urljoin
from urllib.request import urlopen

base_url = sys.argv[1]


def fetch(path: str) -> str:
    with urlopen(urljoin(base_url, path), timeout=60) as response:
        return response.read().decode("utf-8", "replace")


def strip_html_comments(html: str) -> str:
    return re.sub(r"<!--.*?-->", "", html, flags=re.S)


index_html = strip_html_comments(fetch("index.php"))
month_pages = sorted(set(re.findall(r'href="([a-z]+\.php)#INI"', index_html)))
seen = set()

for month_page in month_pages:
    month_html = strip_html_comments(fetch(month_page))
    week_pages = sorted(set(re.findall(r'href="([a-z]+_week\d+\.php)#INI"', month_html)))
    for week_page in week_pages:
        week_html = strip_html_comments(fetch(week_page))
        archive_paths = re.findall(
            r'<input id="[^"]*nfcapd[^"]*hidden" type="hidden" value="([^"]+)"',
            week_html,
        )
        for archive_path in archive_paths:
            archive_url = urljoin(base_url, archive_path)
            if archive_url not in seen:
                seen.add(archive_url)
                print(archive_url)
PY

sort -u "$TMP_URL_LIST" > "$URL_LIST_PATH"
URL_COUNT="$(wc -l < "$URL_LIST_PATH" | tr -d ' ')"

if [[ "$URL_COUNT" -eq 0 ]]; then
  echo "No nfcapd archive URLs were found." >&2
  exit 1
fi

echo "Wrote $URL_COUNT archive URLs to $URL_LIST_PATH"
echo "Destination directory: $DEST_DIR"

if [[ "$URLS_ONLY" -eq 1 ]]; then
  exit 0
fi

if [[ -z "$DOWNLOADER" ]]; then
  if command -v aria2c >/dev/null 2>&1; then
    DOWNLOADER="aria2c"
  elif command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
  elif command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
  else
    echo "No supported downloader found. Install aria2c, wget, or curl." >&2
    exit 1
  fi
fi

case "$DOWNLOADER" in
  aria2c)
    exec aria2c -c -j 2 -x 4 -s 4 -i "$URL_LIST_PATH" -d "$DEST_DIR"
    ;;
  wget)
    exec wget \
      -c \
      --tries=0 \
      --retry-connrefused \
      --waitretry=30 \
      --read-timeout=60 \
      --timeout=60 \
      -i "$URL_LIST_PATH" \
      -P "$DEST_DIR"
    ;;
  curl)
    while IFS= read -r url; do
      filename="${url##*/}"
      echo "Downloading $filename"
      curl -fL --retry 10 --retry-delay 30 -C - -o "$DEST_DIR/$filename" "$url"
    done < "$URL_LIST_PATH"
    ;;
  *)
    echo "Unsupported downloader: $DOWNLOADER" >&2
    exit 1
    ;;
esac
