#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_PATH="$ROOT_DIR/tools/netflow-db/maad_fast.cpp"
OUTPUT_PATH="$ROOT_DIR/tools/netflow-db/maad_fast"
CXX="${CXX:-g++}"

if [[ -x "$OUTPUT_PATH" && "$OUTPUT_PATH" -nt "$SOURCE_PATH" ]]; then
  exit 0
fi

"$CXX" -O3 -std=c++17 -o "$OUTPUT_PATH" "$SOURCE_PATH"
