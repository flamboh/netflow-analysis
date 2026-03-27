#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -eq 0 ]; then
	echo "usage: $0 <command> [args...]" >&2
	exit 1
fi

if command -v nix-shell >/dev/null 2>&1; then
	nix-shell --run "$(printf '%q ' "$@")"
else
	"$@"
fi
