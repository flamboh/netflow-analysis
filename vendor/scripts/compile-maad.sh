#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
maad_dir="$repo_root/vendor/maad"

if [ ! -d "$maad_dir" ] || [ ! -f "$maad_dir/compile.sh" ]; then
	echo "vendor/maad not initialized; run: git submodule update --init --recursive" >&2
	exit 1
fi

cd "$maad_dir"

if ! command -v ghc >/dev/null 2>&1; then
	echo "ghc not found; install the dependencies from vendor/maad/shell.nix or use nix-shell." >&2
	exit 1
fi

required_packages=(
	bytestring
	hashable
	unordered-containers
	statistics
	wide-word
	treefold
)
package_args=()
for package in "${required_packages[@]}"; do
	package_args+=("-package" "$package")
done

ghc_version="$(ghc --numeric-version)"
store_dir="$(cabal path --store-dir)"
shopt -s nullglob
package_db_candidates=("$store_dir"/ghc-"$ghc_version"-*/package.db)
shopt -u nullglob

if [ ${#package_db_candidates[@]} -eq 0 ]; then
	echo "missing cabal package db for ghc $ghc_version; run cabal install --lib ${required_packages[*]}" >&2
	exit 1
fi

package_db="${package_db_candidates[0]}"
ghc_path="$(command -v ghc)"
printf -v ghc_path_escaped '%q' "$ghc_path"
printf -v package_db_escaped '%q' "$package_db"
printf -v package_args_escaped '%q ' "${package_args[@]}"

if ! env GHC_ENVIRONMENT=- ghc -package-db "$package_db" "${package_args[@]}" -e 'putStrLn "ok"' >/dev/null 2>&1; then
	echo "missing ghc packages: ${required_packages[*]}" >&2
	echo "install the dependencies from vendor/maad/shell.nix or use nix-shell." >&2
	exit 1
fi

temp_dir="$(mktemp -d)"
singularities_file="$maad_dir/Singularities.hs"
singularities_backup="$temp_dir/Singularities.hs"
cp "$singularities_file" "$singularities_backup"
trap 'cp "$singularities_backup" "$singularities_file"; rm -rf "$temp_dir"' EXIT

python3 - <<'PY'
from pathlib import Path

path = Path("Singularities.hs")
text = path.read_text()

old_import = """import qualified Data.List as L

import qualified Data.Vector.Unboxed as VU
import qualified Statistics.Regression as Reg
"""
new_import = """import qualified Data.List as L

import qualified Data.Vector.Unboxed as VU
import qualified Statistics.Matrix as Mat
import qualified Statistics.Regression as Reg
"""
old_regression = """      muLogs = VU.generate 33 oneLevel & VU.takeWhile snd & VU.map fst
      pl = VU.generate (VU.length muLogs) fromIntegral

      (coef, r2) = Reg.olsRegress [pl] muLogs
"""
new_regression = """      muLogs = VU.generate 33 oneLevel & VU.takeWhile snd & VU.map fst
      pl = VU.generate (VU.length muLogs) fromIntegral
      design =
        [ value
        | index <- [0 .. VU.length muLogs - 1]
        , value <- [pl VU.! index, 1.0]
        ]
          & Mat.fromList (VU.length muLogs) 2

      coef = Reg.ols design muLogs
      r2 = Reg.rSquare design muLogs coef
"""

if old_import not in text or old_regression not in text:
    raise SystemExit("Unexpected Singularities.hs contents; update compile-maad.sh patch logic.")

path.write_text(text.replace(old_import, new_import).replace(old_regression, new_regression))
PY

cat >"$temp_dir/ghc" <<EOF
#!/usr/bin/env bash
exec $ghc_path_escaped -package-db $package_db_escaped $package_args_escaped "\$@"
EOF

chmod +x "$temp_dir/ghc"

if command -v nix-shell >/dev/null 2>&1; then
	env GHC_ENVIRONMENT=- PATH="$temp_dir:$PATH" nix-shell shell.nix --run "./compile.sh"
else
	env GHC_ENVIRONMENT=- PATH="$temp_dir:$PATH" ./compile.sh
fi
