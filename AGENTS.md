# Repository Guidelines

## Project Structure & Module Organization
`netflow-webapp/` covers SvelteKit: `src/routes` for pages, `src/routes/api` for endpoints, shared UI and types in `src/lib` (extend `src/lib/types/types.ts`), assets in `static/`. `netflow-db/` houses `flow_db.py`, `ip_db.py`, planned `maad_db.py`, logs, and `flowStats.db`. `maad/` contains the Haskell MAAD tools; compile with `nix-shell` and `./compile.sh`. Keep binaries, SQLite files, and captures out of git.

## Architecture & Key Endpoints
`flow_db.py` ingests captures from `NETFLOW_DATA_PATH` into `flowStats.db`; `maad_db.py` will batch MAAD spectra into that store. The frontend queries `better-sqlite3` endpoints—`/api/netflow/stats`, `/api/netflow/files/[slug]`, `/api/netflow/files/[slug]/structure-function`—and renders Chart.js views in `src/routes/+page.svelte`. File detail pages surface structure-function plots for routers `cc-ir1-gw` and `oh-ir1-gw`.

## Build, Test, and Development Commands
```bash
cd netflow-webapp
npm install
npm run dev
npm run build
npm run preview
npm run check
npm run lint
npm run format
```
Commands map to dev server (`dev`), production bundle and preview (`build`/`preview`), type checks (`check`), linting (`lint`), and formatting (`format`).
```bash
cd netflow-db
poetry install
poetry run python flow_db.py
```
Refresh `flowStats.db` with the command above; invoke `maad_db.py` when caching spectra.  
```bash
cd maad
nix-shell
./compile.sh
./Singularities 10 test_data/simple.csv
```
Use the Nix shell for GHC deps, then run MAAD binaries against fixtures in `test_data/`.

## Coding Style & Naming Conventions
Frontend code follows TypeScript + Svelte conventions with 2-space indent; keep components PascalCase, colocate helpers in `src/lib`, and share types through `src/lib/types/types.ts`. Let the Prettier Tailwind plugin order utility classes. Python scripts use 4-space indent, snake_case, and `Path` utilities with configuration sourced from `.env`. Haskell modules stay CamelCase with minimal inline notes.

## Testing Guidelines
Run `npm run check` and `npm run lint` before committing, adding route-level smoke tests for new pages or endpoints. Validate ingestion changes by running `poetry run python flow_db.py` on a small window and checking counts via `sqlite3 flowStats.db "SELECT COUNT(*) FROM netflow_stats;"`. Exercise MAAD updates against `maad/test_data/` and regenerate plots through `maad/plots/` when logic changes.

## Commit & Pull Request Guidelines
Keep commit subjects short, present-tense, and single-sentence (`parallelize ip_db.py with multiprocessing`). PRs should call out intent, affected areas, validation commands, and environment needs (SSH tunnel, `.env`). Link tickets when available, attach refreshed UI screenshots for frontend tweaks, and flag database or schema updates.

## Data Access & Configuration
Populate `.env` with `NETFLOW_DATA_PATH`, `AVAILABLE_ROUTERS`, and `DATABASE_PATH`. NetFlow captures stay on the secured storage referenced by `NETFLOW_DATA_PATH`; keep those assets out of git. Use SSH tunneling (`ssh -L 5173:localhost:5173 user@pinot`) for remote datasets and format timestamps as `YYYY-MM-DD HH:mm:ss`.
