# Coding Agent Guidelines

## Scope

This file is for coding agents working in this repository. Prefer concrete, verifiable commands over assumptions.

## Project Structure

- `apps/web/`: SvelteKit frontend and API routes.
- `apps/web/src/routes`: Pages and route-level loaders.
- `apps/web/src/routes/api`: HTTP endpoints and file-detail utilities.
- `apps/web/src/lib`: Shared UI, stores, helpers, and common types (`src/lib/types/types.ts`).
- `tools/netflow-db/`: Python data ingestion/aggregation scripts, logs, and SQLite database files.
- `vendor/maad/`: Haskell MAAD tooling (`nix-shell` + `./compile.sh` when MAAD binaries are needed).
- `vendor/burstify/`: Zig MAAD tooling.

## Backend Pipeline Notes

- `tools/netflow-db/flow_db.py`
- `tools/netflow-db/ip_db.py`
- `tools/netflow-db/protocol_db.py`
- `tools/netflow-db/spectrum_db.py`
- `tools/netflow-db/structure_db.py`
- `tools/netflow-db/discovery.py`
- `tools/netflow-db/pipeline.py`

## Git

Use Graphite to draft and create PRs:

```bash
# Create a branch with a single commit
#   - the --all flag will stage any modified files
#   - a branch will be created from the given `--message`
#   - the commit will have the given `--message`
#   - the branch will be checked out for you
gt create --all --message "feat(api): Add new API method for fetching users"

# Push changes to your remote and create a new pull request
gt submit


# If you need to make any follow up changes to the PR, you can
# amend the existing commit with gt modify
echo "some more changes" >> file.js
gt modify --all


# Submit new changes
gt submit
```

See `gt --help` for more.
Keep large generated artifacts out of git (SQLite DB/WAL/SHM files, captures, compiled binaries).

## Key Endpoints

Core routes commonly used by the frontend:

- `/api/netflow/stats`
- `/api/netflow/files/[slug]`
- `/api/netflow/files/[slug]/structure-function`

Other utility routes exist for protocol, IP, spectrum, singularities, and router metadata. Discover the full set in `apps/web/src/routes/api`.

## Commands

Frontend (`apps/web/`, run from repo root):

```bash
bun install
bun run build:web
bun run check:web
bun run lint:web
bun run format:web
bun run format:check:web
bun run precommit
```

Do not start `bun run dev:web` unless the user explicitly asks. Assume a dev server may already be running.

Backend (`tools/netflow-db/`) validation:

```bash
python -m compileall tools/netflow-db
```

Run backend compile checks after backend Python edits and before handing off work.

## Style Conventions

- Frontend: TypeScript + Svelte, 2-space indent, PascalCase components, shared types in `src/lib/types/types.ts`.
- Python: 4-space indent, snake_case, prefer `pathlib.Path` and environment-driven config.
- Keep comments short and only where logic is non-obvious.

## Testing Expectations

- Frontend changes: run `bun run precommit:web` from repo root.
- Backend changes: run `python -m compileall tools/netflow-db` from repo root.
- Add or update route-level smoke coverage when introducing new pages or endpoints.

## Environment & Data

Expected `.env` keys include:

- `DATASETS_CONFIG_PATH`
- `DEFAULT_DATASET`
- `LOG_PATH`
- `NETFLOW_DB_DIR`
- `MAAD_DIR`
- `FIRST_RUN`

Router identities are environment-configured and must not be hardcoded in documentation or logic assumptions. Use `YYYY-MM-DD HH:mm:ss` for timestamp formatting.
