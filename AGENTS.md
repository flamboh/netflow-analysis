# Coding Agent Guidelines

## Scope

This file is for coding agents working in this repository. Prefer concrete, verifiable commands over assumptions.

## Project Structure

- `netflow-webapp/`: SvelteKit frontend and API routes.
- `netflow-webapp/src/routes`: Pages and route-level loaders.
- `netflow-webapp/src/routes/api`: HTTP endpoints and file-detail utilities.
- `netflow-webapp/src/lib`: Shared UI, stores, helpers, and common types (`src/lib/types/types.ts`).
- `netflow-db/`: Python data ingestion/aggregation scripts, logs, and SQLite database files.
- `maad/`: Haskell MAAD tooling (`nix-shell` + `./compile.sh` when MAAD binaries are needed).
- `burstify/`: Zig MAAD tooling.

## Backend Pipeline Notes

- `netflow-db/flow_db.py`
- `netflow-db/ip_db.py`
- `netflow-db/protocol_db.py`
- `netflow-db/spectrum_db.py`
- `netflow-db/structure_db.py`
- `netflow-db/discovery.py`
- `netflow-db/pipeline.py`

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

Other utility routes exist for protocol, IP, spectrum, singularities, and router metadata. Discover the full set in `netflow-webapp/src/routes/api`.

## Commands

Frontend (`netflow-webapp/`):

```bash
npm install
npm run build
npm run check
npm run lint
npm run format
npm run validate
```

Do not start `npm run dev` or `npm run preview` unless the user explicitly asks. Assume a dev server may already be running.

Backend (`netflow-db/`) validation:

```bash
python -m py_compile *.py
```

Run backend compile checks after backend Python edits and before handing off work.

## Style Conventions

- Frontend: TypeScript + Svelte, 2-space indent, PascalCase components, shared types in `src/lib/types/types.ts`.
- Python: 4-space indent, snake_case, prefer `pathlib.Path` and environment-driven config.
- Keep comments short and only where logic is non-obvious.

## Testing Expectations

- Frontend changes: run `npm run validate` in `netflow-webapp/`.
- Backend changes: run `python -m py_compile *.py` in `netflow-db/`.
- Add or update route-level smoke coverage when introducing new pages or endpoints.

## Environment & Data

Expected `.env` keys include:

- `NETFLOW_DATA_PATH`
- `AVAILABLE_ROUTERS`
- `DATABASE_PATH`
- `LOG_PATH`
- `FIRST_RUN`

Router identities are environment-configured and must not be hardcoded in documentation or logic assumptions. Use `YYYY-MM-DD HH:mm:ss` for timestamp formatting.
