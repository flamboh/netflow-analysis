# File Detail DB Graphs Plan

## Goal

Move the file-detail graph routes from live CLI execution to database-backed reads, while keeping the file page contract stable.

## Scope

- Replace the CLI implementation in `netflow-webapp/src/routes/api/netflow/files/[slug]/spectrum/+server.ts` with a `spectrum_stats` lookup.
- Rename `netflow-webapp/src/routes/api/netflow/files/[slug]/structure-function/+server.ts` to `structure/+server.ts` and replace its CLI implementation with a `structure_stats` lookup.
- Keep `netflow-webapp/src/routes/api/netflow/files/[slug]/singularities/+server.ts` CLI-backed.
- Update the structure chart and related types to use `tau` instead of `tauTilde`.

## Plan

### 1. Share file-bucket timestamp parsing

Extract the existing `slug -> bucket_start` PST conversion from `ip-counts/+server.ts` into a shared helper under the file-detail route utilities.

Use that helper from:

- `ip-counts/+server.ts`
- `spectrum/+server.ts`
- `structure/+server.ts`

This keeps all file-scoped DB lookups aligned on the same timestamp semantics.

### 2. Convert spectrum to database reads

Update `files/[slug]/spectrum/+server.ts` to query `spectrum_stats` with:

- `router = ?`
- `granularity = '5m'`
- `bucket_start = ?`
- `ip_version = 4`

Parse the stored JSON and return the same file-page response shape the current chart already consumes.

### 3. Rename and convert structure route

Rename `files/[slug]/structure-function/+server.ts` to `files/[slug]/structure/+server.ts`.

Update it to query `structure_stats` with the same file-scoped filters used for spectrum.

Return the existing chart-friendly payload shape, but use `tau` consistently in the response data.

### 4. Update the file page to use the renamed route

Change the fetch in `files/[slug]/+page.svelte` from `/structure-function` to `/structure`.

Keep the rest of the file-page loading flow unchanged for this pass.

### 5. Normalize the structure chart to `tau`

Update `StructureFunctionChart.svelte` and any related local typing to read `tau`.

Remove the old `tauTilde` naming so the chart matches the database schema and shared app types.

## Validation

- Run `npm run validate` in `netflow-webapp/`.
- Verify the file-detail page still renders structure, spectrum, IP counts, and CLI-backed singularities for a known slug.
