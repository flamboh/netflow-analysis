# Multi-Dataset NFCapd Port Plan

## Goal

Port the current single-database, router-centric setup into a multi-dataset model where:

- each dataset has its own dashboard
- each dataset has its own SQLite database
- each dataset may expose one or more sources/routers inside that dashboard

This keeps the product focused on time-series analysis without introducing meaningless cross-dataset unions.

## Desired Outcome

- UOregon remains usable from its existing synced `nfcapd` tree.
- Static external `nfcapd` datasets such as UGR16 can be prepared once into a canonical layout.
- Each dataset is processed into its own database under a repo-local `./data` directory.
- The web app routes users into dataset-specific dashboards rather than a single global dashboard.
- Existing processor logic stays mostly intact inside each dataset database.

## Core Decision

Do not build one mega-database spanning all datasets.

Instead:

- maintain a dataset registry
- run the pipeline against one dataset at a time
- write one SQLite database per dataset
- serve one dashboard context per dataset

This is preferable because:

- different datasets should not be merged for aggregate queries
- repair and reprocessing become per-dataset operations
- static and live datasets can coexist without sharing one index space
- most current table schemas can remain router-based within each dataset

## Data Locations

### 1. Raw immutable inputs

Use `/research/obo/raw_datasets` as the immutable source area for downloaded external archives and files.

Examples:

- `/research/obo/raw_datasets/april_week2_nfcapd.tar.gz`
- `/research/obo/raw_datasets/july_week5_nfcapd.tar.gz`

Rules:

- treat this directory as read-only source material
- do not rewrite or reorganize files in place
- keep downloader artifacts or URL manifests here when useful

### 2. Prepared dataset roots

Use `/research/obo/netflow_datasets` as the top-level working area for dataset inputs after preparation.

Examples:

- `/research/obo/netflow_datasets/uoregon -> /research/tango_cis/uonet-in`
- `/research/obo/netflow_datasets/ugr16/default/YYYY/MM/DD/nfcapd.YYYYMMDDHHmm`

### 3. Repo-local databases

Add a repo-local `./data` directory for generated databases.

Recommended shape:

- `./data/uoregon/netflow.sqlite`
- `./data/ugr16/netflow.sqlite`
- `./data/dataset3/netflow.sqlite`

Optional supporting files per dataset:

- `./data/<dataset>/netflow.sqlite-wal`
- `./data/<dataset>/netflow.sqlite-shm`
- `./data/<dataset>/manifests/...`
- `./data/<dataset>/logs/...`

This keeps all generated DB state clearly partitioned by dataset.

## Dataset Identity Model

At the application level, identity becomes:

- `dataset_id`
- `source_id`
- `timestamp`

Examples:

- `uoregon/cc_ir1_gw`
- `uoregon/oh_ir1_gw`
- `ugr16/default`

At the database level, each dataset has its own DB, so `source_id` can continue to use the existing `router` column semantics inside that DB.

That means:

- no global `dataset` column is required inside every processing table
- the dataset is implied by which database is open
- `router` can remain the per-dataset source dimension

## Filesystem Model

Prepared static datasets should use a canonical layout after migration out of `/research/obo/raw_datasets`:

- `/research/obo/netflow_datasets/<dataset_id>/<source_id>/<YYYY>/<MM>/<DD>/nfcapd.<YYYYMMDDHHmm>`

Examples:

- `/research/obo/netflow_datasets/ugr16/default/2016/05/30/nfcapd.201605300000`
- `/research/obo/netflow_datasets/dataset3/default/...`

UOregon remains a live special case:

- `/research/obo/netflow_datasets/uoregon/<router>/<YYYY>/<MM>/<DD>/nfcapd.*`

because the top-level dataset path is a symlink to the upstream synced tree.

## UOregon Special Case

UOregon should not be copied or rewritten.

Current approved approach:

- `/research/obo/netflow_datasets/uoregon -> /research/tango_cis/uonet-in`

Why this is acceptable:

- only one top-level symlink is involved
- the downstream tree already matches `source/YYYY/MM/DD/nfcapd.*`
- discovery can traverse it normally
- no per-file link farm is required

Operational assumption:

- if the upstream tree remains stable, discovery treats it as the dataset root for `uoregon`

## Proposed Configuration Model

Replace the current global environment model with a dataset registry.

Each dataset entry should define:

- `dataset_id`
- `label`
- `root_path`
- `db_path`
- `source_mode`
- `discovery_mode`
- optional source allowlist
- optional metadata

Suggested first-pass shape:

```json
[
  {
    "dataset_id": "uoregon",
    "label": "UOregon",
    "root_path": "/research/obo/netflow_datasets/uoregon",
    "db_path": "./data/uoregon/netflow.sqlite",
    "source_mode": "subdirs",
    "discovery_mode": "live"
  },
  {
    "dataset_id": "ugr16",
    "label": "UGR16",
    "root_path": "/research/obo/netflow_datasets/ugr16",
    "db_path": "./data/ugr16/netflow.sqlite",
    "source_mode": "subdirs",
    "discovery_mode": "static"
  }
]
```

`source_mode = subdirs` means:

- immediate child directories below `root_path` are treated as sources

This covers both:

- `uoregon/<router>/...`
- `ugr16/default/...`

## Why Not A Shared Database

A shared DB would require:

- threading `dataset` through every table and index
- changing primary keys across all aggregate tables
- guarding every query against invalid cross-dataset combinations

That complexity is not justified if:

- dashboards are dataset-specific
- cross-dataset comparison is not an immediate requirement
- datasets should be processed and repaired independently

## Database Strategy

Keep the existing per-dataset schemas mostly intact.

Inside each dataset DB:

- `router` continues to mean "source within this dataset"
- `processed_files`, `netflow_stats`, `ip_stats`, `protocol_stats`, `spectrum_stats`, and `structure_stats` can remain structurally similar

What changes:

- each dataset gets its own SQLite file
- the pipeline opens the DB for the selected dataset
- the web app opens the DB for the selected dataset

This avoids a disruptive schema rewrite.

## Migration Strategy

### Phase 1. Introduce the dataset registry

Create a registry that defines:

- dataset root path
- dataset DB path
- source discovery mode
- dataset label and metadata

### Phase 2. Move DB location selection out of `.env`

The current single `DATABASE_PATH` assumption should be replaced by:

- a dataset-aware DB resolver

The resolver should:

- create `./data/<dataset>/` if needed
- open `./data/<dataset>/netflow.sqlite`

### Phase 3. Keep existing schema inside each DB

Do not rename `router` yet.

Within a given dataset DB:

- existing tables can continue to use `router`
- current processor insert/select logic can be adapted with minimal changes

### Phase 4. Backfill current deployment into `uoregon`

Treat the existing active database as the seed for:

- `./data/uoregon/netflow.sqlite`

This preserves current data while transitioning to the per-dataset layout.

## Discovery Refactor

Refactor discovery from:

- one global root
- one global `AVAILABLE_ROUTERS`

to:

- one selected dataset
- one dataset root
- dataset-specific source resolution

Required changes:

### 1. Dataset-scoped discovery

Discovery should run for a single dataset config at a time.

### 2. Source resolution

For `source_mode = subdirs`:

- list immediate child directories under the dataset root
- treat each as one source/router

### 3. File parsing

Do not derive router/source identity from path depth arithmetic.

Instead:

- discovery already knows which source directory it is scanning
- filename parsing only needs to extract timestamp from `nfcapd.<YYYYMMDDHHmm>`

### 4. Gap detection

Gap detection remains per source within that dataset DB.

The existing logic can stay conceptually the same once scoped to the selected dataset.

## Prepared Dataset Import

Add a one-time preparation script for static external datasets:

- `netflow-db/prepare_nfcapd_dataset.py`

Its job is to normalize external `nfcapd` data from `/research/obo/raw_datasets` into the canonical layout under `/research/obo/netflow_datasets/<dataset>/...`.

It should support:

- scanning an input tree for `nfcapd` files
- unpacking raw immutable archives into a temporary work area when needed
- assigning a fixed or derived source ID
- validating files with `nfdump`
- copying, hardlinking, or symlinking already bucketed files
- emitting normalized 5-minute bucket files when input files span longer windows

## Handling Different External Shapes

### Case 1. Already in per-bucket `nfcapd.YYYYMMDDHHmm`

If the dataset already contains correctly bucketed files after extraction:

- prefer hardlink, symlink, or copy into canonical layout

### Case 2. Long-span binary files

If the dataset contains week-long or otherwise aggregated binary files:

- use `nfdump -r ... -t ... -w ...` in a loop to emit one 5-minute file per bucket

This preserves the original historical timestamps and produces the same kind of on-disk layout the current pipeline already expects.

The expected data flow is:

- immutable archive in `/research/obo/raw_datasets`
- extraction or staging in temporary working space
- normalized output in `/research/obo/netflow_datasets/<dataset>/...`

Reason:

- replay buckets by replay time rather than historical capture time

## Pipeline Changes

Update the pipeline to target one dataset at a time.

Inputs to the pipeline should become:

- selected dataset ID
- dataset root path
- dataset DB path

Task tuples can remain close to the current form:

- `(file_path, router, timestamp, file_exists)`

where `router` now means:

- source within the selected dataset

That keeps processor internals largely unchanged.

## Web App Changes

The frontend should shift from one global dashboard to dataset-specific dashboards.

Recommended changes:

### 1. Add dataset selection

Add a dataset index route such as:

- `/datasets`

and a dataset-specific dashboard route such as:

- `/datasets/[dataset]`

### 2. Keep source selection inside the dataset dashboard

Within `/datasets/[dataset]`, keep the existing router/source filter behavior.

### 3. Scope APIs by dataset

Existing APIs should become dataset-aware, for example:

- `/api/datasets`
- `/api/datasets/[dataset]/sources`
- `/api/datasets/[dataset]/netflow/stats`
- `/api/datasets/[dataset]/netflow/files/[slug]`

### 4. Resolve the correct DB per request

The web layer should open the SQLite database for the selected dataset rather than one global database.

## Implementation Plan

### 1. Add `./data` support

Create repo-local storage conventions for dataset databases:

- `./data/<dataset>/netflow.sqlite`

Add helpers to create directories as needed.

### 2. Add dataset registry support

Create a shared dataset configuration loader used by:

- the pipeline
- discovery
- the web app

### 3. Add dataset-aware DB resolution

Replace the single `DATABASE_PATH` usage with:

- `resolve_database_path(dataset_id)`

This should point into `./data/<dataset>/netflow.sqlite`.

### 4. Refactor discovery to operate on one dataset config

Discovery should:

- load one dataset entry
- enumerate its sources
- scan dated directories for `nfcapd.*`
- populate that dataset’s DB

### 5. Adapt the pipeline entry point

Update `pipeline.py` to accept a selected dataset, such as:

- `python pipeline.py --dataset uoregon`
- `python pipeline.py --dataset ugr16`

### 6. Keep existing table schemas where possible

Minimize schema churn by preserving current table shapes inside each per-dataset DB.

### 7. Build `prepare_nfcapd_dataset.py`

Implement one-time normalization support for static datasets.

First target:

- UGR16 style inputs

Initial source-of-truth location:

- `/research/obo/raw_datasets`

### 8. Update the web routes

Add dataset index and dataset-scoped dashboard/API routes.

### 9. Add operational docs

Document:

- how raw immutable dataset archives are stored under `/research/obo/raw_datasets`
- how to register a dataset
- how `./data` is organized
- how UOregon is discovered through the symlink
- how to prepare and ingest static external datasets

## Validation Plan

Validate at least these cases:

1. `./data/uoregon/netflow.sqlite` can be created and populated successfully.
2. UOregon discovery works correctly through `/research/obo/netflow_datasets/uoregon`.
3. Multiple UOregon sources coexist correctly inside the UOregon dashboard.
4. `./data/ugr16/netflow.sqlite` can be created from prepared UGR16 inputs.
5. The web app routes to separate dataset dashboards instead of one global combined view.
6. Dataset-scoped APIs always open the correct SQLite database.
7. File detail and chart routes continue to work inside each dataset dashboard.

## Risks And Tradeoffs

### 1. More DB files to manage

Per-dataset DBs increase the number of database files.

Mitigation:

- keep them under a single repo-local `./data` tree
- use one folder per dataset

### 2. Route churn in the web app

The app currently assumes one dashboard and one DB.

Mitigation:

- add dataset-scoped routes first
- migrate the existing root dashboard later if needed

### 3. Existing env assumptions

The code currently assumes one `DATABASE_PATH` and one `AVAILABLE_ROUTERS`.

Mitigation:

- move those concerns into the dataset registry
- keep compatibility shims temporarily if needed

### 4. One-time normalization cost for archival datasets

Splitting large binary files into 5-minute buckets can be expensive.

Mitigation:

- treat it as offline preparation
- support resumable manifests if needed

## Recommendation

Implement the port as:

- one dataset registry
- one dataset root under `/research/obo/netflow_datasets`
- one SQLite database per dataset under `./data/<dataset>/netflow.sqlite`
- one dashboard per dataset

This keeps the current time-series product intact, respects the fact that datasets should be viewed separately, and avoids a high-churn shared-schema redesign that is not needed right now.
