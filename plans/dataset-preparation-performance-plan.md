# Dataset Preparation Performance Plan

## Goal

Build a dataset preparation flow that is flexible enough to normalize multiple external `nfcapd`-compatible datasets into the canonical 5-minute layout under `/research/obo/netflow_datasets/<dataset>/<source>/YYYY/MM/DD/nfcapd.YYYYMMDDHHmm`.

The design should not depend on UGR16-specific assumptions such as "one archive contains one week file." Instead, it should work from the time ranges present in the input files.

## Core Idea

Preparation should be driven by a time-range inventory, not by archive naming conventions.

For each candidate input file:

1. extract or locate the file
2. determine its `first` and `last` timestamps with `nfdump -I`
3. record that span in a manifest
4. plan canonical 5-minute buckets from the ordered set of spans
5. materialize each bucket from the best source file for that time range

This supports:

- already-bucketed `nfcapd.YYYYMMDDHHmm` files
- day files
- week files
- mixed archive drops
- future datasets with irregular chunking

## Why This Replaces Dataset-Specific Split Logic

The current long-span approach assumes:

- find one big file
- loop over every 5-minute bucket
- run `nfdump -r <big-file> -t <bucket-range> -w <bucket-output>`

That is correct, but expensive, and it does not generalize well.

The better model is:

- build a map of all available time spans
- process buckets in chronological order
- for each bucket, consult only the files whose spans overlap that bucket

That gives us one generic planner that can work across datasets instead of one splitter per archive shape.

## Proposed Phases

### 1. Inventory

Scan raw archives and extracted files to build a manifest of candidate inputs.

For each file, record:

- `file_id`
- `dataset_id`
- `source_id`
- `archive_path`
- `archive_member`
- `staging_path`
- `first_ts`
- `last_ts`
- `size_bytes`
- `kind`
- `status`

`kind` should distinguish at least:

- `canonical_bucket`
- `long_span`

The timestamp metadata should come from `nfdump -I`, not from file names alone.

### 2. Planning

Sort the inventory by `first_ts` and build an ordered segment index.

The planner should:

- detect overlaps
- detect gaps
- detect exact duplicates
- build the set of canonical 5-minute buckets to emit
- assign each bucket to one or more candidate source files

This produces a durable worklist rather than recomputing decisions on every run.

### 3. Materialization

For each planned 5-minute bucket:

- if the source file is already a canonical bucket, place or link it directly
- if the source file spans a larger range, slice just that bucket from the selected source file
- write the result to the canonical output path
- record success or failure in the manifest

## Efficient Bucket Planning

The planner should not rescan all source files for every bucket.

Instead:

- sort files once by `first_ts`
- iterate buckets in chronological order
- maintain a moving window of files whose `[first_ts, last_ts]` overlaps the current bucket

This keeps the planning cost closer to:

- number of files
- plus number of buckets

rather than:

- number of files times number of buckets

## Selection Rules

When multiple files cover the same bucket, the planner needs explicit resolution rules.

First-pass policy:

1. prefer an exact canonical bucket file over any sliced output
2. otherwise prefer the smallest file that fully covers the bucket
3. if multiple candidates remain, pick the earliest discovered file and log the ambiguity

This keeps behavior deterministic and debuggable.

## Performance Optimizations

### 1. Treat exact bucket files as a fast path

If a file is already one canonical bucket:

- do not rewrite it
- place, hardlink, or symlink it into the canonical tree

### 2. Make staged splitting an optimization, not the whole design

For very large files, add optional staged splitting:

- `week -> day`
- `day -> 5m`

But keep this as an executor optimization for `long_span` files, not as the top-level planning model.

### 3. Cache extraction and metadata

Avoid re-extracting archive members and rerunning `nfdump -I` on retries.

If the staging file still exists and matches the manifest metadata, reuse it.

### 4. Parallelize at coarse boundaries

Parallel work should happen at a bounded level such as:

- day shards
- or groups of contiguous buckets

Avoid unconstrained per-bucket parallelism against the same large source file.

## Manifest Design

Store manifests under `./data/<dataset>/manifests/`.

Two manifest layers are useful.

### File inventory manifest

Tracks discovered source files and metadata.

Suggested fields:

- `file_id`
- `dataset_id`
- `source_id`
- `archive_path`
- `archive_member`
- `staging_path`
- `first_ts`
- `last_ts`
- `size_bytes`
- `kind`
- `checksum` or `mtime`
- `inventory_status`

### Bucket work manifest

Tracks planned outputs and execution state.

Suggested fields:

- `bucket_start`
- `bucket_end`
- `dataset_id`
- `source_id`
- `output_path`
- `candidate_file_ids`
- `selected_file_id`
- `action`
- `status`
- `error`

`action` should distinguish at least:

- `place`
- `link`
- `slice`
- `skip`

## Script Changes

Refactor `netflow-db/prepare_nfcapd_dataset.py` into explicit modes.

Suggested first-pass interface:

- `--dataset <id>`
- `--source <id>`
- `--archive <path>` or `--input-root <path>`
- `--mode inventory|plan|materialize|all`
- `--from YYYY-MM-DD`
- `--to YYYY-MM-DD`
- `--workers N`
- `--staging-dir <path>`
- `--materialization-mode copy|hardlink|symlink`

Behavior:

- `inventory`: discover files and record spans
- `plan`: build the bucket worklist
- `materialize`: execute an existing plan
- `all`: run the whole flow in order

## Validation Plan

### Functional validation

1. Inventory one external dataset archive and confirm that every extracted file gets a valid time span.
2. Build a bucket plan for a bounded window.
3. Materialize the planned outputs into canonical paths.
4. Confirm emitted bucket files are readable with `nfdump -I -r <bucket-file>`.
5. Run the existing pipeline against that prepared subset.

### Performance validation

Compare:

- current direct bucket slicing
- planned bucket materialization using the indexed manifest
- optional staged splitting for large files

Measure:

- wall clock time
- total `nfdump` invocations
- buckets written per minute
- resumed work skipped on rerun

## Initial Proof Target

Do not start with a full dataset.

Use a bounded sample such as:

- one UGR archive from `/research/obo/raw_datasets`
- one selected day from that archive

Success criteria:

- manifest built correctly
- canonical bucket files emitted correctly
- pipeline runs against the prepared subset
- the same design remains suitable for other external `nfcapd` datasets

## Non-Goals

- changing the canonical bucket layout
- changing the downstream pipeline to consume anything other than bucketed files
- making one-off per-dataset splitter scripts the primary architecture
- processing the entire UGR corpus before the manifest-driven flow is proven

## Recommendation

Build the next version of dataset preparation around:

- inventorying file time spans with `nfdump -I`
- planning canonical buckets from an ordered span index
- materializing buckets from the selected source files
- using staged splitting only as an optimization for large files

That gives us a general preparation system that fits UGR16 well and remains flexible for future `nfcapd` datasets.
