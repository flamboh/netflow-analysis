# Web V2 Database Compatibility

## Goal

Make `apps/web` read pipeline v2 databases without blocking on the eventual v1 table swap.

This should be a stacked PR on top of the pipeline v2 PR. Keep it web-focused: no pipeline changes unless a web blocker exposes a real schema bug.

## Current Problem

The web API currently assumes v1 table names and column names:

- `router`
- `timestamp`
- `file_path`
- `netflow_stats`
- `ip_stats`
- `protocol_stats`
- `structure_stats`
- `spectrum_stats`

Pipeline v2 writes new tables and uses source/bucket language:

- `source_id`, not `router`
- `bucket_start`, not `timestamp`
- `processed_inputs_v2.input_locator`, not `netflow_stats.file_path`
- `*_stats_v2` tables
- `dimension_stats_v2` exists in addition to structure/spectrum

The frontend can still expose the word "router" for now, but server-side database access should treat that as a display/API alias for `source_id`.

## V2 Tables

### `processed_inputs_v2`

Tracks each logical input bucket.

Important columns:

- `input_kind`: `nfcapd` or `csv`
- `input_locator`: nfcapd path or csv path
- `source_id`
- `bucket_start`
- `bucket_end`
- `status`: `pending`, `processed`, or `failed`

Use this for bucket/file detail lookup. For nfcapd, `input_locator` is the capture file path. For csv, it is the source csv path, so file-detail UI should be treated as bucket details.

### `netflow_stats_v2`

5-minute flow counters, split by IP family.

Primary key:

- `source_id`
- `bucket_start`
- `ip_version`

Important columns:

- `bucket_end`
- `ip_version`: `4` or `6`
- `flows`, `flows_tcp`, `flows_udp`, `flows_icmp`, `flows_other`
- `packets`, protocol packet splits
- `bytes`, protocol byte splits

To match the current web aggregate chart, sum across `ip_version`.

### `ip_stats_v2`

Distinct source/destination IP counts by granularity.

Primary key:

- `source_id`
- `granularity`
- `bucket_start`

Granularities:

- `5m`
- `30m`
- `1h`
- `1d`

Important columns:

- `sa_ipv4_count`
- `da_ipv4_count`
- `sa_ipv6_count`
- `da_ipv6_count`

### `protocol_stats_v2`

Distinct protocols by granularity and IP family.

Primary key:

- `source_id`
- `granularity`
- `bucket_start`

Important columns:

- `unique_protocols_count_ipv4`
- `unique_protocols_count_ipv6`
- `protocols_list_ipv4`
- `protocols_list_ipv6`

Prefer using the count columns for existing charts. Keep protocol-list parsing out of this PR unless the UI needs it.

### MAAD Tables

Tables:

- `structure_stats_v2`
- `spectrum_stats_v2`
- `dimension_stats_v2`

Shared key:

- `source_id`
- `granularity`
- `bucket_start`
- `ip_version`

Current v2 writes MAAD for IPv4 only. Query with `ip_version = 4`.

Important columns:

- structure: `structure_json_sa`, `structure_json_da`
- spectrum: `spectrum_json_sa`, `spectrum_json_da`
- dimensions: `dimensions_json_sa`, `dimensions_json_da`
- metadata: `metadata_json_sa`, `metadata_json_da`

The existing UI only renders structure and spectrum. Dimension data can be left unread unless this PR explicitly adds a new chart/API.

## Implementation Shape

Add a small server-side compatibility module, for example:

`apps/web/src/lib/server/netflow-schema.ts`

Responsibilities:

- Detect whether a dataset DB has v2 tables.
- Provide canonical table/column names for routes.
- Keep route SQL from scattering v1/v2 conditionals everywhere.

Suggested detection:

```sql
SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'netflow_stats_v2'
```

If v2 exists, use v2. If not, keep v1 behavior for now. This is compatibility for development and comparison, not a long-term fallback strategy.

## Route Work

### Aggregate APIs

Port these first:

- `/api/netflow/stats`
- `/api/ip/stats`
- `/api/protocol/stats`
- `/api/netflow/structure-stats`
- `/api/netflow/spectrum-stats`

V2 query rules:

- filter `source_id IN (...)`
- filter `bucket_start >= ? AND bucket_start < ?`
- alias `source_id AS router` in responses to preserve frontend contracts
- use `granularity = ?` for v2 aggregate tables
- for netflow stats, sum `netflow_stats_v2` rows across `ip_version`

For `/api/netflow/stats`, do not re-bucket 5-minute rows with SQLite localtime if a matching v2 granularity table exists elsewhere. `netflow_stats_v2` currently only has 5-minute rows, so if the API still supports `date/hour/30min`, group `bucket_start` directly. Keep this minimal and covered by tests.

### Bucket Detail APIs

Port these after aggregate APIs:

- `/api/netflow/files/[slug]/details`
- `/api/netflow/files/[slug]/ip-counts`
- `/api/netflow/files/[slug]/structure`
- `/api/netflow/files/[slug]/spectrum`

Rename internal concepts toward bucket details. Public routes can stay as-is for now.

V2 lookup rules:

- slug maps to `bucket_start`
- router query param maps to `source_id`
- use `processed_inputs_v2` for `input_locator`
- join stats by `source_id`, `bucket_start`, and `granularity = '5m'`
- query MAAD with `ip_version = 4`

V1 file-only fields may not exist in v2:

- `first_timestamp`
- `last_timestamp`
- `msec_first`
- `msec_last`
- `sequence_failures`

For v2 bucket details, return sane null/zero values or update the type to make them optional. Do not fake precision the v2 database does not store.

### Singularities

Treat the singularities endpoint as deprecated. Do not port it to v2.

Web behavior should be one of:

- hide singularities UI for v2 datasets
- return a clear `410 Gone` or disabled response from the endpoint

## Testing

Use small test fixtures, not local uoregon data.

Add route tests with an in-memory or temp SQLite v2 database containing:

- two `source_id` values
- one or two 5-minute buckets
- IPv4 and IPv6 `netflow_stats_v2` rows for the same bucket
- one `ip_stats_v2` row for `5m` and one aggregate row like `1h`
- one `protocol_stats_v2` row
- one structure row and one spectrum row with tiny JSON arrays
- one `processed_inputs_v2` row with an nfcapd-like `input_locator`

Assertions:

- existing API response fields still use `router` where the client expects it
- aggregate netflow sums IPv4 and IPv6 rows
- `source_id` filtering works through the existing `routers` query param
- structure/spectrum parse JSON from v2 columns
- bucket detail routes work without v1 file metadata
- singularities are hidden or explicitly disabled for v2

## Non-Goals

- Do not rewrite the dashboard UI.
- Do not add local-only data tests.
- Do not port singularities.
- Do not add compatibility SQL views unless route-level compatibility gets too noisy.
- Do not make v1/v2 fallback elaborate; this is greenfield and v2 is the target.

## Suggested PR Boundary

One stacked PR should be enough:

1. Add schema adapter.
2. Port aggregate APIs.
3. Port bucket detail APIs.
4. Disable singularities for v2.
5. Add focused route tests.

If this gets too large, split after aggregate APIs. Aggregate charts provide the fastest signal that v2 databases are web-readable.
