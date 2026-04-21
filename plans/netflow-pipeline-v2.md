# Netflow Pipeline V2

## Goal

Replace the file-format-specific pipeline with a v2 ingestion boundary that can consume either:

- `nfcapd` via `nfdump`
- external csv via explicit user-written mapping json

The v2 path should be clean, strict, and independent from the current web/API shape.

## Decisions

- Build v2 beside the current pipeline; do not mutate v1 in place
- No persisted raw/intermediate rows
- External csv is treated as unbucketed input
- `source_id` replaces transport-specific `router`
- Web compatibility is deferred until the v2 backend shape settles
- MAAD is mandatory in v2 and uses:
  - `--input -`
  - `--output -`
  - `--format json`

## Normalized Row Contract

Every adapter must yield the same logical row shape before aggregation:

- `source_id`
- `time_start`
- `time_end`
- `time_received`
- `src_ip`
- `dst_ip`
- `src_port`
- `dst_port`
- `protocol`
- `packets`
- `bytes`
- `src_tos`
- `dst_tos`

Derived fields:

- `ip_version`
- `bucket_start`
- `bucket_end`

Notes:

- `ip_version` is inferred from parsed IP addresses
- `bucket_start` is the chosen timestamp floored to 5 minutes
- `bucket_end = bucket_start + 300`

## Timestamp Precedence

For unbucketed csv input, bucket selection uses:

1. `time_received`
2. else `time_end`
3. else `time_start`

If none are present, validation fails.

## CSV Mapping Contract

External csv ingestion is strict.

- Users provide a json mapping file
- Missing required mapped columns fail validation
- Bad timestamp values fail ingestion
- Extra unmapped columns are ignored
- No best-effort guessing outside trusted presets

Initial required logical fields:

- one of `time_received`, `time_end`, `time_start`
- `src_ip`
- `dst_ip`

Strongly recommended:

- `protocol`
- `packets`
- `bytes`

Optional:

- `src_port`
- `dst_port`
- `src_tos`
- `dst_tos`
- row-level `source_id`

If no row-level `source_id` is mapped, config must provide a constant `source_id`.

## Dataset Semantics

`source_id` means “stable producer or stream identifier”.

Examples:

- `nfcapd`: router-like source such as `oh_ir1_gw`
- external csv: named source/feed supplied by config or row column

## Zero Fill

- `nfcapd`: zero-fill allowed because cadence is explicit
- external csv: zero-fill only when dataset config declares expected cadence

Missing csv buckets are otherwise treated as unknown, not zero traffic.

## V2 Tables

First slice:

- `processed_inputs_v2`
- `netflow_stats_v2`

- `ip_stats_v2`
- `protocol_stats_v2`
- `spectrum_stats_v2`
- `structure_stats_v2`
- `dimension_stats_v2`

`netflow_stats_v2` should include `ip_version` in its primary key shape instead of collapsing families together.

## First Slice

Implement in this order:

1. csv mapping json loader + validator
2. timestamp selection + 5-minute bucketing utilities
3. streaming adapter boundary for:
   - `nfcapd -> normalized rows`
   - `csv -> normalized rows`
4. `processed_inputs_v2`
5. `netflow_stats_v2`

Hold for later:

- web/API migration
- compatibility views

## Testing Approach

Use TDD for v2 work:

- write failing unit tests first
- implement the smallest passing slice
- keep tests focused on contracts, not incidental internals

Immediate test targets:

- csv mapping validation
- timestamp precedence
- 5-minute floor bucketing
- source-id resolution
- IPv4/IPv6 inference

## Risks

- letting old `router` naming leak into v2
- silently accepting malformed csv mappings
- assuming missing external csv buckets mean zero traffic
- mixing v1 schema compatibility concerns into v2 design
