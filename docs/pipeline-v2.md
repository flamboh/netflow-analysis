# Pipeline V2

Pipeline v2 is the clean-path ingest flow for:

- external csv inputs
- `nfcapd` inputs via `nfdump`

This path is independent from the legacy pipeline tables. MAAD is part of the
v2 ingest contract, not an optional side path.

## Current Scope

Pipeline v2 currently writes:

- `processed_inputs_v2`
- `netflow_stats_v2`
- `ip_stats_v2`
- `protocol_stats_v2`
- `structure_stats_v2`
- `spectrum_stats_v2`
- `dimension_stats_v2`

MAAD is always part of pipeline v2.

`processed_inputs_v2` tracks processing at the input-bucket level. A complete
bucket payload is written transactionally, so the ledger has one `status` value
per `(input_kind, input_locator, source_id, bucket_start)` instead of per-table
status columns.

## Input Config

For canonical on-disk nfcapd datasets, run v2 directly from `datasets.json`:

```bash
python tools/netflow-db/pipeline_v2.py --dataset uoregon --start-date 2025-02-01
```

This assumes the standard nfcapd layout:

```text
<dataset-root>/<source_id>/YYYY/MM/DD/nfcapd.YYYYMMDDHHMM
```

`--end-date` is optional and defaults to the latest discovered nfcapd day. The
pipeline processes this tree one calendar day at a time into
`./data/<dataset>-v2/netflow.sqlite`, keeping memory bounded. Reruns skip days
where every discovered nfcapd file is already marked `processed`; partial days
are rebuilt as a full day so aggregate rows stay coherent.

Explicit json configs are still supported for csv inputs and unusual file
layouts:

```json
{
  "database_path": "./data/uoregon/netflow-v2.sqlite",
  "maad_bin": "./vendor/maad/MAAD",
  "maad_backend": "subprocess",
  "maad_workers": 1,
  "run_maad": true,
  "inputs": [
    {
      "input_kind": "csv",
      "path": "/data/external/flows.csv",
      "mapping_path": "/data/external/flows.mapping.json"
    },
    {
      "input_kind": "nfcapd",
      "path": "/research/obo/netflow_datasets/uoregon/oh_ir1_gw/2025/04/15/nfcapd.202504150005",
      "source_id": "oh_ir1_gw"
    },
    {
      "input_kind": "csv_tree",
      "root_path": "/research/obo/raw_datasets/ugr_csv",
      "mapping_path": "/path/to/ugr16-csv.mapping.json"
    },
    {
      "input_kind": "nfcapd_tree",
      "root_path": "/research/obo/netflow_datasets/uoregon",
      "source_ids": ["cc_ir1_gw", "oh_ir1_gw"],
      "start_date": "2025-02-01",
      "end_date": "2025-02-07"
    }
  ]
}
```

Run it with:

```bash
python tools/netflow-db/pipeline_v2.py --config /path/to/pipeline-v2.json
```

UGR'16 CSV configs are local because deployments use different data roots. The
operator helpers live in `scripts/local/`. Create
`scripts/local/ugr16-csv.pipeline-v2.json`, or pass another config path. For the
full CSV corpus, build to a candidate path first, then verify and promote only
after all discovered input buckets are processed:

```bash
scripts/local/build_ugr16_netflow_v2.sh \
  --detach \
  --config scripts/local/ugr16-csv.pipeline-v2.json \
  --database data/ugr16/netflow.detached.build.sqlite \
  --log data/ugr16/netflow.detached.build.log \
  --pid data/ugr16/netflow.detached.build.pid

scripts/local/finalize_ugr16_netflow_v2.sh \
  --candidate data/ugr16/netflow.detached.build.sqlite \
  --target data/ugr16/netflow.sqlite \
  --promote
```

`datasets.json` should point the `ugr16` entry at `./data/ugr16/netflow.sqlite`
with `source_ids: ["ugr16"]`. The finalizer runs
`verify_web_compatible_v2.py --require-data --require-maad-data
--require-processed --require-no-raw-ip` before replacing the target database,
then verifies the promoted dataset through the running web app's dataset,
aggregate stats, MAAD stats, and file-detail API routes. Pass `--web-base-url`
if the web app is not on the default local Vite ports.

## CSV Mapping JSON

External csv inputs require a user-written mapping json.

Example:

```json
{
  "timestamp_format": "unix",
  "datetime_format": "%Y-%m-%d %H:%M:%S",
  "delimiter": ",",
  "has_header": true,
  "input_order": "timestamp_ascending",
  "out_of_order_lag_buckets": 12,
  "columns": {
    "time_received": "tr",
    "time_end": "te",
    "time_start": "ts",
    "src_ip": "sa",
    "dst_ip": "da",
    "src_port": "sp",
    "dst_port": "dp",
    "protocol": "pr",
    "packets": "pkt",
    "bytes": "byt",
    "src_tos": "stos",
    "dst_tos": "dtos"
  },
  "source_id": {
    "value": "uo-feed"
  }
}
```

`source_id` may be supplied either as:

- a constant value
- a row column name

Example column-based source id:

```json
{
  "source_id": {
    "column": "router_name"
  }
}
```

## Required CSV Fields

Required logical fields:

- one of `time_received`, `time_end`, or `time_start`
- `src_ip`
- `dst_ip`
- `source_id` via constant or mapped column

Optional fields:

- `src_port`
- `dst_port`
- `protocol`
- `packets`
- `bytes`
- `src_tos`
- `dst_tos`

Missing optional numeric fields default to `0`.

Headered CSV files are validated against the mapping before row iteration.
Headerless CSV files must provide `fieldnames` in exact input order.

`input_order: "timestamp_ascending"` enables bounded-memory streaming. Rows may
arrive within `out_of_order_lag_buckets` 5-minute buckets, but rows older than
the flushed cutoff fail ingestion. Use `input_order: "unsorted"` only for small
files that can be aggregated in memory as a full input.

## Timestamp Precedence

Bucket assignment uses this precedence:

1. `time_received`
2. else `time_end`
3. else `time_start`

The chosen timestamp is floored to the start of the 5-minute bucket.

Pipeline v2 uses `NETFLOW_TIMEZONE` for local-time filename and aggregate
bucket boundaries. The default is `America/Los_Angeles`, matching the current
uoregon dataset convention.

## UGR'16 CSV Config

UGR'16 uses the generic `csv` adapter. The dataset-specific behavior is fully
declared in mapping JSON:

```json
{
  "timestamp_format": "datetime",
  "datetime_format": "%Y-%m-%d %H:%M:%S",
  "timestamp_timezone": "Europe/Madrid",
  "input_order": "timestamp_ascending",
  "out_of_order_lag_buckets": 0,
  "has_header": false,
  "fieldnames": [
    "time_end",
    "duration",
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "flags",
    "forwarding_status",
    "src_tos",
    "packets",
    "bytes",
    "label"
  ],
  "skip_bad_column_count": true,
  "archive": {
    "member_contains": "csv"
  },
  "discovery": {
    "include_contains": [],
    "include_suffixes": [".tar.gz", ".tgz"],
    "exclude_suffixes": [".aria2", ".txt"]
  },
  "columns": {
    "time_end": "time_end",
    "src_ip": "src_ip",
    "dst_ip": "dst_ip",
    "src_port": "src_port",
    "dst_port": "dst_port",
    "protocol": "protocol",
    "packets": "packets",
    "bytes": "bytes",
    "src_tos": "src_tos"
  },
  "source_id": {
    "value": "ugr16"
  }
}
```

Then process a single file/archive with `input_kind: "csv"` or a flat download
directory with `input_kind: "csv_tree"`. The generic CSV reader accepts extracted
CSV files and tar archives such as `*_csv.tar.gz`.

## NFDUMP Contract

The current `nfcapd` adapter calls `nfdump` with a fixed csv order:

```bash
nfdump -r <file> -q -o 'csv:%trr,%ter,%tsr,%sa,%da,%sp,%dp,%pr,%pkt,%byt,%stos,%dtos' ipv4
nfdump -r <file> -q -o 'csv:%trr,%ter,%tsr,%sa,%da,%sp,%dp,%pr,%pkt,%byt,%stos,%dtos' ipv6 -6
```

The v2 adapter treats these fields as:

- `time_received = %trr`
- `time_end = %ter`
- `time_start = %tsr`

The raw epoch forms are required because `%tr`, `%te`, and `%ts` emit formatted
timestamps in nfdump CSV output.

nfdump can emit ICMP type/code values such as `3.1` in port fields. Pipeline v2
normalizes decimal pseudo-ports to `0` because they are not transport ports.

## MAAD Contract

Pipeline v2 runs MAAD once for source IPv4 addresses and once for destination
IPv4 addresses for each source/bucket. Ordered CSV ingest keeps the raw IPv4
sets in memory only until each bucket is closed, writes the derived
structure/spectrum/dimension rows, then discards the sets. It does not persist
raw MAAD input IP sets to SQLite or sidecar files.

`maad_backend` can be `"subprocess"` for a compiled MAAD-compatible binary or
`"python"` for the in-process implementation. UGR'16 uses the native
`tools/netflow-db/maad_fast` helper to avoid the compiled Haskell MAAD runtime
cost while preserving the same derived output contract.
`maad_workers` controls bounded in-memory MAAD parallelism for closed buckets.

Build the native helper with:

```bash
g++ -O3 -std=c++17 -o tools/netflow-db/maad_fast tools/netflow-db/maad_fast.cpp
```

Set `"run_maad": false` only for diagnostic builds that intentionally omit
structure/spectrum/dimension rows.

Current MAAD command shape:

```bash
MAAD --input - --output - --format json --structure --spectrum --dimensions
```

Expected JSON shape:

```json
{
  "schemaVersion": 1,
  "metadata": {
    "input": "-",
    "minPrefixLength": 7,
    "maxPrefixLength": 23,
    "totalAddrs": 84651
  },
  "structure": [{ "q": -0.5, "tauTilde": -0.986, "sd": 0.007 }],
  "spectrum": [{ "alpha": 0.754, "f": 0.602 }],
  "dimensions": [{ "q": 1, "dim": 0.481 }]
}
```

Pipeline v2 stores source and destination outputs in the same row using `*_json_sa`
and `*_json_da` columns. MAAD currently runs for IPv4 address buckets.

## Notes

- Pipeline v2 does not persist raw/intermediate rows
- Pipeline v2 has first-class support for canonical nfcapd directory trees
- MAAD is mandatory and assumes the JSON stdout contract shown above
