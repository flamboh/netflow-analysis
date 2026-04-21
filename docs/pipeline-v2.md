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

`pipeline_v2.py` takes an explicit json config:

```json
{
  "database_path": "./data/uoregon/netflow-v2.sqlite",
  "maad_bin": "./vendor/maad/MAAD",
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
    }
  ]
}
```

Run it with:

```bash
python tools/netflow-db/pipeline_v2.py --config /path/to/pipeline-v2.json
```

## CSV Mapping JSON

External csv inputs require a user-written mapping json.

Example:

```json
{
  "timestamp_format": "unix",
  "delimiter": ",",
  "has_header": true,
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

## Timestamp Precedence

Bucket assignment uses this precedence:

1. `time_received`
2. else `time_end`
3. else `time_start`

The chosen timestamp is floored to the start of the 5-minute bucket.

Pipeline v2 uses `NETFLOW_TIMEZONE` for local-time filename and aggregate
bucket boundaries. The default is `America/Los_Angeles`, matching the current
uoregon dataset convention.

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
IPv4 addresses for each source/bucket.

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
- Pipeline v2 currently works on explicit inputs; it does not yet replace the
  legacy discovery flow
- MAAD is mandatory and assumes the JSON stdout contract shown above
- Real uoregon correctness checks live in `tests/local_only/uoregon_v2_validation.py`.
  They are not part of default pytest discovery and require `RUN_LOCAL_NETFLOW_V2=1`.
