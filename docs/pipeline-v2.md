# Pipeline V2

Pipeline v2 is the clean-path ingest flow for:

- external csv inputs
- `nfcapd` inputs via `nfdump`

This path is independent from the legacy pipeline tables and is intended to
settle the non-MAAD contracts before MAAD integration lands.

## Current Scope

Pipeline v2 currently writes:

- `processed_inputs_v2`
- `netflow_stats_v2`
- `ip_stats_v2`
- `protocol_stats_v2`

It does not yet run MAAD.

## Input Config

`pipeline_v2.py` takes an explicit json config:

```json
{
  "database_path": "./data/uoregon/netflow-v2.sqlite",
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

## NFDUMP Contract

The current `nfcapd` adapter calls `nfdump` with a fixed csv order:

```bash
nfdump -r <file> -q -o 'csv:%tr,%te,%ts,%sa,%da,%sp,%dp,%pr,%pkt,%byt,%stos,%dtos' ipv4
nfdump -r <file> -q -o 'csv:%tr,%te,%ts,%sa,%da,%sp,%dp,%pr,%pkt,%byt,%stos,%dtos' ipv6 -6
```

The v2 adapter treats these fields as:

- `time_received = %tr`
- `time_end = %te`
- `time_start = %ts`

## Notes

- Pipeline v2 does not persist raw/intermediate rows
- Pipeline v2 currently works on explicit inputs; it does not yet replace the
  legacy discovery flow
- MAAD integration is intentionally deferred until the upstream json/stdout
  contract is available
