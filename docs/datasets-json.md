# `datasets.json` setup

`datasets.json` defines local pipeline inputs: roots, source discovery, and
SQLite output paths. The web app does not read this file directly; it reads the
`datasets` table inside SQLite/D1.

The file is not meant to be committed with machine-specific paths. Start from
`datasets.json.example` and create your own local `datasets.json` in the repo root.

## Basic flow

1. Copy `datasets.json.example` to `datasets.json`
2. Update the dataset paths for your machine
3. Make sure each dataset has a unique `dataset_id`
4. Run the pipeline; new v2 SQLite databases are seeded with a `datasets` row
5. Set `DEFAULT_DATASET` in `.env` if you want one dataset to be used by default

## Example

```json
[
  {
    "dataset_id": "uoregon",
    "label": "UONet-in",
    "root_path": "/path/to/netflow_datasets/uoregon",
    "db_path": "./data/uoregon/netflow.sqlite",
    "default_start_date": "2025-02-11",
    "source_mode": "subdirs",
    "discovery_mode": "live"
  }
]
```

## Fields

- `dataset_id`: stable slug used in routes and the pipeline `--dataset` flag
- `label`: human-readable display name for the web app
- `root_path`: root directory containing the dataset's prepared NetFlow files
- `db_path`: SQLite output path for that dataset
- `default_start_date`: default dashboard start date in `YYYY-MM-DD`
- `source_mode`: currently `subdirs`, meaning each top-level subdirectory is treated as a source/router
- `discovery_mode`: descriptive only for now; use `live` for synced data and `static` for imported datasets

## Notes

- `db_path` can be relative to the repo root
- `root_path` is usually absolute
- if you want to store the config somewhere else, point `DATASETS_CONFIG_PATH` in `.env` at that file
- local web discovery scans `data/*/netflow.sqlite` and reads each database's
  `datasets` table
