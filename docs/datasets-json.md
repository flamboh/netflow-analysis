# `datasets.json` setup

`datasets.json` defines which NetFlow datasets the pipeline and web app know about.

The file is not meant to be committed with machine-specific paths. Start from
`datasets.json.example` and create your own local `datasets.json` in the repo root.

## Basic flow

1. Copy `datasets.json.example` to `datasets.json`
2. Update the dataset paths for your machine
3. Make sure each dataset has a unique `dataset_id`
4. Set `DEFAULT_DATASET` in `.env` if you want one dataset to be used by default

## Example

```json
[
  {
    "dataset_id": "uoregon",
    "label": "UONet-in",
    "root_path": "/research/obo/netflow_datasets/uoregon",
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
