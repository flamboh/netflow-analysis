# Pipeline usage

The pipeline lives in `netflow-db/pipeline.py`.

It does two things:

- discovers files and recent gaps
- processes pending data into the stats tables

## Normal run

The repo-level `.env` is the single source of truth.

The pipeline resolves that file relative to the script location, so you can run it either from the repo root or from `netflow-db/`.

```bash
python netflow-db/pipeline.py
```

or

```bash
cd netflow-db
python pipeline.py
```

That uses the default reprocessing window of 30 days.

In practice, that means:

- recent missing data can still be discovered and filled in
- recent stale days can be reopened and rebuilt
- older history stays recorded in `processed_files`, but is not automatically revisited in that run

## Common flags

Only do discovery:

```bash
python netflow-db/pipeline.py --discover-only
```

Only do processing:

```bash
python netflow-db/pipeline.py --process-only
```

Only run specific tables:

```bash
python netflow-db/pipeline.py --tables flow_stats,ip_stats
```

Limit how much work is done per table:

```bash
python netflow-db/pipeline.py --limit 500
```

Disable log-file output:

```bash
python netflow-db/pipeline.py --no-log
```

## Delayed uploads

If data showed up late and you need to revisit older days, widen the reprocessing window.

Example: look back 90 days

```bash
python netflow-db/pipeline.py --reprocess-window-days 90
```

Example: full historical reconciliation

```bash
python netflow-db/pipeline.py --reprocess-window-days 0
```

`0` means unlimited.

The selected window is the control knob for that run:

- files outside the window are left alone
- stale days inside the window can be reopened and rebuilt

A practical pattern is:

- weekly: run a 14-day window
- monthly: run a wider window like 90 days

That gives you regular recent updates plus a periodic deeper reconciliation pass.

## Important detail about aggregates

`flow_stats` can be updated file by file.

The aggregate-heavy tables work at the day level:

- `ip_stats`
- `protocol_stats`
- `spectrum_stats`
- `structure_stats`

If any file in a day needs to be revisited for one of those tables, the pipeline reopens and rebuilds that whole day for that table.

So if you know delayed uploads affected older history, use a larger `--reprocess-window-days` value when you rerun the pipeline.

## Sanity check

If you edited backend Python, run:

```bash
cd netflow-db
python -m py_compile *.py
```
