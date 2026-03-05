# Reprocessing Window Plan

## Goal

Align the pipeline with this operating model:

- In normal operation, automatically check for missing data only within a configurable recent window.
- Preserve enough historical state to reconstruct what days were complete, incomplete, or discovered late.
- Allow operators to widen the window manually when upstream upload delays exceed the default horizon.
- Ensure aggregate tables are always recomputed consistently when any file for a day is reprocessed.

## Desired Behavior

### Normal mode

- The pipeline uses a default reprocessing window of 30 days.
- Missing-file discovery and automatic stale-day reopening are limited to that window.
- Synthetic gap placeholders older than the active window are preserved for historical visibility, but are not automatically processed.
- Files older than the active window are left untouched in that run.

### Recovery mode

- Operators can widen the active reprocessing horizon with a CLI flag.
- Example uses:
  - default cron behavior: `python pipeline.py`
  - delayed upload recovery: `python pipeline.py --reprocess-window-days 90`
  - full historical reconciliation: `python pipeline.py --reprocess-window-days 0`
- A value of `0` means no window limit.
- Widening the window is what allows older stale aggregate days to be reopened and rebuilt.
- A practical operating model is weekly 30-day runs plus a monthly wider reconciliation run, such as 180 days.

### Processing model

- `flow_stats` can be safely reprocessed at the file level.
- `ip_stats`, `protocol_stats`, `spectrum_stats`, and `structure_stats` must be reprocessed at the day level.
- If any file in a `(router, day)` changes state in an aggregate table, all aggregates for that day must be rebuilt from the full day.

## Why This Change Is Needed

The current code limits stale-day reset to the last 30 days, but older synthetic gap rows can still remain pending. That creates two problems:

1. The system still checks older historical missing data, which conflicts with the intended default behavior.
2. Aggregate tables can become inconsistent if an old file is processed without reopening the full day.

For aggregate tables, the correct unit of recomputation is the entire day:

- a changed 5-minute file affects its `5m` bucket
- that also affects the containing `30m` bucket
- that affects the containing `1h` bucket
- that affects the `1d` bucket for the day

Therefore, for aggregate tables, reprocessing any file in a day means reopening and recomputing the full day.

## Implementation Plan

### 1. Add a configurable window to the CLI

Update `netflow-db/pipeline.py` to accept:

- `--reprocess-window-days N`

Behavior:

- default: `30`
- `0`: unlimited historical window

This value should be threaded into discovery and all processors.

### 2. Centralize cutoff calculation

Add a shared helper in `netflow-db/discovery.py` to calculate the active cutoff timestamp.

Requirements:

- one implementation of window logic
- used by both gap handling and stale-day reset logic
- supports unlimited mode when the CLI passes `0`

This avoids hardcoding `30` in multiple places.

### 3. Preserve historical placeholder state

Do not delete historical `file_exists = 0` rows from `processed_files`.

Reason:

- those rows are the durable record of expected-but-missing data
- they allow reconstruction of whether a day was complete or incomplete at discovery time
- they support later manual recovery if uploads arrive after the default horizon
- they support scheduled wider-window reconciliation runs

### 4. Restrict automatic gap processing to the active window

Update `get_files_needing_processing()` in `netflow-db/discovery.py` so that:

- files are only automatically eligible when within the active window
- synthetic gap placeholders older than the active window remain stored, but inert
- real files older than the active window are deferred until a wider-window run

This enforces the intended default behavior without losing historical state.

### 5. Apply the same window to stale-day reopening

Update `handle_stale_days()` in `netflow-db/discovery.py` to use the configurable window instead of a fixed 30-day cutoff.

Behavior:

- normal mode reopens stale days only inside the default window
- manual recovery mode can reopen older days by widening the window
- unlimited mode can fully reconcile all history

### 6. Make aggregate tables explicitly day-level

For `ip_stats`, `protocol_stats`, `spectrum_stats`, and `structure_stats`, enforce this rule:

- if any file in a `(router, day)` needs reprocessing, the processor must reopen and recompute the full day for that table before writing aggregates

This should be treated as a core invariant, not an incidental side effect.

Implication:

- the day worker must operate on a complete reopened day, not just the currently pending subset, whenever aggregates are being rebuilt

### 7. Keep `flow_stats` file-level

`flow_stats` has no higher-level day aggregates in the current model, so file-level processing remains acceptable.

No day-level reopening requirement is needed for `flow_stats` unless that table later gains aggregate derivations.

### 8. Verify recovery workflows

After implementation, validate these cases:

1. A missing synthetic gap within the last 30 days is detected and processed automatically.
2. A synthetic gap older than 30 days remains recorded but is not automatically processed.
3. A late real file older than 30 days is deferred until a wider-window run.
4. Widening the window reopens affected aggregate-table days and rebuilds them fully.
5. Unlimited mode can reconcile all stale historical days.

## Policy Decision Captured Here

The system should distinguish between:

- automatic recent-data maintenance
- manual historical recovery

Default automation should stay bounded. Historical recovery should be explicit and operator-driven.

## Summary

The target design is:

- keep the default 30-day operational window
- preserve historical missing-data state in the database
- expose a CLI override for delayed-upload recovery
- use the selected window as the single control knob for automatic processing in that run
- treat aggregate-table reprocessing as a day-level operation whenever any file in that day changes
