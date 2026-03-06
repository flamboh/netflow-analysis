# Discovery Window Optimization Plan

## Goal

Reduce filesystem discovery cost by scanning only a bounded date window instead of walking the full historical tree on every run.

The file layout under `NETFLOW_DATA_PATH` is assumed to be stable and predictable:

- `router/YYYY/MM/DD/nfcapd.YYYYMMDDHHMM`

That means discovery can jump directly to expected day directories instead of recursively traversing all years, months, and days.

## Desired Outcome

- Normal runs only inspect recent day directories.
- Wider reconciliation runs can inspect older day directories when needed.
- Full historical scan remains available as an explicit override.
- Processing semantics stay the same; this is primarily a discovery optimization.

## Proposed Model

Introduce a discovery window that is distinct from the reprocessing window.

- `reprocess_window_days` controls which discovered data the pipeline is willing to act on during that run.
- `discovery_window_days` controls how far back the filesystem scan looks for real files.

These two windows can be set independently, but the default behavior should be simple and predictable.

## Time Semantics

Both windows should use the same day-based definition:

- compute from local midnight, not from the current wall-clock timestamp
- clamp to `DATA_START_DATE`
- treat `0` as unlimited

This avoids off-by-one behavior at day boundaries and makes the system easier to reason about operationally.

## Edge Cases To Account For

### 1. Boundary-day mismatches

Do not mix:

- "last N calendar days from local midnight"
- with "last N x 24 hours from right now"

The implementation should use calendar-day windows consistently for both discovery and reprocessing.

### 2. Discovery may need a slightly wider horizon than processing

Using the same default value for both windows is reasonable, but discovery and reprocessing are not identical concepts.

Example:

- a late file arrives for 15 days ago
- weekly run uses `14`
- if discovery also uses `14`, that file will not be discovered during that run

That may be acceptable operationally, but it should be an explicit consequence of the chosen window, not an accidental artifact.

The plan should therefore:

- default `discovery_window_days` to `reprocess_window_days`
- keep it configurable as a separate flag
- allow a future cushion if needed, such as discovery being slightly wider than reprocessing

### 3. Complete-day behavior around the edge of the window

A newly discovered file can still belong to a day that is not yet considered complete.

That is expected.

Discovery and processing should remain decoupled enough that:

- a file can be discovered in one run
- but only processed once its day is complete and within the active reprocessing window

### 4. Missed wider reconciliation runs

If the operational model relies on:

- frequent narrow runs
- occasional wider runs

then skipping a wider run means older delayed files will stay undiscovered longer.

This is acceptable if intentional, but it should be documented as part of the operating model.

## CLI Changes

Add a new pipeline flag:

- `--discovery-window-days N`

Behavior:

- default: if unspecified, use the same value as `--reprocess-window-days`
- `0`: unlimited discovery scan

Examples:

- weekly run: `python pipeline.py --reprocess-window-days 14 --discovery-window-days 14`
- monthly run: `python pipeline.py --reprocess-window-days 90 --discovery-window-days 90`
- full historical audit: `python pipeline.py --discovery-window-days 0 --reprocess-window-days 0`

## Implementation Plan

### 1. Add discovery-window plumbing to the pipeline

Update `netflow-db/pipeline.py` to accept and print:

- `--discovery-window-days N`

Thread that value into discovery functions the same way `reprocess_window_days` is threaded today.

Default behavior:

- if the user does not pass `--discovery-window-days`, set it equal to `reprocess_window_days`

That gives one intuitive control by default, while still allowing them to diverge later if needed.

### 2. Add a shared helper for the discovery start date

In `netflow-db/discovery.py`, add a helper that computes the discovery lower bound:

- `None` when unlimited
- otherwise `today_midnight - discovery_window_days`
- always clamped to `DATA_START_DATE`

This helper should be separate from the reprocessing cutoff helper because the semantics are different.

### 3. Replace recursive tree walking with direct date-directory enumeration

Refactor `scan_filesystem()` so it no longer walks every year/month/day directory under each router.

Instead:

- compute the inclusive set of days in the discovery window
- for each router
- for each day in that range
  - construct `NETFLOW_DATA_PATH/router/YYYY/MM/DD`
  - if the directory exists, glob `nfcapd.*` within it
  - if it does not exist, skip it

This should become the primary discovery path.

### 4. Keep full historical discovery mode available

When `discovery_window_days = 0`:

- preserve current unbounded behavior
- either via the old recursive scan implementation or by enumerating every day from `DATA_START_DATE` to today

If performance and simplicity favor it, a direct day-enumeration loop from `DATA_START_DATE` to today is acceptable even in unlimited mode.

### 5. Ensure discovery and gap detection remain compatible

Real-file discovery and synthetic-gap detection should operate over compatible horizons.

Recommended rule:

- discovery only creates/updates real-file entries from within the discovery window
- gap detection only checks within the reprocessing window

This preserves the current processing semantics while making file discovery cheaper.

If we later find we need gap detection to track a broader horizon than reprocessing, that can be added explicitly, but should not be mixed into this first optimization.

### 6. Preserve historical state in the database

Do not delete old `processed_files` entries.

This optimization is about reducing filesystem traversal, not changing what historical state is retained.

That means:

- old real-file rows stay in the database
- old synthetic-gap rows stay in the database
- only the filesystem scan range becomes narrower

### 7. Make defaults match operations

Recommended operating pattern:

- weekly runs: 14-day discovery + 14-day reprocessing
- monthly runs: 90-day discovery + 90-day reprocessing

This matches the current intended use:

- regular recent updates
- periodic fill-in of older late-arriving data

The docs should explicitly note that if a wider reconciliation run is skipped, older delayed files outside the narrow window will remain undiscovered until the next wider run.

### 8. Optional second phase: scan watermark per router

After the date-window optimization lands, consider a follow-up improvement:

- store last scanned day per router
- on normal runs, scan from `min(window_start, last_scanned_day - overlap)` to today

This is optional and should not be bundled into the first implementation.

The first pass should stay simple and easy to reason about.

## Validation Plan

Validate these cases:

1. A narrow discovery window only inspects recent day directories.
2. A wider discovery window finds older late-arriving files correctly.
3. Unlimited mode still discovers all historical files.
4. Processing behavior remains unchanged for already-discovered rows.
5. Boundary-day behavior is stable around local midnight.
6. Weekly and monthly operational runs work as expected with their respective windows.

## Recommendation

Implement the first pass as:

- `--discovery-window-days`
- default to `reprocess_window_days` when unset
- direct date-directory enumeration in `scan_filesystem()`
- preserve `0` as unlimited mode
- keep the two windows conceptually separate even if they share a default value

That gives a meaningful discovery-speed improvement without changing the underlying pipeline model.
