# Dashboard Data Loading + Chart Refactor

## Goal

Fix data-loading churn, chart re-render churn, and heavy eager work with incremental cleanup.

## Non-goals

- No global `ssr = false`
- No server-blocking move of heavy dashboard fetches into `+page.server.ts`

## Principles

- Keep SSR on globally
- Use SSR/load for shell + nav-critical + lightweight data only
- Keep heavy interactive/dashboard data progressive on client
- Centralize resource loading; remove per-component fetch orchestration
- Use `$derived` for state derivation; `$effect` only for imperative edges
- Keep stale chart data visible during incremental refetches
- Prefer small vertical slices over big-bang rewrites
- Lazy-load expensive secondary analysis when user intent is clear

## Learnings from PR1

- Small slices work well here; meaningful wins landed without infra churn
- Expensive analysis is a good fit for explicit user-triggered loading
- Chart correctness issues are often config/lifecycle problems, not library problems
- File detail state is already too dense; breakup should happen early, not later
- Existing mutable `Map` + effect-heavy patterns make small UI changes harder than they should be
- Plan should prioritize shared client resources and file-detail breakup sooner

## PR1: Immediate UX fixes

### Scope

- Fix NetFlow line-mode zero handling
- Remove production debug logging from chart click path
- Make singularities click-to-load instead of eager fanout

### Why first

- Smallest risk
- Immediate perf/UX win
- Clears obvious correctness issue

### Acceptance

- NetFlow line mode renders zero-value periods correctly
- File detail page does not start singularities analysis until user asks
- No chart click debug spam in browser console

## PR2: Route/data-loading cleanup

### Scope

- Move lightweight nav-critical data to route `load`
- Keep heavy detail/chart data out of blocking server loads
- Move the file detail page implementation out of `apps/web/src/routes/api/netflow/files/[slug]/+page.svelte`
- Replace the current route reuse hack with a normal page implementation under `apps/web/src/routes/netflow/files/[slug]/`
- Start with:
  - dataset list page
  - file jump page
  - dataset dashboard router bootstrap
  - file detail shell metadata + route structure cleanup
- Prefer `+page.ts` over `+page.server.ts` unless server-only access is required

### Notes

- Verified SvelteKit pattern:
  - SSR enabled by default
  - universal `load` runs on initial SSR, then client on later nav
  - `onMount` is client-only, so should not own primary page data
- The file detail page currently lives under `routes/api/` even though it is a UI page; fix that while breaking it apart

### Acceptance

- Primary pages render useful initial state without mount-only fetch dependency
- Navigation remains fast; no heavy blocking server load introduced
- Fewer route waterfalls on first render
- File detail page no longer lives in `routes/api/`

## PR3: Shared client resource layer + stale-while-refetch

### Scope

- Introduce shared keyed resource loaders for file-detail/dashboard client data
- Deduplicate fetch/cache/state handling now spread across components
- Start with file-detail data first, since that page is currently the most state-dense
- Replace destroy-and-rebuild loading behavior on partial cache misses
- Keep stale chart data rendered while fetching missing window segments

### Candidate resources

- file details
- netflow stats
- IP stats
- protocol stats
- spectrum stats
- singularities requests

### Implementation direction

- Resource shape: `{ data, loading, error, refresh }`
- Keys based on dataset + routers + range + granularity
- Components consume derived resource state; do not own fetch lifecycle
- Break file-detail page into smaller view components while moving resource state out of the page shell

### Acceptance

- File-detail page state surface is materially smaller
- Partial range extensions do not blank charts
- Shared cache/resource logic replaces bespoke per-chart fetch logic
- Reduced duplicate request/state code across chart components

## PR4: Chart architecture consolidation

### Scope

- Extract shared chart controller/helpers for:
  - create/update/destroy
  - theme sync
  - crosshair sync
  - range selection
  - loading overlay strategy
- Reduce duplicated logic across NetFlow/IP/Protocol/Spectrum charts
- Replace avoidable `$effect`-driven state syncing with `$derived` where possible
- Fetch only current router for `SpectrumStatsChart`

### Acceptance

- Shared chart lifecycle path used by core charts
- Fewer imperative effects; clearer reactive graph
- Spectrum fetch scope matches visible router selection

## Success signals

- Faster first useful render
- Fewer blank/loading flashes during filter/range changes
- Lower code duplication in chart components
- Heavy analysis only when user requests it
- Clear SSR/client boundary: shell SSR, heavy data progressive
