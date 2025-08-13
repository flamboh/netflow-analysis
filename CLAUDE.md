# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NetFlow Analysis is a web-based network flow analysis tool that visualizes network traffic patterns from University of Oregon infrastructure. It consists of a SvelteKit frontend with SQLite database backend for efficient data querying and analysis.

## Architecture

**Data Flow:**

1. Netflow data files processed by `netflow-db/db.py` into SQLite database (`flowStats.db`)
2. SvelteKit API endpoint (`src/routes/data/+server.ts`) queries SQLite database with better-sqlite3
3. Frontend visualizes data using Chart.js with interactive date/time controls and groupBy functionality
4. Data aggregation supports month/date/hour grouping with 15 different metrics

**Key Components:**

- `netflow-webapp/` - SvelteKit application with interactive Chart.js visualization
- `netflow-db/` - Python SQLite database setup and data processing pipeline
- `maad/` - Haskell-based Multifractal Address Anomaly Detection module for advanced network security analysis
- `flowStats.db` - SQLite database containing processed netflow statistics
- `src/routes/+page.svelte` - Main interface with intelligent chart type selection
- `src/routes/data/+server.ts` - API endpoint with binary metric encoding

**Supported Routers:**

- `cc-ir1-gw` and `oh-ir1-gw` with data aggregation across multiple sources

## Development Commands

```bash
# Frontend development (run from netflow-webapp/)
npm run dev          # Start development server
npm run build        # Build for production
npm run preview      # Preview production build

# Code quality
npm run check        # TypeScript type checking
npm run format       # Format with Prettier
npm run lint         # Run ESLint and Prettier checks

# MAAD compilation (run from maad/)
nix-shell            # Enter nix environment with required Haskell dependencies
./compile.sh         # Compile Haskell executables (Singularities, StructureFunction)
```

## File Structure Patterns

**Netflow Data Files:**

- Path: `/research/tango_cis/uonet-in/{router}/YYYY/MM/DD/nfcapd.YYYYMMDDHHmm`
- Routers: `cc-ir1-gw`, `oh-ir1-gw`

**Database Schema** (from `netflow-db/db.py`):

- Table: `netflow_stats` with 20+ columns tracking flows, packets, bytes by protocol
- Indexed by router and timestamp for efficient querying
- Supports aggregation queries for different time groupings

## Technical Stack

- **Frontend:** SvelteKit 2.x, TypeScript, TailwindCSS 4.x, Chart.js 4.4.9
- **UI Components:** Shadcn-Svelte with Lucide icons and bits-ui components
- **Database:** SQLite with better-sqlite3 for Node.js integration
- **Data Processing:** Python pipeline using `nfdump` to populate database
- **Security Analysis:** Haskell-based MAAD tools for multifractal anomaly detection
- **Build Tools:** Vite 6.x with ESLint 9.x and Prettier formatting

## Data Visualization Features

The main interface (`src/routes/+page.svelte`) supports:

### Chart Types and Intelligence
- **Stacked Area Charts**: Automatically displayed when selecting metrics of the same type (all flows, all packets, or all bytes)
- **Logarithmic Line Charts**: Used for heterogeneous metric combinations with different scales
- Intelligent chart type detection based on metric homogeneity

### Interactive Features
- **3-Level Drill-Down**: Click month→date→hour→reset navigation
- **Auto-Loading**: Data loads automatically on mount and state changes
- **Real-time Updates**: Chart responds instantly to router/metric selection changes
- 15 different network metrics (flows, packets, bytes by protocol TCP/UDP/ICMP/Other)

### Navigation and Controls
- Date range selection with router filtering (`cc-ir1-gw`, `oh-ir1-gw`)
- Time aggregation by month, date, or hour with automatic date range adjustment
- Quick selection buttons for "All Flows", "All Packets", "All Bytes"
- Dynamic x-axis formatting and y-axis scaling (K/M/B/T/P units)
- Responsive chart design with 20-color cycling palette

## Access Method

Application requires SSH tunnel access as noted in README - designed for research environment use.

## MAAD (Multifractal Address Anomaly Detection)

The MAAD module provides advanced network security analysis capabilities using multifractal analysis techniques to detect anomalous IP address patterns in network traffic.

### MAAD Components

**Core Haskell Modules:**
- `Singularities.hs` - Computes alpha(x) singularity metrics to identify anomalous addresses
- `StructureFunction.hs` - Calculates tau(q) structure functions for multifractal analysis
- `Common.hs` - IPv4 address utilities and bit manipulation functions
- `PrefixMap.hs` - Efficient prefix-based IP address mapping and aggregation

**Build System:**
- `shell.nix` - Nix environment with required Haskell dependencies (bytestring, statistics, etc.)
- `compile.sh` - GHC compilation script for optimized executables
- `test_data/` - Sample IPv4 address lists for testing (simple.csv, caida_100k.csv)

### MAAD Usage

**Singularities Analysis:**
```bash
# Identify top N anomalous addresses
./Singularities <N> <input_file>
# Example: ./Singularities 10 test_data/simple.csv
```

**Structure Function Analysis:**
```bash
# Compute multifractal structure function
./StructureFunction <input_file>  
# Example: ./StructureFunction test_data/simple.csv
```

**Input Format:**
- IPv4 addresses in dotted-decimal notation (192.0.2.1)
- One address per line
- Use "-" for stdin input

### Integration with NetFlow Analysis

**Current Status:**
- `netflow-db/maad.py` - Empty Python interface (future integration point)
- MAAD designed to complement existing netflow statistics with anomaly detection capabilities
- Planned integration: Extract IP addresses from netflow data → MAAD analysis → Display anomaly metrics in webapp

**Future Integration Plans:**
1. **Data Pipeline**: Extract IP addresses from SQLite database to feed MAAD tools
2. **API Endpoints**: Add anomaly detection results to existing REST API structure
3. **Visualization**: Display singularity metrics and anomalous addresses in Chart.js interface
4. **Real-time Analysis**: Integrate MAAD processing into regular netflow data updates

**Technical Requirements:**
- Haskell GHC compiler with statistics, bytestring, and vector libraries
- Nix package manager recommended for dependency management
- Python integration layer for database connectivity

## Svelte/SvelteKit Architecture Analysis

### Current Alignment with Svelte Design Philosophy

**✅ Strengths:**

1. **Modern Svelte 5 Runes Usage**: The project correctly uses Svelte 5's new `$state` runes for reactive state management, replacing legacy `let` declarations with explicit reactivity
2. **Proper File-based Routing**: Follows SvelteKit's file-based routing conventions with `+page.svelte`, `+page.server.ts`, and `+server.ts` files
3. **Server-side Data Loading**: Uses `+page.server.ts` for database operations, keeping database logic on the server side
4. **TypeScript Integration**: Proper use of TypeScript with SvelteKit's generated types (`$types`)
5. **Component Structure**: Well-organized component library in `src/lib/components/ui/` following modern component patterns

**⚠️ Areas for Improvement:**

1. **State Management Architecture**: The main page (`+page.svelte`) contains ~500 lines with complex state logic mixed with UI. This violates Svelte's philosophy of component composition
2. **Missing Reactive Derivations**: Heavy use of imperative effects (`onMount`, manual chart updates) instead of Svelte 5's `$derived` for computed state
3. **Mixed Concerns**: Chart.js integration, data fetching, and UI logic are tightly coupled in a single component
4. **Legacy Patterns**: Still uses `onMount` and imperative DOM manipulation instead of leveraging Svelte 5's reactive primitives
5. **Type Safety**: Several `any` types and missing proper TypeScript interfaces for data structures

### Recommended Refactoring for Svelte Best Practices

**1. Component Decomposition:**
```svelte
src/routes/+page.svelte (simplified coordinator)
src/lib/components/
├── charts/
│   ├── ChartContainer.svelte
│   ├── ChartControls.svelte
│   └── chart-utils.ts
├── filters/
│   ├── DateRangeFilter.svelte
│   ├── RouterFilter.svelte
│   └── MetricSelector.svelte
└── netflow/
    ├── NetflowDashboard.svelte
    └── types.ts
```

**2. State Management with Runes:**
```javascript
// src/lib/stores/netflow.svelte.js
export const chartState = $state({
  startDate: '2024-03-01',
  endDate: new Date().toISOString().slice(0, 10),
  routers: { 'cc-ir1-gw': true, 'oh-ir1-gw': true },
  groupBy: 'date'
});

export const chartData = $derived.by(async () => {
  // Reactive data fetching based on chartState
});
```

**3. Better Load Function Architecture:**
```typescript
// src/routes/+layout.server.ts
export const load = async () => ({
  routers: await getAvailableRouters(),
  dateRange: await getAvailableDateRange()
});

// src/routes/+page.js (universal load)
export const load = async ({ fetch, url, depends }) => {
  depends('netflow:data');
  // Handle client-side navigation and data fetching
};
```

### REST API Recommendations

The current API endpoint (`/data/+server.ts`) could be restructured to follow REST conventions:

**Current:**
- `GET /data?params...` (single endpoint with query parameters)

**Proposed RESTful Structure:**
```
GET    /api/routers                    # List available routers
GET    /api/routers/{id}/metrics       # Get metrics for specific router
GET    /api/netflow/stats             # Aggregated statistics (current /data functionality)
GET    /api/netflow/files/{slug}      # Individual file details (current /nfcapd/[slug])
GET    /api/netflow/summary           # Dashboard summary data
GET    /api/health                    # Service health check
```

**Benefits:**
- Clear resource-based URLs
- Cacheable endpoints
- Better separation of concerns
- Easier API documentation and testing
- Future extensibility for additional data sources

**Implementation Strategy:**
```
src/routes/api/
├── routers/
│   └── +server.ts
├── netflow/
│   ├── stats/+server.ts
│   ├── files/[slug]/+server.ts
│   └── summary/+server.ts
└── health/+server.ts
```

### Action Items for Svelte 5 Modernization

1. **Refactor Main Component**: Break down `+page.svelte` into focused, single-responsibility components
2. **Implement Reactive Derivations**: Replace imperative effects with `$derived` for computed state
3. **Add Proper TypeScript**: Define interfaces for all data structures and API responses
4. **State Management**: Move shared state to `.svelte.js` files with proper rune usage
5. **Error Boundaries**: Add `<svelte:boundary>` for better error handling
6. **Performance**: Use `$state.raw` for large datasets that don't need deep reactivity
7. **Testing**: Add component tests using Svelte Testing Library with runes support
8. **Environment Variables**: Add environment variables for the location of the NetFlow data

This refactoring would align the project with Svelte 5's design philosophy of explicit reactivity, component composition, and clear separation of concerns while maintaining the existing functionality.
