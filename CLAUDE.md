# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NetFlow Analysis is a web-based network flow analysis tool that visualizes network traffic patterns from University of Oregon infrastructure. It consists of a SvelteKit frontend with SQLite database backend for efficient data querying and analysis.

## Architecture

**Data Flow:**

1. Netflow data files processed by `netflow-db/flow_db.py` into SQLite database (`flowStats.db`)
2. MAAD analysis results managed by `netflow-db/maad_db.py` for multifractal anomaly detection
3. SvelteKit API endpoints query SQLite database with better-sqlite3:
   - `/api/netflow/stats/` - Aggregated network statistics
   - `/api/netflow/files/[slug]/` - Individual file analysis
   - `/api/netflow/files/[slug]/structure-function/` - Real-time MAAD analysis
   - `/api/database/` - Database update trigger with logging
4. Frontend visualizes data using Chart.js with interactive date/time controls and groupBy functionality
5. Data aggregation supports month/date/hour grouping with 15 different metrics
6. Individual NetFlow file pages display structure function analysis for source and destination addresses

**Key Components:**

- `netflow-webapp/` - SvelteKit application with interactive Chart.js visualization
- `netflow-db/` - Python SQLite database setup and data processing pipeline
  - `flow_db.py` - Main NetFlow data processing and database population
  - `maad_db.py` - MAAD analysis results management and storage
- `maad/` - Haskell-based Multifractal Address Anomaly Detection module for advanced network security analysis
- `flowStats.db` - SQLite database containing processed netflow statistics and MAAD analysis results
- `src/routes/+page.svelte` - Main interface with intelligent chart type selection
- `src/routes/+layout.svelte` - Navigation layout with database update button
- `src/routes/api/netflow/files/[slug]/+page.svelte` - Individual file analysis with structure function visualization
- `src/lib/types/types.ts` - TypeScript type definitions for NetFlow API responses and database query results; add reusable types here when they occur multiple times across the codebase

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

# Database management (run from netflow-db/)
python3 flow_db.py   # Update NetFlow statistics database
python3 maad_db.py   # Process MAAD analysis results (planned)

# MAAD compilation (run from maad/)
nix-shell            # Enter nix environment with required Haskell dependencies
./compile.sh         # Compile Haskell executables (Singularities, StructureFunction)
```

## File Structure Patterns

**Netflow Data Files:**

- Path: `/research/tango_cis/uonet-in/{router}/YYYY/MM/DD/nfcapd.YYYYMMDDHHmm`
- Routers: `cc-ir1-gw`, `oh-ir1-gw`

**Database Schema**:

- `netflow_stats` - 20+ columns tracking flows, packets, bytes by protocol (from `flow_db.py`)
- `maad_metadata` - Links MAAD analysis to netflow_stats records (planned, from `maad_db.py`)
- `structure_function_results` - Cached structure function analysis results (planned)
- `spectrum_results` - Cached spectrum analysis results (planned)
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
- **Database Update Button**: Triggers flow_db.py execution with PST logging to `./logs/flowStats/`

### Individual NetFlow File Analysis

- **File Pages**: Accessible via `/files` route, individual file analysis at `/api/netflow/files/[slug]/`
- **Structure Function Visualization**: Real-time MAAD analysis with Chart.js display
- **Dual Analysis**: Separate graphs for source and destination address analysis
- **Error Handling**: Retry buttons for failed analysis, loading states, timeout management
- **Metadata Display**: File information, processing timestamps, protocol breakdowns

## Access Method

Application requires SSH tunnel access as noted in README - designed for research environment use.

## MAAD (Multifractal Address Anomaly Detection)

The MAAD submodule provides advanced network security analysis capabilities using multifractal analysis techniques to detect anomalous IP address patterns in network traffic.

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

**Current Implementation:**

- **Real-time Structure Function Analysis**: `/api/netflow/files/[slug]/structure-function/` endpoint
- **Direct NetFlow Processing**: Uses `nfdump` to extract IP addresses and pipe to MAAD tools
- **Web Visualization**: Chart.js displays structure function results with error bars
- **Dual Address Analysis**: Separate processing for source and destination addresses
- **Database Integration**: Queries `netflow_stats` to locate NetFlow files by timestamp
- **Performance Optimization**: 60-second timeout, 10MB buffer limits for large files

**Database Storage (Planned):**

- `netflow-db/maad_db.py` - Batch processing and caching of MAAD analysis results
- Pre-computed storage for Structure Function and Spectrum analysis
- Immutable analysis results cached permanently after computation
- Integration with existing flowStats.db schema

**Processing Pipeline:**

1. **Real-time**: API endpoint extracts addresses with `nfdump` → pipes to `StructureFunction` → returns JSON
2. **Batch**: `maad_db.py` processes unanalyzed files → stores results in database → serves cached results
3. **Hybrid**: Check database cache first, compute on-demand for cache misses

**Technical Requirements:**

- Haskell GHC compiler with statistics, bytestring, and vector libraries
- Nix package manager recommended for dependency management
- Python integration layer for database connectivity

- Use YYYY-MM-DD HH:mm:ss format for any dates
- Keep commit messages brief. One sentence usually. Avoid adding bullet points for commit messages with 1-2 significant changes. Don't explain much of implementation details.

- Avoid using the `any` type. If it is truly unavoidable in complex chart components, use a narrowly scoped `as any` cast and add an inline ESLint disable for `@typescript-eslint/no-explicit-any` with a short justification.
