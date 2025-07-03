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
