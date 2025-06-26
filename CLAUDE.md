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

- `netflow-webapp/` - SvelteKit application with TailwindCSS, TypeScript, and better-sqlite3
- `netflow-db/` - Python SQLite database setup and data processing pipeline
- `flowStats.db` - SQLite database containing processed netflow statistics

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

- **Frontend:** SvelteKit 2.x, TypeScript, TailwindCSS 4.x, Chart.js
- **UI Components:** Shadcn-Svelte with custom calendar components
- **Database:** SQLite with better-sqlite3 for Node.js integration
- **Data Processing:** Python pipeline using `nfdump` to populate database

## Data Visualization Features

The main interface (`src/routes/+page.svelte`) supports:

- Date range selection with router filtering (`cc-ir1-gw`, `oh-ir1-gw`)
- 15 different network metrics (flows, packets, bytes by protocol)
- Logarithmic scale visualization with Chart.js
- Time aggregation by month, date, or hour
- Dynamic x-axis formatting based on grouping selection
- Full-day vs. specific time analysis

## Access Method

Application requires SSH tunnel access as noted in README - designed for research environment use.
