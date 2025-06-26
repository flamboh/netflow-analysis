# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NetFlow Analysis is a web-based network flow analysis tool that visualizes network traffic patterns from University of Oregon infrastructure. It consists of a SvelteKit frontend that processes netflow data using the `nfdump` command-line tool.

## Architecture

**Data Flow:**

1. Netflow data files stored in `/research/tango_cis/uonet-in/{router}/YYYY/MM/DD/` directories
2. SvelteKit API endpoint (`src/routes/data/+server.ts`) executes `nfdump` commands to extract statistics
3. Frontend visualizes data using Chart.js with interactive date/time controls
4. SQLite database (`netflow-db/flowStats.db`) exists but is not used in current data flow

**Key Components:**

- `netflow-webapp/` - SvelteKit application with TailwindCSS and TypeScript
- `netflow-db/` - Python SQLite setup (experimental, not actively used)
- Data processing happens real-time via `nfdump` rather than database queries

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
- Currently unused in main application flow

## Technical Stack

- **Frontend:** SvelteKit 2.x, TypeScript, TailwindCSS 4.x, Chart.js
- **UI Components:** Shadcn-Svelte with custom calendar components
- **Data Processing:** `nfdump` command-line tool
- **Database:** SQLite (experimental)

## Data Visualization Features

The main interface (`src/routes/+page.svelte`) supports:

- Date range selection with router filtering
- 20 different network metrics (flows, packets, bytes by protocol)
- Logarithmic scale visualization
- Full-day vs. specific time analysis

## Access Method

Application requires SSH tunnel access as noted in README - designed for research environment use.
