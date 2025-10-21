# NetFlow Analysis Tool

A web-based network flow analysis tool for visualizing University of Oregon network traffic patterns. Built with SvelteKit frontend and SQLite database backend for efficient querying of netflow data.

## Features

- **Interactive Data Visualization**: Chart.js-powered line graphs with logarithmic scaling
- **Flexible Time Grouping**: Aggregate data by month, date, or hour
- **Multiple Metrics**: Track 15 different network statistics including flows, packets, and bytes by protocol
- **Router Filtering**: Support for multiple routers
- **Date Range Selection**: Query specific time periods with intuitive controls
- **Dynamic X-Axis**: Automatically formats labels based on selected grouping (YYYY-MM, MM/DD/YY, DD:HH)

## Architecture

## Database

### Data Access

```zsh
> cd netflow-db
> sqlite3 flowStats.db
```

Now you're connected to the database. Try some of the following to explore the shape of the database

```zsh
sqlite> .tables
sqlite> .schema ip_stats
sqlite> .schema netflow_stats
```

Some example queries:

```sql
-- Netflow dashboard: daily flow/packet summary for selected routers and time window
SELECT
    DATE(time_start, 'unixepoch') AS day,
    router,
    SUM(flows_total) AS flows,
    SUM(packets_total) AS packets,
    SUM(bytes_total) AS bytes
FROM netflow_stats
WHERE router IN ('router1', 'router2')
  AND time_start BETWEEN strftime('%s', '2025-01-01') AND strftime('%s', '2025-01-08')
GROUP BY day, router
ORDER BY day ASC, router ASC;

-- Drilldown: 30-minute granularity for flows vs protocols (mirrors chart drill interaction)
SELECT
    strftime('%Y-%m-%d %H:%M', time_start, 'unixepoch') AS bucket,
    SUM(flows_tcp) AS flows_tcp,
    SUM(flows_udp) AS flows_udp,
    SUM(flows_icmp) AS flows_icmp
FROM netflow_stats
WHERE router = 'router1'
  AND time_start BETWEEN strftime('%s', '2025-01-03') AND strftime('%s', '2025-01-04')
GROUP BY bucket
ORDER BY bucket;

-- IP statistics chart: per-router unique source/destination IP counts
SELECT
    router,
    bucket_start,
    sa_ipv4_count,
    da_ipv4_count,
    sa_ipv6_count,
    da_ipv6_count
FROM ip_stats
WHERE granularity = '1h'
  AND router IN ('router1', 'router2')
  AND bucket_start BETWEEN strftime('%s', '2025-01-01') AND strftime('%s', '2025-01-02')
ORDER BY router, bucket_start;
```

### Data Processing Pipeline

STATS

1. Raw netflow files from a configurable location
2. Python script (`netflow-db/db.py`) processes files using `nfdump` and populates SQLite database
3. SvelteKit API queries database with `better-sqlite3` for fast aggregation
4. Frontend renders interactive charts with real-time filtering

MAAD

1. Raw netflow files are processed on demand for source/destination IP addresses
2. IPv4 addresses are piped into MAAD CLI
3. Charts are rendered on the frontend

### Database Schema

SQLite database (`flowStats.db`) with `netflow_stats` table containing:

- Router identification and timestamps
- Flow counts by protocol (TCP, UDP, ICMP, Other)
- Packet counts by protocol
- Byte counts by protocol
- Sequence failure tracking
- Indexed for efficient time-range queries

## Getting Started

### Prerequisites

- Node.js 20+
- Python 3.x with `nfdump` available
- SSH access to pinot, to use ONRG database

### Installation

1. **Clone the repository**

   ```bash
   git clone https://gitlab.com/onrg/netflow-analysis
   cd netflow-analysis
   ```

2. **Install frontend dependencies**

   ```bash
   cd netflow-webapp
   npm install
   ```

3. **Populate database** (if needed)

   ```bash
   cd ../netflow-db
   python db.py
   ```

4. **Start development server**

   ```bash
   cd ../netflow-webapp
   npm run dev
   ```

5. **Access via SSH tunnel**
   ```bash
   ssh -L 5173:localhost:5173 user@pinot
   ```
   Then open http://localhost:5173 in your browser

## Usage

### Web Interface

- **Date Range**: Select start and end dates for analysis
- **Time Options**: Choose specific time or analyze full day
- **Router Selection**: Filter by different routers
- **Metrics**: Select from 15 available network statistics
- **Grouping**: Aggregate by month, date, or hour for different perspectives

### Available Metrics

- Flows (Total, TCP, UDP, ICMP, Other)
- Packets (Total, TCP, UDP, ICMP, Other)
- Bytes (Total, TCP, UDP, ICMP, Other)

## Development

### Frontend Commands

```bash
cd netflow-webapp/

npm run dev          # Start development server
npm run build        # Build for production
npm run preview      # Preview production build
npm run check        # TypeScript type checking
npm run format       # Format with Prettier
npm run lint         # Run ESLint and Prettier checks
```

### Database Management

```bash
cd netflow-db/

# Process new netflow files
python db.py

# Query database directly
sqlite3 flowStats.db
```

## Technical Stack

- **Frontend**: SvelteKit 2.x, TypeScript, TailwindCSS 4.x
- **Charts**: Chart.js with logarithmic scaling
- **Database**: SQLite with better-sqlite3 Node.js integration
- **UI Components**: Shadcn-Svelte with custom calendar components
- **Data Processing**: Python with nfdump command-line tool

## Access Requirements

This application requires SSH tunnel access to the UO research infrastructure where netflow data is stored. It's designed specifically for research environment use with proper authentication and network access.

## Known Issues/Bugs
