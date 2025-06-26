# NetFlow Analysis Tool

A web-based network flow analysis tool for visualizing University of Oregon network traffic patterns. Built with SvelteKit frontend and SQLite database backend for efficient querying of netflow data.

## Features

- **Interactive Data Visualization**: Chart.js-powered line graphs with logarithmic scaling
- **Flexible Time Grouping**: Aggregate data by month, date, or hour
- **Multiple Metrics**: Track 15 different network statistics including flows, packets, and bytes by protocol
- **Router Filtering**: Support for `cc-ir1-gw` and `oh-ir1-gw` routers
- **Date Range Selection**: Query specific time periods with intuitive controls
- **Dynamic X-Axis**: Automatically formats labels based on selected grouping (YYYY-MM, MM/DD/YY, HH:MM)

## Architecture

### Data Processing Pipeline

1. Raw netflow files from `/research/tango_cis/uonet-in/{router}/YYYY/MM/DD/`
2. Python script (`netflow-db/db.py`) processes files using `nfdump` and populates SQLite database
3. SvelteKit API queries database with `better-sqlite3` for fast aggregation
4. Frontend renders interactive charts with real-time filtering

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
- SSH access to UO research infrastructure

### Installation

1. **Clone the repository**

   ```bash
   git clone <repository-url>
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
- **Router Selection**: Filter by `cc-ir1-gw` and/or `oh-ir1-gw`
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

## Database Queries

Example queries for exploring the data:

```sql
-- Daily traffic summary
SELECT date(datetime(timestamp, 'unixepoch')) as day,
       SUM(flows) as total_flows,
       SUM(bytes) as total_bytes
FROM netflow_stats
GROUP BY day
ORDER BY day;

-- Protocol breakdown by router
SELECT router,
       SUM(flows_tcp) as tcp_flows,
       SUM(flows_udp) as udp_flows,
       SUM(flows_icmp) as icmp_flows
FROM netflow_stats
GROUP BY router;

-- Hourly traffic patterns
SELECT strftime('%H', datetime(timestamp, 'unixepoch')) as hour,
       AVG(flows) as avg_flows
FROM netflow_stats
GROUP BY hour
ORDER BY hour;
```

## Access Requirements

This application requires SSH tunnel access to the UO research infrastructure where netflow data is stored. It's designed specifically for research environment use with proper authentication and network access.

## Contributing

Please ensure any changes maintain compatibility with the existing database schema and follow the established TypeScript/SvelteKit patterns.
