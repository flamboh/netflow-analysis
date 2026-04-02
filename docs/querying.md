# Querying the Database

Each dataset has its own SQLite database at `data/<dataset>/netflow.sqlite`.

## Direct Access

```bash
sqlite3 data/<dataset>/netflow.sqlite
```

Useful SQLite commands:

```
.tables
.schema netflow_stats
.schema ip_stats
```

## Schema Overview

**`netflow_stats`** — per-router flow/packet/byte counts, bucketed by time:

- `time_start` (Unix timestamp), `router`
- `flows_total`, `flows_tcp`, `flows_udp`, `flows_icmp`, `flows_other`
- `packets_*`, `bytes_*` (same protocol breakdown)
- Indexed for efficient time-range queries

**`ip_stats`** — unique source/destination IP counts per router per hour:

- `router`, `bucket_start`, `granularity`
- `sa_ipv4_count`, `da_ipv4_count`, `sa_ipv6_count`, `da_ipv6_count`

## Example Queries

Daily flow/packet summary for a time window:

```sql
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
ORDER BY day, router;
```

30-minute protocol breakdown for a single day:

```sql
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
```

Per-router IP counts:

```sql
SELECT router, bucket_start, sa_ipv4_count, da_ipv4_count, sa_ipv6_count, da_ipv6_count
FROM ip_stats
WHERE granularity = '1h'
  AND router IN ('router1', 'router2')
  AND bucket_start BETWEEN strftime('%s', '2025-01-01') AND strftime('%s', '2025-01-02')
ORDER BY router, bucket_start;
```
