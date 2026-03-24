import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import type { NetflowStatsResult } from '$lib/types/types';
import { getDatasetDb, getRequestedDataset } from '$lib/server/datasets';

// Bucket sizes in seconds
const BUCKET_SIZES: Record<string, number> = {
	date: 86400, // 24 hours
	hour: 3600, // 1 hour
	'30min': 1800, // 30 minutes
	'5min': 300 // 5 minutes
};

/**
 * Generate SQL expression for epoch-based bucket calculation in local time.
 * Server timezone is set to America/Los_Angeles to handle DST correctly.
 */
function getBucketStartQuery(groupBy: string): string {
	const bucketSize = BUCKET_SIZES[groupBy] ?? BUCKET_SIZES.date;
	return `(CAST(strftime('%s', datetime(timestamp, 'unixepoch', 'localtime', 'start of day', 'utc', printf('+%d seconds', ((CAST(strftime('%s', datetime(timestamp, 'unixepoch', 'localtime')) AS integer) - CAST(strftime('%s', datetime(timestamp, 'unixepoch', 'localtime', 'start of day')) AS integer)) / ${bucketSize}) * ${bucketSize}))) AS integer))`;
}

export const GET: RequestHandler = async ({ url }) => {
	const dataset = getRequestedDataset(url);
	const startDate = url.searchParams.get('startDate') || '';
	const endDate = url.searchParams.get('endDate') || '';
	// const fullDay = url.searchParams.get('fullDay') === 'true';
	// const time = url.searchParams.get('time') || '1200';
	// const endTime = url.searchParams.get('endTime') || '0100';
	const routersParam = url.searchParams.get('routers') || '';
	const groupBy = url.searchParams.get('groupBy') || 'date';

	// Parse routers
	const routers = routersParam.split(',').filter((r) => r.length > 0);

	if (routers.length === 0) {
		return json({ error: 'No routers selected' }, { status: 400 });
	}

	try {
		const db = getDatasetDb(dataset);
		const bucketStartQuery = getBucketStartQuery(groupBy);

		const query = `
			SELECT 
				${bucketStartQuery} as bucketStart,
				SUM(flows) AS flows,
				SUM(flows_tcp) AS flowsTcp,
				SUM(flows_udp) AS flowsUdp,
				SUM(flows_icmp) AS flowsIcmp,
				SUM(flows_other) AS flowsOther,
				SUM(packets) AS packets,
				SUM(packets_tcp) AS packetsTcp,
				SUM(packets_udp) AS packetsUdp,
				SUM(packets_icmp) AS packetsIcmp,
				SUM(packets_other) AS packetsOther,
				SUM(bytes) AS bytes,
				SUM(bytes_tcp) AS bytesTcp,
				SUM(bytes_udp) AS bytesUdp,
				SUM(bytes_icmp) AS bytesIcmp,
				SUM(bytes_other) AS bytesOther
			FROM netflow_stats 
			WHERE router IN (${routers.map(() => '?').join(',')})
			AND timestamp >= ? 
			AND timestamp < ?
			GROUP BY bucketStart
			ORDER BY bucketStart
		`;

		const params = [...routers, startDate, endDate];

		const stmt = db.prepare(query);
		const rows = stmt.all(...params) as NetflowStatsResult[];
		const result: NetflowStatsResult[] = rows.map((row) => ({
			bucketStart: row.bucketStart,
			flows: row.flows ?? 0,
			flowsTcp: row.flowsTcp ?? 0,
			flowsUdp: row.flowsUdp ?? 0,
			flowsIcmp: row.flowsIcmp ?? 0,
			flowsOther: row.flowsOther ?? 0,
			packets: row.packets ?? 0,
			packetsTcp: row.packetsTcp ?? 0,
			packetsUdp: row.packetsUdp ?? 0,
			packetsIcmp: row.packetsIcmp ?? 0,
			packetsOther: row.packetsOther ?? 0,
			bytes: row.bytes ?? 0,
			bytesTcp: row.bytesTcp ?? 0,
			bytesUdp: row.bytesUdp ?? 0,
			bytesIcmp: row.bytesIcmp ?? 0,
			bytesOther: row.bytesOther ?? 0
		}));
		return json({ result });
	} catch (error) {
		console.error('Database error:', error);
		return json({ error: 'Database query failed' }, { status: 500 });
	}
};
