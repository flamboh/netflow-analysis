import { json } from '@sveltejs/kit';
import { DATABASE_PATH } from '$env/static/private';
import type { RequestHandler } from './$types';
import type { NetflowStatsRow, NetflowStatsResult } from '$lib/types/types';
import Database from 'better-sqlite3';

const DB_PATH = DATABASE_PATH;
const DATA_OPTIONS = [
	{ label: 'Flows', value: 'flows' },
	{ label: 'Flows TCP', value: 'flows_tcp' },
	{ label: 'Flows UDP', value: 'flows_udp' },
	{ label: 'Flows ICMP', value: 'flows_icmp' },
	{ label: 'Flows Other', value: 'flows_other' },
	{ label: 'Packets', value: 'packets' },
	{ label: 'Packets TCP', value: 'packets_tcp' },
	{ label: 'Packets UDP', value: 'packets_udp' },
	{ label: 'Packets ICMP', value: 'packets_icmp' },
	{ label: 'Packets Other', value: 'packets_other' },
	{ label: 'Bytes', value: 'bytes' },
	{ label: 'Bytes TCP', value: 'bytes_tcp' },
	{ label: 'Bytes UDP', value: 'bytes_udp' },
	{ label: 'Bytes ICMP', value: 'bytes_icmp' },
	{ label: 'Bytes Other', value: 'bytes_other' }
	// { label: 'Sequence Failures', value: 'sequence_failures' }
];

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
	return `(CAST(strftime('%s', datetime(timestamp, 'unixepoch', 'localtime')) AS integer) / ${bucketSize}) * ${bucketSize}`;
}

function getDataOptions(dataOptionsBinary: number) {
	const dataOptionsArray: boolean[] = [];
	const result: string[] = [];
	for (let i = 0; i < 16; i++) {
		dataOptionsArray.push(dataOptionsBinary & (1 << i) ? true : false);
	}
	for (let i = 0; i < 16; i++) {
		if (dataOptionsArray[i]) {
			const option = DATA_OPTIONS[i];
			result.push(`SUM(${option.value}) as ${option.value}`);
		}
	}
	return result;
}

export const GET: RequestHandler = async ({ url }) => {
	const startDate = url.searchParams.get('startDate') || '';
	const endDate = url.searchParams.get('endDate') || '';
	// const fullDay = url.searchParams.get('fullDay') === 'true';
	// const time = url.searchParams.get('time') || '1200';
	// const endTime = url.searchParams.get('endTime') || '0100';
	const routersParam = url.searchParams.get('routers') || '';
	const dataOptionsBinary = parseInt(url.searchParams.get('dataOptions') || '0');
	const groupBy = url.searchParams.get('groupBy') || 'date';

	// Parse routers
	const routers = routersParam.split(',').filter((r) => r.length > 0);

	if (routers.length === 0) {
		return json({ error: 'No routers selected' }, { status: 400 });
	}

	try {
		const db = new Database(DB_PATH, { readonly: true });
		const bucketStartQuery = getBucketStartQuery(groupBy);
		const dataOptions = getDataOptions(dataOptionsBinary);

		// Build query using epoch-based bucket calculation
		const query = `
			SELECT 
				${bucketStartQuery} as bucket_start,
				${dataOptions.join(', ')}
			FROM netflow_stats 
			WHERE router IN (${routers.map(() => '?').join(',')})
			AND timestamp >= ? 
			AND timestamp < ?
			GROUP BY bucket_start
			ORDER BY bucket_start
		`;

		const params = [...routers, startDate, endDate];

		const stmt = db.prepare(query);
		const rows = stmt.all(...params) as (NetflowStatsRow & { bucket_start: number })[];

		db.close();

		// Format results - time is now an epoch timestamp (as string for compatibility)
		// Frontend will format using PST utilities
		const result: NetflowStatsResult[] = rows.map((row) => {
			// Use epoch timestamp as the time field (stored as string for backward compatibility)
			const bucketStart = row.bucket_start;

			// Build data string in the expected format (matching nfdump -I output)
			const dataLines = [
				`Date: ${bucketStart}`,
				`Flows: ${row.flows ?? 0}`,
				`Flows_tcp: ${row.flows_tcp ?? 0}`,
				`Flows_udp: ${row.flows_udp ?? 0}`,
				`Flows_icmp: ${row.flows_icmp ?? 0}`,
				`Flows_other: ${row.flows_other ?? 0}`,
				`Packets: ${row.packets ?? 0}`,
				`Packets_tcp: ${row.packets_tcp ?? 0}`,
				`Packets_udp: ${row.packets_udp ?? 0}`,
				`Packets_icmp: ${row.packets_icmp ?? 0}`,
				`Packets_other: ${row.packets_other ?? 0}`,
				`Bytes: ${row.bytes ?? 0}`,
				`Bytes_tcp: ${row.bytes_tcp ?? 0}`,
				`Bytes_udp: ${row.bytes_udp ?? 0}`,
				`Bytes_icmp: ${row.bytes_icmp ?? 0}`,
				`Bytes_other: ${row.bytes_other ?? 0}`
			];

			return {
				time: String(bucketStart),
				data: dataLines.join('\n')
			};
		});
		return json({ result });
	} catch (error) {
		console.error('Database error:', error);
		return json({ error: 'Database query failed' }, { status: 500 });
	}
};
