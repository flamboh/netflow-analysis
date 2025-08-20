import { json } from '@sveltejs/kit';
import { DATABASE_PATH } from '$env/static/private';
import type { RequestHandler } from './$types';
import type { NetflowStatsRow, NetflowStatsResult } from '$lib/types/types';
import Database from 'better-sqlite3';
import path from 'path';

const DB_PATH = path.join(process.cwd(), DATABASE_PATH);
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

function getGroupByQuery(groupBy: string) {
	if (groupBy === 'month') {
		return "strftime('%Y-%m', timestamp, 'unixepoch')";
	} else if (groupBy === 'date') {
		return "strftime('%Y-%m-%d', timestamp, 'unixepoch')";
	} else if (groupBy === 'hour') {
		return "strftime('%Y-%m-%d %H:00:00', timestamp, 'unixepoch')";
	} else if (groupBy === '30min') {
		return "strftime('%Y-%m-%d %H:', timestamp, 'unixepoch') || CASE WHEN CAST(strftime('%M', timestamp, 'unixepoch') AS INTEGER) < 30 THEN '00:00' ELSE '30:00' END";
	} else if (groupBy === '5min') {
		return "strftime('%Y-%m-%d %H:%M:00', timestamp, 'unixepoch')";
	}
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
	const groupBy = url.searchParams.get('groupBy') || 'month';

	// Parse routers
	const routers = routersParam.split(',').filter((r) => r.length > 0);

	if (routers.length === 0) {
		return json({ error: 'No routers selected' }, { status: 400 });
	}

	try {
		const db = new Database(DB_PATH, { readonly: true });
		const groupByQuery = getGroupByQuery(groupBy);
		const dataOptions = getDataOptions(dataOptionsBinary);

		// Build query based on parameters
		let query = `
			SELECT 
				${groupByQuery} as date,
				${dataOptions.join(', ')}
			FROM netflow_stats 
			WHERE router IN (${routers.map(() => '?').join(',')})
			AND timestamp >= ? 
			AND timestamp < ?
		`;

		const params = [...routers, startDate, endDate];

		// // Add time filtering if not full day
		// if (!fullDay) {
		// 	const startTimeFormatted = `${time.slice(0, 2)}:${time.slice(2, 4)}`;
		// 	const endTimeFormatted = `${endTime.slice(0, 2)}:${endTime.slice(2, 4)}`;

		// 	query += ` AND TIME(timestamp) >= ? AND TIME(timestamp) <= ?`;
		// 	params.push(startTimeFormatted, endTimeFormatted);
		// }

		query += ` GROUP BY ${groupByQuery} ORDER BY date`;

		const stmt = db.prepare(query);
		const rows = stmt.all(...params) as NetflowStatsRow[];

		db.close();

		// Format results to match the expected structure from +page.svelte
		const result: NetflowStatsResult[] = rows.map((row) => {
			// Convert date back to YYYYMMDD format
			const dateStr = row.date.replace(/-/g, '');

			// Build data string in the expected format (matching nfdump -I output)
			const dataLines = [
				`Date: ${dateStr}`,
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
				// `Sequence failures: ${row.sequence_failures}`
			];

			return {
				time: dateStr,
				data: dataLines.join('\n')
			};
		});
		return json({ result });
	} catch (error) {
		console.error('Database error:', error);
		return json({ error: 'Database query failed' }, { status: 500 });
	}
};
