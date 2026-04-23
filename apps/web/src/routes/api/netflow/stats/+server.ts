import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import type { NetflowStatsResult } from '$lib/types/types';
import { getDatasetDb, getRequestedDataset } from '$lib/server/datasets';
import {
	getBucketStartQuery,
	getNetflowSchemaVersion,
	parseSourceIds,
	placeholders
} from '$lib/server/netflow-v2';

export const GET: RequestHandler = async ({ url }) => {
	const dataset = getRequestedDataset(url);
	const startDate = url.searchParams.get('startDate') || '';
	const endDate = url.searchParams.get('endDate') || '';
	const groupBy = url.searchParams.get('groupBy') || 'date';
	const routers = parseSourceIds(url.searchParams.get('routers'));

	if (routers.length === 0) {
		return json({ error: 'No routers selected' }, { status: 400 });
	}

	try {
		const db = getDatasetDb(dataset);
		const schema = getNetflowSchemaVersion(db);
		const timeColumn = schema === 'v2' ? 'bucket_start' : 'timestamp';
		const sourceColumn = schema === 'v2' ? 'source_id' : 'router';
		const tableName = schema === 'v2' ? 'netflow_stats_v2' : 'netflow_stats';
		const bucketStartQuery = getBucketStartQuery(timeColumn, groupBy);

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
			FROM ${tableName} 
			WHERE ${sourceColumn} IN (${placeholders(routers)})
			AND ${timeColumn} >= ? 
			AND ${timeColumn} < ?
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
