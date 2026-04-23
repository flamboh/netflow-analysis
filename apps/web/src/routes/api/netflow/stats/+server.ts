import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import type {
	NetflowIpFamily,
	NetflowMetricField,
	NetflowSplitMetricField,
	NetflowStatsResponse,
	NetflowStatsResult
} from '$lib/types/types';
import { getDatasetDb, getRequestedDataset } from '$lib/server/datasets';
import { NETFLOW_DATA_OPTION_FIELDS } from '$lib/components/netflow/constants';
import {
	getBucketStartQuery,
	getNetflowSchemaVersion,
	parseSourceIds,
	placeholders
} from '$lib/server/netflow-v2';

const V2_IP_VERSION_BY_FAMILY: Record<Exclude<NetflowIpFamily, 'all'>, 4 | 6> = {
	ipv4: 4,
	ipv6: 6
};

function getBaseMetricSelects(): string[] {
	return NETFLOW_DATA_OPTION_FIELDS.map((field) => {
		const columnName = field.replace(/[A-Z]/g, (match) => `_${match.toLowerCase()}`).toLowerCase();
		return `SUM(${columnName}) AS ${field}`;
	});
}

function getFamilyMetricSelects(family: Exclude<NetflowIpFamily, 'all'>): string[] {
	const ipVersion = V2_IP_VERSION_BY_FAMILY[family];
	const suffix = family === 'ipv4' ? 'Ipv4' : 'Ipv6';
	return NETFLOW_DATA_OPTION_FIELDS.map((field) => {
		const columnName = field.replace(/[A-Z]/g, (match) => `_${match.toLowerCase()}`).toLowerCase();
		return `SUM(CASE WHEN ip_version = ${ipVersion} THEN ${columnName} ELSE 0 END) AS ${field}${suffix}`;
	});
}

function getMetricValue(
	row: Record<string, number | null>,
	field: NetflowMetricField | NetflowSplitMetricField
): number {
	return row[field] ?? 0;
}

function normalizeRow(row: Record<string, number | null>): NetflowStatsResult {
	return {
		bucketStart: row.bucketStart ?? 0,
		flows: getMetricValue(row, 'flows'),
		flowsTcp: getMetricValue(row, 'flowsTcp'),
		flowsUdp: getMetricValue(row, 'flowsUdp'),
		flowsIcmp: getMetricValue(row, 'flowsIcmp'),
		flowsOther: getMetricValue(row, 'flowsOther'),
		packets: getMetricValue(row, 'packets'),
		packetsTcp: getMetricValue(row, 'packetsTcp'),
		packetsUdp: getMetricValue(row, 'packetsUdp'),
		packetsIcmp: getMetricValue(row, 'packetsIcmp'),
		packetsOther: getMetricValue(row, 'packetsOther'),
		bytes: getMetricValue(row, 'bytes'),
		bytesTcp: getMetricValue(row, 'bytesTcp'),
		bytesUdp: getMetricValue(row, 'bytesUdp'),
		bytesIcmp: getMetricValue(row, 'bytesIcmp'),
		bytesOther: getMetricValue(row, 'bytesOther'),
		flowsIpv4: getMetricValue(row, 'flowsIpv4'),
		flowsTcpIpv4: getMetricValue(row, 'flowsTcpIpv4'),
		flowsUdpIpv4: getMetricValue(row, 'flowsUdpIpv4'),
		flowsIcmpIpv4: getMetricValue(row, 'flowsIcmpIpv4'),
		flowsOtherIpv4: getMetricValue(row, 'flowsOtherIpv4'),
		packetsIpv4: getMetricValue(row, 'packetsIpv4'),
		packetsTcpIpv4: getMetricValue(row, 'packetsTcpIpv4'),
		packetsUdpIpv4: getMetricValue(row, 'packetsUdpIpv4'),
		packetsIcmpIpv4: getMetricValue(row, 'packetsIcmpIpv4'),
		packetsOtherIpv4: getMetricValue(row, 'packetsOtherIpv4'),
		bytesIpv4: getMetricValue(row, 'bytesIpv4'),
		bytesTcpIpv4: getMetricValue(row, 'bytesTcpIpv4'),
		bytesUdpIpv4: getMetricValue(row, 'bytesUdpIpv4'),
		bytesIcmpIpv4: getMetricValue(row, 'bytesIcmpIpv4'),
		bytesOtherIpv4: getMetricValue(row, 'bytesOtherIpv4'),
		flowsIpv6: getMetricValue(row, 'flowsIpv6'),
		flowsTcpIpv6: getMetricValue(row, 'flowsTcpIpv6'),
		flowsUdpIpv6: getMetricValue(row, 'flowsUdpIpv6'),
		flowsIcmpIpv6: getMetricValue(row, 'flowsIcmpIpv6'),
		flowsOtherIpv6: getMetricValue(row, 'flowsOtherIpv6'),
		packetsIpv6: getMetricValue(row, 'packetsIpv6'),
		packetsTcpIpv6: getMetricValue(row, 'packetsTcpIpv6'),
		packetsUdpIpv6: getMetricValue(row, 'packetsUdpIpv6'),
		packetsIcmpIpv6: getMetricValue(row, 'packetsIcmpIpv6'),
		packetsOtherIpv6: getMetricValue(row, 'packetsOtherIpv6'),
		bytesIpv6: getMetricValue(row, 'bytesIpv6'),
		bytesTcpIpv6: getMetricValue(row, 'bytesTcpIpv6'),
		bytesUdpIpv6: getMetricValue(row, 'bytesUdpIpv6'),
		bytesIcmpIpv6: getMetricValue(row, 'bytesIcmpIpv6'),
		bytesOtherIpv6: getMetricValue(row, 'bytesOtherIpv6')
	};
}

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
		const metricSelects = getBaseMetricSelects();
		if (schema === 'v2') {
			metricSelects.push(...getFamilyMetricSelects('ipv4'), ...getFamilyMetricSelects('ipv6'));
		}

		const query = `
			SELECT 
				${bucketStartQuery} as bucketStart,
				${metricSelects.join(',\n\t\t\t\t')}
			FROM ${tableName} 
			WHERE ${sourceColumn} IN (${placeholders(routers)})
			AND ${timeColumn} >= ? 
			AND ${timeColumn} < ?
			GROUP BY bucketStart
			ORDER BY bucketStart
		`;

		const params = [...routers, startDate, endDate];

		const stmt = db.prepare(query);
		const rows = stmt.all(...params) as Record<string, number | null>[];
		const result = rows.map(normalizeRow);
		const availableIpFamilies: NetflowIpFamily[] =
			schema === 'v2' ? ['all', 'ipv4', 'ipv6'] : ['all'];
		return json({ result, availableIpFamilies } satisfies NetflowStatsResponse);
	} catch (error) {
		console.error('Database error:', error);
		return json({ error: 'Database query failed' }, { status: 500 });
	}
};
