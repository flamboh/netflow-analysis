import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import {
	IP_GRANULARITIES,
	type IpGranularity,
	type ProtocolStatsBucket,
	type ProtocolStatsResponse
} from '$lib/types/types';
import { getDatasetDb, getRequestedDataset } from '$lib/server/datasets';
import {
	getNetflowSchemaVersion,
	parseSourceIds,
	parseTimestamp,
	placeholders
} from '$lib/server/netflow-v2';

const VALID_GRANULARITIES = new Set<string>(IP_GRANULARITIES);

function parseGranularity(param: string | null): IpGranularity | null {
	if (!param) return null;
	if (VALID_GRANULARITIES.has(param)) {
		return param as IpGranularity;
	}
	return null;
}

export const GET: RequestHandler = async ({ url }) => {
	const routers = parseSourceIds(url.searchParams.get('routers'));
	const granularity =
		parseGranularity(url.searchParams.get('granularity')) ?? (IP_GRANULARITIES[2] as IpGranularity); // default 1h
	const start = parseTimestamp(url.searchParams.get('startDate'));
	const end = parseTimestamp(url.searchParams.get('endDate'));

	if (routers.length === 0) {
		return json({ error: 'No routers selected' }, { status: 400 });
	}

	if (start === null || end === null) {
		return json({ error: 'Invalid start or end time' }, { status: 400 });
	}

	if (start >= end) {
		return json({ error: 'Start time must be before end time' }, { status: 400 });
	}

	try {
		const dataset = getRequestedDataset(url);
		const db = getDatasetDb(dataset);
		const schema = getNetflowSchemaVersion(db);
		const tableName = schema === 'v2' ? 'protocol_stats_v2' : 'protocol_stats';
		const sourceColumn = schema === 'v2' ? 'source_id' : 'router';
		const params = [granularity, ...routers, start, end];

		const query = `
			SELECT
				${sourceColumn} AS router,
				bucket_start AS bucketStart,
				bucket_end   AS bucketEnd,
				granularity,
				SUM(unique_protocols_count_ipv4) AS uniqueProtocolsIpv4,
				SUM(unique_protocols_count_ipv6) AS uniqueProtocolsIpv6,
				MAX(processed_at) AS processedAt
			FROM ${tableName}
			WHERE granularity = ?
				AND ${sourceColumn} IN (${placeholders(routers)})
				AND bucket_start >= ?
				AND bucket_start < ?
			GROUP BY ${sourceColumn}, bucket_start, bucket_end, granularity
			ORDER BY ${sourceColumn} ASC, bucket_start ASC
		`;

		const stmt = db.prepare(query);
		const rows = stmt.all(...params) as ProtocolStatsBucket[];
		const response: ProtocolStatsResponse = {
			buckets: rows.map((row) => ({
				...row,
				granularity
			})),
			availableGranularities: [...IP_GRANULARITIES],
			requestedRouters: routers
		};

		return json(response);
	} catch (error) {
		console.error('Failed to query protocol_stats:', error);
		const message = error instanceof Error ? error.message : 'Database query failed';
		const status = message.startsWith('Unknown dataset') ? 400 : 500;
		return json({ error: message }, { status });
	}
};
