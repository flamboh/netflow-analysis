import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { IP_GRANULARITIES } from '$lib/types/types';
import type { IpGranularity, IpStatsBucket, IpStatsResponse } from '$lib/types/types';
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
	const dataset = getRequestedDataset(url);
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
		const db = getDatasetDb(dataset);
		const schema = getNetflowSchemaVersion(db);
		const tableName = schema === 'v2' ? 'ip_stats_v2' : 'ip_stats';
		const sourceColumn = schema === 'v2' ? 'source_id' : 'router';
		const params = [granularity, ...routers, start, end];

		const query = `
			SELECT
				${sourceColumn} AS router,
				bucket_start AS bucketStart,
				bucket_end   AS bucketEnd,
				granularity,
				SUM(sa_ipv4_count) AS saIpv4Count,
				SUM(da_ipv4_count) AS daIpv4Count,
				SUM(sa_ipv6_count) AS saIpv6Count,
				SUM(da_ipv6_count) AS daIpv6Count,
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
		const rows = stmt.all(...params) as IpStatsBucket[];
		const response: IpStatsResponse = {
			buckets: rows.map((row) => ({
				...row,
				granularity
			})),
			availableGranularities: [...IP_GRANULARITIES],
			requestedRouters: routers
		};

		return json(response);
	} catch (error) {
		console.error('Failed to query ip_stats:', error);
		return json({ error: 'Database query failed' }, { status: 500 });
	}
};
