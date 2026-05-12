import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { IP_GRANULARITIES, type IpGranularity } from '$lib/types/types';
import type { SpectrumPoint, SpectrumStatsBucket, SpectrumStatsResponse } from '$lib/types/types';
import { getDatasetDb, getRequestedDataset } from '$lib/server/datasets';
import { parseSourceIds, parseTimestamp, placeholders } from '$lib/server/netflow-v2';

const VALID_GRANULARITIES = new Set<string>(IP_GRANULARITIES);

function parseGranularity(param: string | null): IpGranularity | null {
	if (!param) return null;
	if (VALID_GRANULARITIES.has(param)) {
		return param as IpGranularity;
	}
	return null;
}

export const GET: RequestHandler = async ({ url, platform }) => {
	const dataset = await getRequestedDataset(url, platform);
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
		const db = await getDatasetDb(dataset, platform);
		const tableName = 'spectrum_stats_v2';
		const sourceColumn = 'source_id';
		const params = [granularity, ...routers, start, end];

		const query = `
			SELECT
				${sourceColumn} AS router,
				bucket_start AS bucketStart,
				spectrum_json_sa AS spectrumJsonSa,
				spectrum_json_da AS spectrumJsonDa
			FROM ${tableName}
			WHERE granularity = ?
				AND ${sourceColumn} IN (${placeholders(routers)})
				AND bucket_start >= ?
				AND bucket_start < ?
				AND ip_version = 4
			ORDER BY ${sourceColumn} ASC, bucket_start ASC
		`;

		const rows = await db.all<{
			router: string;
			bucketStart: number;
			spectrumJsonSa: string;
			spectrumJsonDa: string;
		}>(query, params);
		const buckets: SpectrumStatsBucket[] = rows.map((row) => {
			let spectrumSa: SpectrumPoint[] = [];
			let spectrumDa: SpectrumPoint[] = [];

			try {
				if (row.spectrumJsonSa) {
					spectrumSa = JSON.parse(row.spectrumJsonSa) as SpectrumPoint[];
				}
			} catch (e) {
				console.error('Failed to parse spectrum_json_sa:', e);
			}

			try {
				if (row.spectrumJsonDa) {
					spectrumDa = JSON.parse(row.spectrumJsonDa) as SpectrumPoint[];
				}
			} catch (e) {
				console.error('Failed to parse spectrum_json_da:', e);
			}

			return {
				bucketStart: row.bucketStart,
				router: row.router,
				spectrumSa,
				spectrumDa
			};
		});

		const response: SpectrumStatsResponse = {
			buckets,
			requestedRouters: routers
		};

		return json(response);
	} catch (error) {
		console.error('Failed to query spectrum_stats:', error);
		return json({ error: 'Database query failed' }, { status: 500 });
	}
};
