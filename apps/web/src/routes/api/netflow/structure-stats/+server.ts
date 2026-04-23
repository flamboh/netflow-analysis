import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { IP_GRANULARITIES, type IpGranularity } from '$lib/types/types';
import type {
	StructureFunctionPoint,
	StructureStatsBucket,
	StructureStatsResponse
} from '$lib/types/types';
import { getDatasetDb, getRequestedDataset } from '$lib/server/datasets';
import {
	getNetflowSchemaVersion,
	normalizeStructurePoints,
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
		const tableName = schema === 'v2' ? 'structure_stats_v2' : 'structure_stats';
		const sourceColumn = schema === 'v2' ? 'source_id' : 'router';
		const params = [granularity, ...routers, start, end];

		const query = `
			SELECT
				${sourceColumn} AS router,
				bucket_start AS bucketStart,
				structure_json_sa AS structureJsonSa,
				structure_json_da AS structureJsonDa
			FROM ${tableName}
			WHERE granularity = ?
				AND ${sourceColumn} IN (${placeholders(routers)})
				AND bucket_start >= ?
				AND bucket_start < ?
				AND ip_version = 4
			ORDER BY ${sourceColumn} ASC, bucket_start ASC
		`;

		const stmt = db.prepare(query);
		const rows = stmt.all(...params) as Array<{
			router: string;
			bucketStart: number;
			structureJsonSa: string;
			structureJsonDa: string;
		}>;
		const buckets: StructureStatsBucket[] = rows.map((row) => {
			let structureSa: StructureFunctionPoint[] = [];
			let structureDa: StructureFunctionPoint[] = [];

			try {
				if (row.structureJsonSa) {
					structureSa = normalizeStructurePoints(
						JSON.parse(row.structureJsonSa) as StructureFunctionPoint[]
					);
				}
			} catch (e) {
				console.error('Failed to parse structure_json_sa:', e);
			}

			try {
				if (row.structureJsonDa) {
					structureDa = normalizeStructurePoints(
						JSON.parse(row.structureJsonDa) as StructureFunctionPoint[]
					);
				}
			} catch (e) {
				console.error('Failed to parse structure_json_da:', e);
			}

			return {
				bucketStart: row.bucketStart,
				router: row.router,
				structureSa,
				structureDa
			};
		});

		const response: StructureStatsResponse = {
			buckets,
			requestedRouters: routers
		};

		return json(response);
	} catch (error) {
		console.error('Failed to query structure_stats:', error);
		return json({ error: 'Database query failed' }, { status: 500 });
	}
};
