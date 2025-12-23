import { json } from '@sveltejs/kit';
import Database from 'better-sqlite3';
import { DATABASE_PATH } from '$env/static/private';
import type { RequestHandler } from './$types';
import { IP_GRANULARITIES, type IpGranularity } from '$lib/types/types';
import type { SpectrumPoint, SpectrumStatsBucket, SpectrumStatsResponse } from '$lib/types/types';

const VALID_GRANULARITIES = new Set<string>(IP_GRANULARITIES);

function parseRouters(param: string | null): string[] {
	if (!param) return [];
	return param
		.split(',')
		.map((router) => router.trim())
		.filter((router) => router.length > 0);
}

function parseGranularity(param: string | null): IpGranularity | null {
	if (!param) return null;
	if (VALID_GRANULARITIES.has(param)) {
		return param as IpGranularity;
	}
	return null;
}

function parseTimestamp(param: string | null): number | null {
	if (!param) return null;
	const value = Number(param);
	return Number.isFinite(value) ? value : null;
}

export const GET: RequestHandler = async ({ url }) => {
	const routers = parseRouters(url.searchParams.get('routers'));
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
		const db = new Database(DATABASE_PATH, { readonly: true });
		const placeholders = routers.map(() => '?').join(',');
		const params = [granularity, ...routers, start, end];

		const query = `
			SELECT
				router,
				bucket_start AS bucketStart,
				spectrum_json_sa AS spectrumJsonSa,
				spectrum_json_da AS spectrumJsonDa
			FROM spectrum_stats
			WHERE granularity = ?
				AND router IN (${placeholders})
				AND bucket_start >= ?
				AND bucket_start < ?
				AND ip_version = 4
			ORDER BY router ASC, bucket_start ASC
		`;

		const stmt = db.prepare(query);
		const rows = stmt.all(...params) as Array<{
			router: string;
			bucketStart: number;
			spectrumJsonSa: string;
			spectrumJsonDa: string;
		}>;
		db.close();

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
