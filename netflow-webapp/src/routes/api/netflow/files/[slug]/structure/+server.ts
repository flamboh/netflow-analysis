import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import type { StructureFunctionPoint } from '$lib/types/types';
import { getDb, slugToBucketStart } from '../utils';

const FIVE_MINUTES = '5m';

type StructureRow = {
	structureJsonSa: string;
	structureJsonDa: string;
};

type StructureResponse = {
	slug: string;
	router: string;
	filename: string;
	structureFunction: StructureFunctionPoint[];
	metadata: {
		dataSource: string;
		uniqueIPCount?: number;
		pointCount: number;
		addressType: string;
		qRange: {
			min: number;
			max: number;
		};
	};
};

export const GET: RequestHandler = async ({ params, url }) => {
	const { slug } = params;
	const router = url.searchParams.get('router');
	const sourceParam = url.searchParams.get('source');

	if (!slug || slug.length !== 12 || !/^\d{12}$/.test(slug)) {
		return json({ error: 'Invalid slug format' }, { status: 400 });
	}

	if (!router) {
		return json({ error: 'Router parameter is required' }, { status: 400 });
	}

	if (sourceParam === null) {
		return json(
			{ error: 'Source parameter is required (true for source addresses, false for destination)' },
			{ status: 400 }
		);
	}

	const isSource = sourceParam === 'true';
	const bucketStart = slugToBucketStart(slug);

	if (bucketStart === null) {
		return json({ error: 'Unable to parse slug timestamp' }, { status: 400 });
	}

	try {
		const db = getDb();
		const row = db
			.prepare(
				`SELECT
					structure_json_sa AS structureJsonSa,
					structure_json_da AS structureJsonDa
				FROM structure_stats
				WHERE router = ?
					AND granularity = ?
					AND bucket_start = ?
					AND ip_version = 4
				LIMIT 1`
			)
			.get(router, FIVE_MINUTES, bucketStart) as StructureRow | undefined;

		if (!row) {
			return json(
				{ error: `Structure statistics not found for router ${router} at ${slug}` },
				{ status: 404 }
			);
		}

		const rawStructure = isSource ? row.structureJsonSa : row.structureJsonDa;
		let data: StructureFunctionPoint[] = [];

		try {
			if (rawStructure) {
				data = JSON.parse(rawStructure) as StructureFunctionPoint[];
			}
		} catch (error) {
			console.error('Failed to parse structure JSON from database:', error);
			return json({ error: 'Failed to parse structure statistics' }, { status: 500 });
		}

		const addressType = isSource ? 'Source' : 'Destination';
		const qValues = data.map((point) => point.q);
		const qRange =
			qValues.length > 0
				? { min: Math.min(...qValues), max: Math.max(...qValues) }
				: { min: 0, max: 0 };

		const response: StructureResponse = {
			slug,
			router,
			filename: `nfcapd.${slug}`,
			structureFunction: data,
			metadata: {
				dataSource: `Database: structure_stats 5m bucket (${addressType} Addresses)`,
				uniqueIPCount: -1,
				pointCount: data.length,
				addressType: addressType,
				qRange
			}
		};

		return json(response);
	} catch (error) {
		console.error('Failed to fetch structure statistics from database:', error);
		return json({ error: 'Failed to get structure statistics' }, { status: 500 });
	}
};
