import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import type { SpectrumData, SpectrumPoint } from '$lib/types/types';
import { getDb, slugToBucketStart } from '../utils';

const FIVE_MINUTES = '5m';

type SpectrumRow = {
	spectrumJsonSa: string | null;
	spectrumJsonDa: string | null;
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
					spectrum_json_sa AS spectrumJsonSa,
					spectrum_json_da AS spectrumJsonDa
				FROM spectrum_stats
				WHERE router = ?
					AND granularity = ?
					AND bucket_start = ?
					AND ip_version = 4
				LIMIT 1`
			)
			.get(router, FIVE_MINUTES, bucketStart) as SpectrumRow | undefined;

		if (!row) {
			return json(
				{ error: `Spectrum statistics not found for router ${router} at ${slug}` },
				{ status: 404 }
			);
		}

		const rawSpectrum = isSource ? row.spectrumJsonSa : row.spectrumJsonDa;
		if (!rawSpectrum) {
			return json(
				{ error: `Spectrum statistics not found for router ${router} at ${slug}` },
				{ status: 404 }
			);
		}

		let data: SpectrumPoint[] = [];

		try {
			data = JSON.parse(rawSpectrum) as SpectrumPoint[];
		} catch (error) {
			console.error('Failed to parse spectrum JSON from database:', error);
			return json({ error: 'Failed to parse spectrum statistics' }, { status: 500 });
		}

		if (data.length === 0) {
			return json(
				{ error: `Spectrum statistics not found for router ${router} at ${slug}` },
				{ status: 404 }
			);
		}

		const addressType = isSource ? 'Source' : 'Destination';
		const alphaValues = data.map((point) => point.alpha);
		const alphaRange =
			alphaValues.length > 0
				? { min: Math.min(...alphaValues), max: Math.max(...alphaValues) }
				: { min: 0, max: 0 };

		const response: SpectrumData = {
			slug,
			router,
			filename: `nfcapd.${slug}`,
			spectrum: data,
			metadata: {
				dataSource: `Database: spectrum_stats 5m bucket (${addressType} Addresses)`,
				uniqueIPCount: -1,
				pointCount: data.length,
				addressType: addressType,
				alphaRange
			}
		};

		return json(response);
	} catch (error) {
		console.error('Failed to fetch spectrum statistics from database:', error);
		return json({ error: 'Failed to get spectrum statistics' }, { status: 500 });
	}
};
