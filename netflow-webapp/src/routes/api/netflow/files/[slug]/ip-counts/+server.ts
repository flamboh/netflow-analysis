import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { getDb } from '../utils';

const FIVE_MINUTES = '5m';

function slugToBucketStart(slug: string): number | null {
	if (slug.length !== 12) {
		return null;
	}
	const year = Number(slug.slice(0, 4));
	const month = Number(slug.slice(4, 6)) - 1;
	const day = Number(slug.slice(6, 8));
	const hour = Number(slug.slice(8, 10));
	const minute = Number(slug.slice(10, 12));

	if (
		Number.isNaN(year) ||
		Number.isNaN(month) ||
		Number.isNaN(day) ||
		Number.isNaN(hour) ||
		Number.isNaN(minute)
	) {
		return null;
	}

	const date = new Date(Date.UTC(year, month, day, hour, minute));
	return Math.floor(date.getTime() / 1000);
}

type IpCountRow = {
	saIpv4Count: number;
	daIpv4Count: number;
	saIpv6Count: number;
	daIpv6Count: number;
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
					sa_ipv4_count AS saIpv4Count,
					da_ipv4_count AS daIpv4Count,
					sa_ipv6_count AS saIpv6Count,
					da_ipv6_count AS daIpv6Count
				FROM ip_stats
				WHERE router = ?
					AND granularity = ?
					AND bucket_start = ?
				LIMIT 1`
			)
			.get(router, FIVE_MINUTES, bucketStart) as IpCountRow | undefined;

		if (!row) {
			return json(
				{ error: `IP statistics not found for router ${router} at ${slug}` },
				{ status: 404 }
			);
		}

		const response = isSource
			? { ipv4Count: row.saIpv4Count, ipv6Count: row.saIpv6Count }
			: { ipv4Count: row.daIpv4Count, ipv6Count: row.daIpv6Count };

		return json(response);
	} catch (error) {
		console.error('Failed to fetch IP counts from database:', error);
		return json({ error: 'Failed to get IP counts' }, { status: 500 });
	}
};
