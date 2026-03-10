import { error } from '@sveltejs/kit';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, url }) => {
	const { slug } = params;
	const dataset = url.searchParams.get('dataset') || 'uoregon';

	if (!slug || slug.length !== 12 || !/^\d{12}$/.test(slug)) {
		throw error(400, `Invalid slug format. Expected 12 digits (YYYYMMDDHHmm) ${slug}`);
	}

	const year = slug.slice(0, 4);
	const month = slug.slice(4, 6);
	const day = slug.slice(6, 8);
	const hour = slug.slice(8, 10);
	const minute = slug.slice(10, 12);
	const filePattern = `nfcapd.${slug}`;

	return {
		dataset,
		slug,
		fileInfo: {
			year,
			month,
			day,
			hour,
			minute,
			filename: filePattern
		}
	};
};
