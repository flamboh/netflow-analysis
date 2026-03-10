import { error } from '@sveltejs/kit';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params }) => {
	const { dataset } = params;

	if (!dataset || dataset.trim().length === 0) {
		throw error(400, 'Dataset parameter is required');
	}

	return {
		dataset
	};
};
