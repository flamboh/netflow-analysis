import { error } from '@sveltejs/kit';
import type { PageServerLoad } from './$types';
import { getDatasetLabel } from '$lib/server/datasets';

export const load: PageServerLoad = async ({ params }) => {
	const { dataset } = params;

	if (!dataset || dataset.trim().length === 0) {
		throw error(400, 'Dataset parameter is required');
	}

	try {
		return {
			dataset,
			title: getDatasetLabel(dataset)
		};
	} catch (err) {
		console.error('Failed to load dataset page metadata:', err);
		throw error(404, `Unknown dataset '${dataset}'`);
	}
};
