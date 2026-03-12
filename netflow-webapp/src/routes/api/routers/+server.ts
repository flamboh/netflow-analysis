import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { getRequestedDataset, listDatasetSources } from '$lib/server/datasets';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const dataset = getRequestedDataset(url);
		const routers = listDatasetSources(dataset);
		if (routers.length === 0) {
			return json({ error: `No routers available for dataset '${dataset}'` }, { status: 404 });
		}
		return json(routers);
	} catch (error) {
		console.error('Failed to list routers:', error);
		return json({ error: 'No routers available' }, { status: 500 });
	}
};
