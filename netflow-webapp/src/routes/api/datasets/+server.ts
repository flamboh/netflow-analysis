import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { listDatasets, listDatasetSources } from '$lib/server/datasets';

export const GET: RequestHandler = async () => {
	try {
		const datasets = listDatasets().map((dataset) => ({
			datasetId: dataset.dataset_id,
			label: dataset.label,
			discoveryMode: dataset.discovery_mode ?? 'static',
			sourceCount: listDatasetSources(dataset.dataset_id).length
		}));
		return json(datasets);
	} catch (error) {
		console.error('Failed to list datasets:', error);
		return json({ error: 'Failed to list datasets' }, { status: 500 });
	}
};
