import { error } from '@sveltejs/kit';
import type { PageLoad } from './$types';
import { parseDatasetSummariesResponse } from '$lib/datasets';
import type { DatasetSummary } from '$lib/types/types';

export const load: PageLoad = async ({ params, fetch }) => {
	const { dataset } = params;

	if (!dataset || dataset.trim().length === 0) {
		throw error(400, 'Dataset parameter is required');
	}

	const response = await fetch('/api/datasets');
	const payload = parseDatasetSummariesResponse(await response.json());
	if (!response.ok || payload.error || payload.data === null) {
		throw error(response.status, payload.error || 'Failed to load dataset metadata');
	}

	const datasets = payload.data as DatasetSummary[];
	const selectedDataset = datasets.find((entry) => entry.datasetId === dataset);
	if (!selectedDataset) {
		throw error(404, `Unknown dataset '${dataset}'`);
	}

	return {
		datasetId: selectedDataset.datasetId,
		title: selectedDataset.label,
		defaultStartDate: selectedDataset.defaultStartDate
	};
};
