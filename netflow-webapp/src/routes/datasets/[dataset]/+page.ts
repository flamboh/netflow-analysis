import { error } from '@sveltejs/kit';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch }) => {
	const { dataset } = params;

	if (!dataset || dataset.trim().length === 0) {
		throw error(400, 'Dataset parameter is required');
	}

	const response = await fetch('/api/datasets');
	if (!response.ok) {
		throw error(response.status, 'Failed to load dataset metadata');
	}

	const datasets = (await response.json()) as Array<{
		datasetId: string;
		label: string;
		defaultStartDate: string;
	}>;
	const selectedDataset = datasets.find((entry) => entry.datasetId === dataset);
	if (!selectedDataset) {
		throw error(404, `Unknown dataset '${dataset}'`);
	}

	return {
		dataset,
		title: selectedDataset.label,
		defaultStartDate: selectedDataset.defaultStartDate
	};
};
