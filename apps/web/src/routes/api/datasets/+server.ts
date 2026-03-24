import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { listDatasetSummaries } from '$lib/server/datasets';
import type { DatasetSummariesResponse } from '$lib/types/types';

export const GET: RequestHandler = async () => {
	try {
		const response: DatasetSummariesResponse = {
			data: listDatasetSummaries(),
			error: null
		};
		return json(response);
	} catch (error) {
		console.error('Failed to list datasets:', error);
		const response: DatasetSummariesResponse = {
			data: null,
			error: 'Failed to list datasets'
		};
		return json(response, { status: 500 });
	}
};
