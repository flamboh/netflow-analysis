import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { listDatasetSummaries } from '$lib/server/datasets';

export const GET: RequestHandler = async () => {
	try {
		return json(listDatasetSummaries());
	} catch (error) {
		console.error('Failed to list datasets:', error);
		return json({ error: 'Failed to list datasets' }, { status: 500 });
	}
};
