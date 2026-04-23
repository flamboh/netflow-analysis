import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	return json({ error: 'Singularities are not available in pipeline v2' }, { status: 410 });
};
