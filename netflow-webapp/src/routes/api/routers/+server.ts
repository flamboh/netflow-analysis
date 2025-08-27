import { AVAILABLE_ROUTERS } from '$env/static/private';
import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	const routers = AVAILABLE_ROUTERS.split(',');
	if (routers.length === 0) {
		return json({ error: 'No routers available' }, { status: 500 });
	}
	return json(routers);
};
