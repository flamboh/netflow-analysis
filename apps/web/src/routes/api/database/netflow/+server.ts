import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

export const POST: RequestHandler = async () => {
	return json(
		{
			success: false,
			message: 'Database updates run offline and are imported into D1.'
		},
		{ status: 410 }
	);
};
