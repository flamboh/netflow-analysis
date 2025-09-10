import { json } from '@sveltejs/kit';
import { exec } from 'child_process';
import { promisify } from 'util';
import { getNetflowFilePath } from '../utils';
import type { RequestHandler } from './$types';

const execAsync = promisify(exec);

async function getUniqueIPCount(filePath: string, isSource: boolean): Promise<number> {
	const command = `nfdump -r "${filePath}"  -o 'fmt:${isSource ? '%sa' : '%da'}' -q -s ${isSource ? 'srcip' : 'dstip'} -n 0 | wc -l`;
	const { stdout } = await execAsync(command, {
		maxBuffer: 10 * 1024 * 1024 * 10,
		timeout: 60_000
	});
	const uniqueIPs = parseInt(stdout.trim());
	return uniqueIPs;
}

async function getTotalIPCount(filePath: string, isSource: boolean): Promise<number> {
	const command = `nfdump -r "${filePath}" -o 'fmt:${isSource ? '%sa' : '%da'}' -q -n 0 | wc -l`;
	const { stdout } = await execAsync(command, {
		maxBuffer: 10 * 1024 * 1024 * 10,
		timeout: 60_000
	});
	const totalIPs = parseInt(stdout.trim());
	return totalIPs;
}

export const GET: RequestHandler = async ({ params, url }) => {
	const { slug } = params;
	const router = url.searchParams.get('router');
	const sourceParam = url.searchParams.get('source');

	if (!slug || slug.length !== 12 || !/^\d{12}$/.test(slug)) {
		return json({ error: 'Invalid slug format' }, { status: 400 });
	}

	if (!router) {
		return json({ error: 'Router parameter is required' }, { status: 400 });
	}

	if (sourceParam === null) {
		return json(
			{ error: 'Source parameter is required (true for source addresses, false for destination)' },
			{ status: 400 }
		);
	}

	const isSource = sourceParam === 'true';

	try {
		console.log(
			`Getting unique IP count for ${router} - ${slug} (${isSource ? 'source' : 'destination'})`
		);

		// Get the absolute file path for the NetFlow file from the database
		const filePath = await getNetflowFilePath(slug, router);
		if (!filePath) {
			return json(
				{ error: `NetFlow file not found for router ${router} and slug ${slug}` },
				{ status: 404 }
			);
		}

		console.log(`Found NetFlow file: ${filePath}`);

		const uniqueIPCount = await getUniqueIPCount(filePath, isSource);
		const totalIPCount = await getTotalIPCount(filePath, isSource);

		return json({ uniqueIPCount, totalIPCount });
	} catch (error) {
		console.error(error);
		return json({ error: 'Failed to get unique IP count' }, { status: 500 });
	}
};
