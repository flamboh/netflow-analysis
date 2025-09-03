import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { exec } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import { getNetflowFilePath } from '../utils';

const execAsync = promisify(exec);

interface Singularity {
	rank: string;
	address: string;
	alpha: number;
	intercept: number;
	r2: number;
	nPls: number;
}

async function runSingularitiesAnalysis(
	filePath: string,
	isSource: boolean,
	timeoutMs: number = 60000
): Promise<Singularity[]> {
	try {
		// Use nfdump to extract IPv4 addresses (source or destination) and pipe directly to MAAD Singularities
		const maadPath = path.join(process.cwd(), '..', 'maad');
		const singularitiesPath = path.join(maadPath, 'Singularities');

		// Use %sa for source addresses, %da for destination addresses
		const addressFormat = isSource ? '%sa' : '%da';
		const topN = 10; // Get top 10 singularities
		const command = `nfdump -r "${filePath}" 'ipv4' -o 'fmt:${addressFormat}' -q | "${singularitiesPath}" ${topN} /dev/stdin`;

		console.log(
			`Executing combined nfdump + Singularities command for ${isSource ? 'source' : 'destination'} addresses`
		);

		const { stdout } = await execAsync(command, {
			timeout: timeoutMs,
			maxBuffer: 10 * 1024 * 1024, // 10MB buffer should be sufficient for singularities output
			cwd: maadPath
		});

		// Parse labeled output from Singularities (format: rank:address,alpha,intercept,r2,nPls)
		const lines = stdout
			.trim()
			.split('\n')
			.filter((line) => line.length > 0);

		const data: Singularity[] = lines.map((line) => {
			const [rankPart, dataPart] = line.split(':');
			const [address, alpha, intercept, r2, nPls] = dataPart.split(',');
			return {
				rank: rankPart,
				address,
				alpha: parseFloat(alpha),
				intercept: parseFloat(intercept),
				r2: parseFloat(r2),
				nPls: parseInt(nPls)
			};
		});

		console.log(
			`Singularities analysis complete: ${data.length} data points generated for ${isSource ? 'source' : 'destination'} addresses`
		);
		return data;
	} catch (error) {
		console.error(
			`Error running singularities analysis for ${isSource ? 'source' : 'destination'} addresses:`,
			error
		);
		throw error;
	}
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

	if (sourceParam !== 'true' && sourceParam !== 'false') {
		return json({ error: 'Source parameter must be "true" or "false"' }, { status: 400 });
	}

	const isSource = sourceParam === 'true';

	try {
		console.log(
			`Starting singularities analysis for ${router} - ${slug} (${isSource ? 'source' : 'destination'} addresses)`
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

		// Run singularities analysis directly with nfdump piped to MAAD
		const data = await runSingularitiesAnalysis(filePath, isSource);

		if (data.length === 0) {
			return json(
				{ error: 'No data points generated from singularities analysis' },
				{ status: 422 }
			);
		}

		const addressType = isSource ? 'Source' : 'Destination';

		return json({
			slug,
			router,
			filename: `nfcapd.${slug}`,
			singularities: data,
			metadata: {
				dataSource: `NetFlow File: ${path.basename(filePath)} (${addressType} Addresses)`,
				uniqueIPCount: -1, // Indicates real NetFlow data (IP count not tracked in new pipeline)
				pointCount: data.length,
				addressType: addressType
			}
		});
	} catch (error) {
		console.error('Singularities analysis failed:', error);

		// Provide more specific error messages based on error type
		if (error instanceof Error) {
			if (error.message.includes('timeout')) {
				return json(
					{ error: 'Analysis timed out. NetFlow file may be too large.' },
					{ status: 408 }
				);
			}
			if (
				error.message.includes('maxBuffer') ||
				error.message.includes('ERR_CHILD_PROCESS_STDIO_MAXBUFFER')
			) {
				return json(
					{ error: 'NetFlow file too large for processing. Try a smaller time window.' },
					{ status: 413 }
				);
			}
			if (error.message.includes('nfdump')) {
				return json({ error: 'Failed to process NetFlow file with nfdump' }, { status: 422 });
			}
			if (error.message.includes('Singularities')) {
				return json({ error: 'MAAD singularities analysis failed' }, { status: 500 });
			}
		}

		return json({ error: 'Failed to generate singularities analysis' }, { status: 500 });
	}
};
