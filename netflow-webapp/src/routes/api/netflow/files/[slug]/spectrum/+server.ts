import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { exec } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import { getNetflowFilePath } from '../utils';

const execAsync = promisify(exec);

interface SpectrumPoint {
	alpha: number;
	f: number;
}

async function runSpectrumAnalysis(
	filePath: string,
	isSource: boolean,
	timeoutMs: number = 60000
): Promise<SpectrumPoint[]> {
	try {
		// Use nfdump to extract IPv4 addresses (source or destination) and pipe directly to MAAD Spectrum
		const maadPath = path.join(process.cwd(), '..', 'maad');
		const spectrumPath = path.join(maadPath, 'Spectrum');

		// Use %sa for source addresses, %da for destination addresses
		const addressFormat = isSource ? '%sa' : '%da';
		const command = `nfdump -r "${filePath}" 'ipv4' -o 'fmt:${addressFormat}' -q | "${spectrumPath}" /dev/stdin`;

		console.log(
			`Executing combined nfdump + Spectrum command for ${isSource ? 'source' : 'destination'} addresses`
		);

		const { stdout } = await execAsync(command, {
			timeout: timeoutMs,
			maxBuffer: 10 * 1024 * 1024, // 10MB buffer should be sufficient for spectrum output
			cwd: maadPath
		});

		// Parse CSV output from Spectrum
		const lines = stdout.trim().split('\n');
		const header = lines[0]; // alpha,f

		if (header !== 'alpha,f') {
			throw new Error('Unexpected Spectrum output format');
		}

		const data: SpectrumPoint[] = lines.slice(1).map((line) => {
			const [alpha, f] = line.split(',').map(Number);
			return { alpha, f };
		});

		console.log(
			`Spectrum analysis complete: ${data.length} data points generated for ${isSource ? 'source' : 'destination'} addresses`
		);
		return data;
	} catch (error) {
		console.error(
			`Error running spectrum analysis for ${isSource ? 'source' : 'destination'} addresses:`,
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

	const isSource = sourceParam === 'true';

	try {
		console.log(
			`Starting spectrum analysis for ${router} - ${slug} (${isSource ? 'source' : 'destination'} addresses)`
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

		// Run spectrum analysis directly with nfdump piped to MAAD
		const data = await runSpectrumAnalysis(filePath, isSource);

		if (data.length === 0) {
			return json({ error: 'No data points generated from spectrum analysis' }, { status: 422 });
		}

		const addressType = isSource ? 'Source' : 'Destination';

		return json({
			slug,
			router,
			filename: `nfcapd.${slug}`,
			spectrum: data,
			metadata: {
				dataSource: `NetFlow File: ${path.basename(filePath)} (${addressType} Addresses)`,
				uniqueIPCount: -1, // Indicates real NetFlow data (IP count not tracked in new pipeline)
				pointCount: data.length,
				addressType: addressType,
				alphaRange: {
					min: Math.min(...data.map((d) => d.alpha)),
					max: Math.max(...data.map((d) => d.alpha))
				}
			}
		});
	} catch (error) {
		console.error('Spectrum analysis failed:', error);

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
			if (error.message.includes('Spectrum')) {
				return json({ error: 'MAAD spectrum analysis failed' }, { status: 500 });
			}
		}

		return json({ error: 'Failed to generate spectrum analysis' }, { status: 500 });
	}
};
