import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { exec } from 'child_process';
import { promisify } from 'util';
import path from 'path';

const execAsync = promisify(exec);

interface StructureFunctionPoint {
	q: number;
	tauTilde: number;
	sd: number;
}

export const GET: RequestHandler = async ({ params, url }) => {
	const { slug } = params;
	const router = url.searchParams.get('router');

	if (!slug || slug.length !== 12 || !/^\d{12}$/.test(slug)) {
		return json({ error: 'Invalid slug format' }, { status: 400 });
	}

	if (!router) {
		return json({ error: 'Router parameter is required' }, { status: 400 });
	}

	try {
		// For now, we use different dummy data for each router to simulate unique analysis
		// In the future, this would extract IP addresses from the actual netflow file for this specific router
		const maadPath = path.join(process.cwd(), '..', 'maad');

		// Use different test data based on router to simulate unique results
		const testDataFile = router === 'cc-ir1-gw' ? 'caida_100k.csv' : 'simple.csv';
		const testDataPath = path.join(maadPath, 'test_data', testDataFile);
		const structureFunctionPath = path.join(maadPath, 'StructureFunction');

		// Execute the MAAD StructureFunction analysis
		const { stdout } = await execAsync(
			`cd "${maadPath}" && "${structureFunctionPath}" "${testDataPath}"`
		);

		// Parse CSV output
		const lines = stdout.trim().split('\n');
		const header = lines[0]; // q,tauTilde,sd

		if (header !== 'q,tauTilde,sd') {
			throw new Error('Unexpected StructureFunction output format');
		}

		const data: StructureFunctionPoint[] = lines.slice(1).map((line) => {
			const [q, tauTilde, sd] = line.split(',').map(Number);
			return { q, tauTilde, sd };
		});

		return json({
			slug,
			router,
			filename: `nfcapd.${slug}`,
			structureFunction: data,
			metadata: {
				dataSource: testDataFile, // Indicates which dummy data file we're using
				pointCount: data.length,
				qRange: {
					min: Math.min(...data.map((d) => d.q)),
					max: Math.max(...data.map((d) => d.q))
				}
			}
		});
	} catch (error) {
		console.error('StructureFunction analysis failed:', error);
		return json({ error: 'Failed to generate structure function analysis' }, { status: 500 });
	}
};
