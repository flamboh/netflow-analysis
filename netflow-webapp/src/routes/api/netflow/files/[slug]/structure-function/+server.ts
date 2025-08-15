import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { exec } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import Database from 'better-sqlite3';
import { DATABASE_PATH } from '$env/static/private';

const execAsync = promisify(exec);

interface StructureFunctionPoint {
	q: number;
	tauTilde: number;
	sd: number;
}

interface NetflowRecord {
	router: string;
	file_path: string;
}

const DB_PATH = path.join(process.cwd(), DATABASE_PATH);
let db: Database.Database | null = null;

function getDb() {
	if (!db) {
		db = new Database(DB_PATH, { readonly: true });
	}
	return db;
}

async function getNetflowFilePath(slug: string, router: string): Promise<string | null> {
	const filePattern = `nfcapd.${slug}`;
	const query = `
		SELECT file_path FROM netflow_stats
		WHERE file_path LIKE '%' || ? AND router = ?
		LIMIT 1
	`;

	const database = getDb();
	const result = database.prepare(query).get(filePattern, router) as NetflowRecord | undefined;

	return result?.file_path || null;
}

async function runStructureFunctionAnalysis(
	filePath: string,
	timeoutMs: number = 60000
): Promise<StructureFunctionPoint[]> {
	try {
		// Use nfdump to extract IPv4 source addresses and pipe directly to MAAD StructureFunction
		const maadPath = path.join(process.cwd(), '..', 'maad');
		const structureFunctionPath = path.join(maadPath, 'StructureFunction');

		const command = `nfdump -r "${filePath}" 'ipv4' -o 'fmt:%sa' -q | "${structureFunctionPath}" /dev/stdin`;

		console.log(`Executing combined nfdump + StructureFunction command`);

		const { stdout } = await execAsync(command, {
			timeout: timeoutMs,
			maxBuffer: 10 * 1024 * 1024, // 10MB buffer should be sufficient for structure function output
			cwd: maadPath
		});

		// Parse CSV output from StructureFunction
		const lines = stdout.trim().split('\n');
		const header = lines[0]; // q,tauTilde,sd

		if (header !== 'q,tauTilde,sd') {
			throw new Error('Unexpected StructureFunction output format');
		}

		const data: StructureFunctionPoint[] = lines.slice(1).map((line) => {
			const [q, tauTilde, sd] = line.split(',').map(Number);
			return { q, tauTilde, sd };
		});

		console.log(`Structure function analysis complete: ${data.length} data points generated`);
		return data;
	} catch (error) {
		console.error('Error running structure function analysis:', error);
		throw error;
	}
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
		console.log(`Starting structure function analysis for ${router} - ${slug}`);

		// Get the absolute file path for the NetFlow file from the database
		const filePath = await getNetflowFilePath(slug, router);
		if (!filePath) {
			return json(
				{ error: `NetFlow file not found for router ${router} and slug ${slug}` },
				{ status: 404 }
			);
		}

		console.log(`Found NetFlow file: ${filePath}`);

		// Run structure function analysis directly with nfdump piped to MAAD
		const data = await runStructureFunctionAnalysis(filePath);

		if (data.length === 0) {
			return json(
				{ error: 'No data points generated from structure function analysis' },
				{ status: 422 }
			);
		}

		return json({
			slug,
			router,
			filename: `nfcapd.${slug}`,
			structureFunction: data,
			metadata: {
				dataSource: `NetFlow File: ${path.basename(filePath)}`,
				uniqueIPCount: -1, // Indicates real NetFlow data (IP count not tracked in new pipeline)
				pointCount: data.length,
				qRange: {
					min: Math.min(...data.map((d) => d.q)),
					max: Math.max(...data.map((d) => d.q))
				}
			}
		});
	} catch (error) {
		console.error('StructureFunction analysis failed:', error);

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
			if (error.message.includes('StructureFunction')) {
				return json({ error: 'MAAD structure function analysis failed' }, { status: 500 });
			}
		}

		return json({ error: 'Failed to generate structure function analysis' }, { status: 500 });
	}
};
