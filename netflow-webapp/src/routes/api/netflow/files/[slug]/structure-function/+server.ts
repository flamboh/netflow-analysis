import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { exec } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import { writeFile, unlink } from 'fs/promises';
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

async function extractIPAddressesFromNetflow(
	filePath: string,
	timeoutMs: number = 60000
): Promise<string[]> {
	try {
		// Use nfdump to extract unique source and destination IP addresses
		// Using aggregation to reduce output size and avoid buffer overflow
		const nfdumpCommand = `nfdump -r "${filePath}" -A srcip,dstip -o csv -q`;

		console.log(`Executing nfdump command: ${nfdumpCommand}`);

		const { stdout } = await execAsync(nfdumpCommand, {
			timeout: timeoutMs,
			maxBuffer: 100 * 1024 * 1024 // 100MB buffer for large files
		});

		// Parse the CSV output to extract unique IP addresses
		const ipSet = new Set<string>();
		const lines = stdout.trim().split('\n');

		// Skip header line
		for (let i = 1; i < lines.length; i++) {
			const line = lines[i].trim();
			if (line) {
				// CSV format: "srcip","dstip","flows","packets","bytes"
				const columns = line.split(',');
				if (columns.length >= 2) {
					const srcIP = columns[0].replace(/"/g, '').trim();
					const dstIP = columns[1].replace(/"/g, '').trim();

					if (srcIP && isValidIP(srcIP)) {
						ipSet.add(srcIP);
					}
					if (dstIP && isValidIP(dstIP)) {
						ipSet.add(dstIP);
					}
				}
			}
		}

		console.log(`Extracted ${ipSet.size} unique IP addresses from aggregated nfdump output`);
		return Array.from(ipSet);
	} catch (error) {
		console.error('Error extracting IP addresses from NetFlow file:', error);

		// If aggregation fails, try a more basic approach with sampling
		if (error instanceof Error && error.message.includes('maxBuffer')) {
			console.log('Trying fallback approach with sampling...');
			return await extractIPAddressesWithSampling(filePath, timeoutMs);
		}

		throw error;
	}
}

async function extractIPAddressesWithSampling(
	filePath: string,
	timeoutMs: number
): Promise<string[]> {
	try {
		// Sample every 10th record to reduce output size
		const nfdumpCommand = `nfdump -r "${filePath}" -o 'fmt:%sa,%da' -c 10000 -q`;

		console.log(`Executing fallback nfdump command with sampling: ${nfdumpCommand}`);

		const { stdout } = await execAsync(nfdumpCommand, {
			timeout: timeoutMs,
			maxBuffer: 50 * 1024 * 1024 // 50MB buffer
		});

		// Parse the output to extract unique IP addresses
		const ipSet = new Set<string>();
		const lines = stdout.trim().split('\n');

		for (const line of lines) {
			if (line.trim()) {
				const [srcIP, dstIP] = line.split(',').map((ip) => ip.trim());
				if (srcIP && isValidIP(srcIP)) {
					ipSet.add(srcIP);
				}
				if (dstIP && isValidIP(dstIP)) {
					ipSet.add(dstIP);
				}
			}
		}

		console.log(`Extracted ${ipSet.size} unique IP addresses from sampled nfdump output`);
		return Array.from(ipSet);
	} catch (error) {
		console.error('Error in fallback IP extraction:', error);
		throw error;
	}
}

function isValidIP(ip: string): boolean {
	const ipRegex = /^(\d{1,3}\.){3}\d{1,3}$/;
	if (!ipRegex.test(ip)) return false;

	const parts = ip.split('.');
	return parts.every((part) => {
		const num = parseInt(part, 10);
		return num >= 0 && num <= 255;
	});
}

async function createTempIPFile(
	ipAddresses: string[],
	slug: string,
	router: string
): Promise<string> {
	const tempFileName = `netflow_ips_${router}_${slug}_${Date.now()}.csv`;
	const tempFilePath = path.join('/tmp', tempFileName);

	// Write IP addresses to temp file, one per line
	const content = ipAddresses.join('\n') + '\n';
	await writeFile(tempFilePath, content, 'utf8');

	console.log(`Created temporary IP file: ${tempFilePath} with ${ipAddresses.length} addresses`);
	return tempFilePath;
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

	let tempFilePath: string | null = null;

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

		// Extract IP addresses from the NetFlow file using nfdump
		const ipAddresses = await extractIPAddressesFromNetflow(filePath);

		if (ipAddresses.length === 0) {
			return json({ error: 'No IP addresses found in NetFlow file' }, { status: 422 });
		}

		console.log(`Extracted ${ipAddresses.length} unique IP addresses`);

		// Create temporary file with IP addresses for MAAD analysis
		tempFilePath = await createTempIPFile(ipAddresses, slug, router);

		// Execute the MAAD StructureFunction analysis
		const maadPath = path.join(process.cwd(), '..', 'maad');
		const structureFunctionPath = path.join(maadPath, 'StructureFunction');

		console.log(`Executing MAAD StructureFunction analysis`);

		const { stdout } = await execAsync(
			`cd "${maadPath}" && "${structureFunctionPath}" "${tempFilePath}"`,
			{ timeout: 60000 } // 60 second timeout for analysis
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

		console.log(`Structure function analysis complete: ${data.length} data points generated`);

		return json({
			slug,
			router,
			filename: `nfcapd.${slug}`,
			structureFunction: data,
			metadata: {
				dataSource: `NetFlow File: ${path.basename(filePath)}`,
				uniqueIPCount: ipAddresses.length,
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
	} finally {
		// Clean up temporary file
		if (tempFilePath) {
			try {
				await unlink(tempFilePath);
				console.log(`Cleaned up temporary file: ${tempFilePath}`);
			} catch (cleanupError) {
				console.warn(`Failed to clean up temporary file ${tempFilePath}:`, cleanupError);
			}
		}
	}
};
