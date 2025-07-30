import { error } from '@sveltejs/kit';
import type { PageServerLoad } from './$types';
import Database from 'better-sqlite3';
import path from 'path';

interface NetflowRecord {
	router: string;
	file_path: string;
	flows: number;
	flows_tcp: number;
	flows_udp: number;
	flows_icmp: number;
	flows_other: number;
	packets: number;
	packets_tcp: number;
	packets_udp: number;
	packets_icmp: number;
	packets_other: number;
	bytes: number;
	bytes_tcp: number;
	bytes_udp: number;
	bytes_icmp: number;
	bytes_other: number;
	first_timestamp: number;
	last_timestamp: number;
	msec_first: number;
	msec_last: number;
	sequence_failures: number;
	processed_at: string;
}

const DB_PATH = path.join(process.cwd(), '..', 'netflow-db', 'flowStats.db');
let db: Database.Database | null = null;

function getDb() {
	if (!db) {
		db = new Database(DB_PATH, { readonly: true });
	}
	return db;
}

export const load: PageServerLoad = async ({ params }) => {
	const { slug } = params;

	if (!slug || slug.length !== 12 || !/^\d{12}$/.test(slug)) {
		throw error(400, 'Invalid slug format. Expected 12 digits (YYYYMMDDHHmm)');
	}

	const year = slug.slice(0, 4);
	const month = slug.slice(4, 6);
	const day = slug.slice(6, 8);
	const hour = slug.slice(8, 10);
	const minute = slug.slice(10, 12);

	// Use exact file path match for better index usage
	const filePattern = `nfcapd.${slug}`;
	const query = `
		SELECT * FROM netflow_stats
		WHERE file_path LIKE '%' || ?
		ORDER BY router
	`;

	const database = getDb();
	const results = database.prepare(query).all(filePattern) as NetflowRecord[];

	if (results.length === 0) {
		throw error(404, `No data found for nfcapd file: ${filePattern}`);
	}

	return {
		slug,
		fileInfo: {
			year,
			month,
			day,
			hour,
			minute,
			filename: filePattern
		},
		summary: results
	};
};
