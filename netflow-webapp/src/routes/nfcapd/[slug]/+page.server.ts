import { error } from '@sveltejs/kit';
import type { PageServerLoad } from './$types';
import Database from 'better-sqlite3';
import path from 'path';

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
	const results = database.prepare(query).all(filePattern);
	
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
