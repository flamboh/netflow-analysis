import { DATABASE_PATH } from '$env/static/private';
import Database from 'better-sqlite3';

export interface NetflowRecord {
	router: string;
	file_path: string;
}

const DB_PATH = DATABASE_PATH;
let db: Database.Database | null = null;

export function getDb() {
	if (!db) {
		db = new Database(DB_PATH, { readonly: true });
	}
	return db;
}

export async function getNetflowFilePath(slug: string, router: string): Promise<string | null> {
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
