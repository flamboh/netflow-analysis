import type { D1Database } from '@cloudflare/workers-types';
import type { DatasetSummary } from '$lib/types/types';

type QueryParam = string | number | boolean | null | Uint8Array;

type DatasetRow = {
	id: string;
	label: string;
	defaultStartDate: string;
	discoveryMode: string;
	sortOrder: number;
};

export type PreparedStatement = {
	get<T = unknown>(...params: QueryParam[]): Promise<T | undefined>;
	all<T = unknown>(...params: QueryParam[]): Promise<T[]>;
};

export interface ReadonlyDatasetDb {
	get<T = unknown>(query: string, params?: QueryParam[]): Promise<T | undefined>;
	all<T = unknown>(query: string, params?: QueryParam[]): Promise<T[]>;
	prepare(sql: string): PreparedStatement;
}

let localDbCache: ReadonlyDatasetDb | null = null;

function getEnv(name: string): string | undefined {
	return globalThis.process?.env?.[name]?.trim() || undefined;
}

async function resolveLocalSqlitePath(): Promise<string> {
	const configured = getEnv('LOCAL_SQLITE_PATH') ?? getEnv('DATABASE_PATH');
	if (configured) {
		return configured;
	}

	const { existsSync } = await import('node:fs');
	const path = await import('node:path');
	const candidates = [
		path.resolve(process.cwd(), 'data/ugr16/netflow.sqlite'),
		path.resolve(process.cwd(), '../../data/ugr16/netflow.sqlite')
	];

	const found = candidates.find((candidate) => existsSync(candidate));
	if (!found) {
		throw new Error(
			`Local SQLite database not found. Set LOCAL_SQLITE_PATH or create ${candidates[0]}`
		);
	}

	return found;
}

function makePrepared(db: ReadonlyDatasetDb, query: string): PreparedStatement {
	return {
		get: <T = unknown>(...params: QueryParam[]) => db.get<T>(query, params),
		all: <T = unknown>(...params: QueryParam[]) => db.all<T>(query, params)
	};
}

async function createLocalDb(): Promise<ReadonlyDatasetDb> {
	const localPath = await resolveLocalSqlitePath();
	const [{ drizzle }, betterSqlite3, schema] = await Promise.all([
		import(/* @vite-ignore */ 'drizzle-orm/better-sqlite3'),
		import(/* @vite-ignore */ 'better-sqlite3'),
		import('$lib/server/db/schema')
	]);
	const sqlite = new betterSqlite3.default(localPath);
	const drizzleDb = drizzle(sqlite, { schema });
	const client = drizzleDb.$client;

	client.exec(`
		CREATE TABLE IF NOT EXISTS datasets (
			id TEXT PRIMARY KEY NOT NULL,
			label TEXT NOT NULL,
			default_start_date TEXT NOT NULL,
			source_mode TEXT DEFAULT 'static' NOT NULL,
			discovery_mode TEXT DEFAULT 'static' NOT NULL,
			sort_order INTEGER DEFAULT 0 NOT NULL
		);
		INSERT INTO datasets (
			id,
			label,
			default_start_date,
			source_mode,
			discovery_mode,
			sort_order
		)
		SELECT 'ugr16', 'UGR16', '2016-04-04', 'static', 'static', 0
		WHERE NOT EXISTS (SELECT 1 FROM datasets);
	`);

	const db: ReadonlyDatasetDb = {
		async get<T = unknown>(query: string, params: QueryParam[] = []) {
			return client.prepare(query).get(...params) as T | undefined;
		},
		async all<T = unknown>(query: string, params: QueryParam[] = []) {
			return client.prepare(query).all(...params) as T[];
		},
		prepare(query: string) {
			return makePrepared(db, query);
		}
	};

	return db;
}

function createD1Db(d1: D1Database): ReadonlyDatasetDb {
	const db: ReadonlyDatasetDb = {
		async get<T = unknown>(query: string, params: QueryParam[] = []) {
			const result = await d1
				.prepare(query)
				.bind(...params)
				.all<T>();
			return result.results[0];
		},
		async all<T = unknown>(query: string, params: QueryParam[] = []) {
			const result = await d1
				.prepare(query)
				.bind(...params)
				.all<T>();
			return [...result.results];
		},
		prepare(query: string) {
			return makePrepared(db, query);
		}
	};

	return db;
}

export async function getDatasetDb(platform?: App.Platform): Promise<ReadonlyDatasetDb> {
	if (getEnv('ATLANTIS_DB_DRIVER') !== 'sqlite' && platform?.env.DB) {
		return createD1Db(platform.env.DB);
	}

	localDbCache ??= await createLocalDb();
	return localDbCache;
}

async function listDatasetRows(platform?: App.Platform): Promise<DatasetRow[]> {
	const db = await getDatasetDb(platform);
	return db.all<DatasetRow>(
		`
			SELECT
				id,
				label,
				default_start_date AS defaultStartDate,
				discovery_mode AS discoveryMode,
				sort_order AS sortOrder
			FROM datasets
			ORDER BY sort_order ASC, id ASC
		`
	);
}

export async function listDatasets(platform?: App.Platform): Promise<DatasetRow[]> {
	return listDatasetRows(platform);
}

export async function getDefaultDatasetId(platform?: App.Platform): Promise<string> {
	const configured = getEnv('DEFAULT_DATASET');
	if (configured) {
		return configured;
	}

	const datasets = await listDatasetRows(platform);
	const firstDataset = datasets[0];
	if (!firstDataset) {
		throw new Error('No datasets configured');
	}

	return firstDataset.id;
}

export async function getDatasetConfig(
	datasetId: string,
	platform?: App.Platform
): Promise<DatasetRow> {
	const datasets = await listDatasetRows(platform);
	const dataset = datasets.find((item) => item.id === datasetId);
	if (!dataset) {
		const available = datasets.map((item) => item.id).join(', ');
		throw new Error(`Unknown dataset '${datasetId}'. Available datasets: ${available}`);
	}

	return dataset;
}

export async function getDatasetLabel(datasetId: string, platform?: App.Platform): Promise<string> {
	const dataset = await getDatasetConfig(datasetId, platform);
	return dataset.label.trim() || dataset.id;
}

export async function listDatasetSources(
	datasetId: string,
	platform?: App.Platform
): Promise<string[]> {
	await getDatasetConfig(datasetId, platform);
	const db = await getDatasetDb(platform);
	const rows = await db.all<{ sourceId: string }>(
		`
			SELECT DISTINCT source_id AS sourceId
			FROM netflow_stats_v2
			ORDER BY source_id
		`
	);

	return rows.map((row) => row.sourceId);
}

export async function listDatasetSummaries(platform?: App.Platform): Promise<DatasetSummary[]> {
	const db = await getDatasetDb(platform);
	const defaultDatasetId = await getDefaultDatasetId(platform);
	const datasets = await listDatasetRows(platform);
	const sourceRows = await db.all<{ sourceCount: number }>(
		'SELECT COUNT(DISTINCT source_id) AS sourceCount FROM netflow_stats_v2'
	);
	const sourceCount = sourceRows[0]?.sourceCount ?? 0;

	return datasets.map((dataset) => ({
		datasetId: dataset.id,
		label: dataset.label,
		defaultStartDate: dataset.defaultStartDate,
		discoveryMode: dataset.discoveryMode,
		sourceCount,
		isDefault: dataset.id === defaultDatasetId
	}));
}

export async function getRequestedDataset(url: URL, platform?: App.Platform): Promise<string> {
	const dataset = url.searchParams.get('dataset')?.trim() || (await getDefaultDatasetId(platform));
	await getDatasetConfig(dataset, platform);
	return dataset;
}
