import fs from 'fs';
import path from 'path';
import { createRequire } from 'node:module';
import Database from 'better-sqlite3';
import type { DatasetSummary } from '$lib/types/types';
import { getDatasetsConfigPath, getRepoRoot } from '$lib/server/paths';

export interface DatasetConfig {
	dataset_id: string;
	label: string;
	root_path: string;
	db_path: string;
	default_start_date?: string;
	source_mode?: string;
	discovery_mode?: string;
	source_ids?: string[];
}

const repoRoot = getRepoRoot();
const defaultRegistryPath = getDatasetsConfigPath();
const require = createRequire(import.meta.url);

type PreparedStatement = {
	get(...params: unknown[]): unknown;
	all(...params: unknown[]): unknown[];
};

export interface ReadonlyDatasetDb {
	prepare(sql: string): PreparedStatement;
	close(): void;
}

const datasetDbCache = new Map<
	string,
	{ db: ReadonlyDatasetDb; dbPath: string; mtimeMs: number }
>();
const datasetDefaultStartCache = new Map<
	string,
	{ dbPath: string; mtimeMs: number; value: string }
>();
let datasetRegistryCache: {
	registryPath: string;
	mtimeMs: number;
	datasets: DatasetConfig[];
} | null = null;

function formatLocalDate(timestampSeconds: number): string {
	const date = new Date(timestampSeconds * 1000);
	const year = date.getFullYear();
	const month = `${date.getMonth() + 1}`.padStart(2, '0');
	const day = `${date.getDate()}`.padStart(2, '0');
	return `${year}-${month}-${day}`;
}

function getFallbackStartDate(): string {
	const fallback = new Date();
	fallback.setDate(fallback.getDate() - 30);
	return fallback.toISOString().slice(0, 10);
}

function resolveRepoPath(value: string): string {
	if (path.isAbsolute(value)) {
		return value;
	}
	return path.resolve(repoRoot, value);
}

function getRegistryPath(): string {
	return defaultRegistryPath;
}

function readDatasetRegistry(): DatasetConfig[] {
	const registryPath = getRegistryPath();
	if (!fs.existsSync(registryPath)) {
		throw new Error(`Dataset registry not found at ${registryPath}`);
	}

	const stat = fs.statSync(registryPath);
	if (
		datasetRegistryCache &&
		datasetRegistryCache.registryPath === registryPath &&
		datasetRegistryCache.mtimeMs === stat.mtimeMs
	) {
		return datasetRegistryCache.datasets;
	}

	const raw = JSON.parse(fs.readFileSync(registryPath, 'utf-8')) as
		| DatasetConfig[]
		| { datasets: DatasetConfig[] };
	const datasets = Array.isArray(raw) ? raw : raw.datasets;

	if (!Array.isArray(datasets) || datasets.length === 0) {
		throw new Error(`Dataset registry at ${registryPath} is empty or invalid`);
	}

	const normalized = datasets.map((dataset) => ({
		...dataset,
		root_path: resolveRepoPath(dataset.root_path),
		db_path: resolveRepoPath(dataset.db_path)
	}));

	datasetRegistryCache = {
		registryPath,
		mtimeMs: stat.mtimeMs,
		datasets: normalized
	};

	return normalized;
}

export function listDatasets(): DatasetConfig[] {
	return readDatasetRegistry();
}

export function listDatasetSummaries(): DatasetSummary[] {
	const defaultDatasetId = getDefaultDatasetId();
	return listDatasets().map((dataset) => ({
		datasetId: dataset.dataset_id,
		label: dataset.label,
		defaultStartDate: getDatasetDefaultStartDate(dataset.dataset_id),
		discoveryMode: dataset.discovery_mode ?? 'static',
		sourceCount: listDatasetSources(dataset.dataset_id).length,
		isDefault: dataset.dataset_id === defaultDatasetId
	}));
}

export function getDefaultDatasetId(): string {
	const configured = process.env.DEFAULT_DATASET?.trim();
	if (configured) {
		return configured;
	}

	const datasets = listDatasets();
	return datasets[0].dataset_id;
}

export function getDatasetConfig(datasetId: string): DatasetConfig {
	const dataset = listDatasets().find((item) => item.dataset_id === datasetId);
	if (!dataset) {
		const available = listDatasets()
			.map((item) => item.dataset_id)
			.join(', ');
		throw new Error(`Unknown dataset '${datasetId}'. Available datasets: ${available}`);
	}
	return dataset;
}

export function getDatasetLabel(datasetId: string): string {
	const dataset = getDatasetConfig(datasetId);
	return dataset.label?.trim() || dataset.dataset_id;
}

export function getDatasetDefaultStartDate(datasetId: string): string {
	const dataset = getDatasetConfig(datasetId);
	const configuredDefaultStartDate = dataset.default_start_date?.trim();
	if (configuredDefaultStartDate) {
		return configuredDefaultStartDate;
	}

	const dbPath = getDatasetDbPath(datasetId);
	if (!fs.existsSync(dbPath)) {
		return getFallbackStartDate();
	}

	const stat = fs.statSync(dbPath);
	const cached = datasetDefaultStartCache.get(datasetId);
	if (cached && cached.dbPath === dbPath && cached.mtimeMs === stat.mtimeMs) {
		return cached.value;
	}

	try {
		const db = getDatasetDb(datasetId);
		let row: { minTimestamp: number | null } | undefined;
		try {
			row = db.prepare('SELECT MIN(bucket_start) AS minTimestamp FROM netflow_stats_v2').get() as
				| { minTimestamp: number | null }
				| undefined;
		} catch {
			row = db.prepare('SELECT MIN(timestamp) AS minTimestamp FROM netflow_stats').get() as
				| { minTimestamp: number | null }
				| undefined;
		}
		if (typeof row?.minTimestamp === 'number' && Number.isFinite(row.minTimestamp)) {
			const value = formatLocalDate(row.minTimestamp);
			datasetDefaultStartCache.set(datasetId, { dbPath, mtimeMs: stat.mtimeMs, value });
			return value;
		}
	} catch (error) {
		console.error(`Failed to derive default start date for dataset '${datasetId}':`, error);
	}

	return getFallbackStartDate();
}

export function listDatasetSources(datasetId: string): string[] {
	const dataset = getDatasetConfig(datasetId);
	if (dataset.source_ids && dataset.source_ids.length > 0) {
		return [...dataset.source_ids].sort();
	}

	const dbPath = getDatasetDbPath(datasetId);
	if (fs.existsSync(dbPath)) {
		try {
			const db = getDatasetDb(datasetId);
			let rows: { sourceId: string }[];
			try {
				rows = db
					.prepare('SELECT DISTINCT source_id AS sourceId FROM netflow_stats_v2 ORDER BY source_id')
					.all() as { sourceId: string }[];
			} catch {
				rows = db
					.prepare('SELECT DISTINCT router AS sourceId FROM netflow_stats ORDER BY router')
					.all() as { sourceId: string }[];
			}
			if (rows.length > 0) {
				return rows.map((row) => row.sourceId);
			}
		} catch {
			// Dataset metadata can still come from the configured source tree while a DB is being replaced.
		}
	}

	if ((dataset.source_mode ?? 'subdirs') !== 'subdirs') {
		throw new Error(`Unsupported source_mode '${dataset.source_mode}' for dataset '${datasetId}'`);
	}

	if (!fs.existsSync(dataset.root_path)) {
		return [];
	}

	return fs
		.readdirSync(dataset.root_path, { withFileTypes: true })
		.filter((entry) => entry.isDirectory())
		.map((entry) => entry.name)
		.sort();
}

export function getDatasetDbPath(datasetId: string): string {
	return getDatasetConfig(datasetId).db_path;
}

function openNodeSqliteDatabase(dbPath: string): ReadonlyDatasetDb {
	const { DatabaseSync } = require('node:sqlite') as typeof import('node:sqlite');
	type SQLInputValue = import('node:sqlite').SQLInputValue;
	const db = new DatabaseSync(dbPath, { open: true, readOnly: true });
	return {
		prepare(sql: string): PreparedStatement {
			const stmt = db.prepare(sql);
			return {
				get: (...params: unknown[]) => stmt.get(...(params as SQLInputValue[])),
				all: (...params: unknown[]) => stmt.all(...(params as SQLInputValue[]))
			};
		},
		close() {
			db.close();
		}
	};
}

export function getDatasetDb(datasetId: string): ReadonlyDatasetDb {
	const dbPath = getDatasetDbPath(datasetId);
	if (!fs.existsSync(dbPath)) {
		throw new Error(`Dataset database not found for '${datasetId}' at ${dbPath}`);
	}

	const stat = fs.statSync(dbPath);
	const existing = datasetDbCache.get(datasetId);
	if (existing && existing.dbPath === dbPath && existing.mtimeMs === stat.mtimeMs) {
		return existing.db;
	}

	if (existing) {
		try {
			existing.db.close();
		} catch (error) {
			console.error(`Failed to close cached database for '${datasetId}':`, error);
		}
	}

	let db: ReadonlyDatasetDb;
	try {
		db = new Database(dbPath, { readonly: true });
	} catch (error) {
		if (error instanceof Error && 'code' in error && error.code === 'ERR_DLOPEN_FAILED') {
			console.warn(`better-sqlite3 failed to load for '${datasetId}', falling back to node:sqlite`);
			db = openNodeSqliteDatabase(dbPath);
		} else {
			throw error;
		}
	}

	datasetDbCache.set(datasetId, { db, dbPath, mtimeMs: stat.mtimeMs });
	return db;
}

export function getRequestedDataset(url: URL): string {
	const dataset = url.searchParams.get('dataset')?.trim() || getDefaultDatasetId();
	getDatasetConfig(dataset);
	return dataset;
}
