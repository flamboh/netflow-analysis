import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';

export interface DatasetConfig {
	dataset_id: string;
	label: string;
	root_path: string;
	db_path: string;
	source_mode?: string;
	discovery_mode?: string;
	source_ids?: string[];
}

export interface DatasetSummary {
	datasetId: string;
	label: string;
	discoveryMode: string;
	sourceCount: number;
}

const repoRoot = path.resolve(process.cwd(), '..');
const defaultRegistryPath = path.resolve(repoRoot, 'datasets.json');
const datasetDbCache = new Map<string, Database.Database>();
const preferredDefaultDataset = 'uoregon';
let datasetRegistryCache: {
	registryPath: string;
	mtimeMs: number;
	datasets: DatasetConfig[];
} | null = null;

function resolveRepoPath(value: string): string {
	if (path.isAbsolute(value)) {
		return value;
	}
	return path.resolve(repoRoot, value);
}

function getRegistryPath(): string {
	const configured = process.env.DATASETS_CONFIG_PATH;
	if (!configured) {
		return defaultRegistryPath;
	}
	return path.isAbsolute(configured) ? configured : path.resolve(repoRoot, configured);
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
	return listDatasets().map((dataset) => ({
		datasetId: dataset.dataset_id,
		label: dataset.label,
		discoveryMode: dataset.discovery_mode ?? 'static',
		sourceCount: listDatasetSources(dataset.dataset_id).length
	}));
}

export function getDefaultDatasetId(): string {
	const configured = process.env.DEFAULT_DATASET?.trim();
	if (configured) {
		return configured;
	}

	const datasets = listDatasets();
	const preferred = datasets.find((dataset) => dataset.dataset_id === preferredDefaultDataset);
	return preferred?.dataset_id ?? datasets[0].dataset_id;
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

export function listDatasetSources(datasetId: string): string[] {
	const dataset = getDatasetConfig(datasetId);
	if (dataset.source_ids && dataset.source_ids.length > 0) {
		return [...dataset.source_ids].sort();
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

export function getDatasetDb(datasetId: string): Database.Database {
	const existing = datasetDbCache.get(datasetId);
	if (existing) {
		return existing;
	}

	const dbPath = getDatasetDbPath(datasetId);
	if (!fs.existsSync(dbPath)) {
		throw new Error(`Dataset database not found for '${datasetId}' at ${dbPath}`);
	}

	const db = new Database(dbPath, { readonly: true });
	datasetDbCache.set(datasetId, db);
	return db;
}

export function getRequestedDataset(url: URL): string {
	const dataset = url.searchParams.get('dataset')?.trim() || getDefaultDatasetId();
	getDatasetConfig(dataset);
	return dataset;
}
