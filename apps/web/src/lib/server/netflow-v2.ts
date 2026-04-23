import type { IpGranularity } from '$lib/types/types';
import type { StructureFunctionPoint } from '$lib/types/types';
import type { ReadonlyDatasetDb } from '$lib/server/datasets';

type TableInfoRow = {
	name: string;
};

type SqliteMasterRow = {
	name: string;
};

type RawStructureFunctionPoint = {
	q: number;
	tau?: number;
	tauTilde?: number;
	sd?: number;
	s?: number;
};

export const FIVE_MINUTE_GRANULARITY: IpGranularity = '5m';
export type NetflowSchemaVersion = 'v1' | 'v2';

const REQUIRED_COLUMNS: Record<string, string[]> = {
	processed_inputs_v2: [
		'input_kind',
		'input_locator',
		'source_id',
		'bucket_start',
		'bucket_end',
		'status',
		'error_message',
		'discovered_at',
		'processed_at'
	],
	netflow_stats_v2: [
		'source_id',
		'bucket_start',
		'bucket_end',
		'ip_version',
		'flows',
		'flows_tcp',
		'flows_udp',
		'flows_icmp',
		'flows_other',
		'packets',
		'packets_tcp',
		'packets_udp',
		'packets_icmp',
		'packets_other',
		'bytes',
		'bytes_tcp',
		'bytes_udp',
		'bytes_icmp',
		'bytes_other',
		'processed_at'
	],
	ip_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'sa_ipv4_count',
		'da_ipv4_count',
		'sa_ipv6_count',
		'da_ipv6_count',
		'processed_at'
	],
	protocol_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'unique_protocols_count_ipv4',
		'unique_protocols_count_ipv6',
		'protocols_list_ipv4',
		'protocols_list_ipv6',
		'processed_at'
	],
	structure_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'ip_version',
		'structure_json_sa',
		'structure_json_da',
		'metadata_json_sa',
		'metadata_json_da',
		'processed_at'
	],
	spectrum_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'ip_version',
		'spectrum_json_sa',
		'spectrum_json_da',
		'metadata_json_sa',
		'metadata_json_da',
		'processed_at'
	],
	dimension_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'ip_version',
		'dimensions_json_sa',
		'dimensions_json_da',
		'metadata_json_sa',
		'metadata_json_da',
		'processed_at'
	]
};

const verifiedDatabases = new WeakSet<ReadonlyDatasetDb>();
const schemaVersionCache = new WeakMap<ReadonlyDatasetDb, NetflowSchemaVersion>();

function getTableNames(db: ReadonlyDatasetDb): Set<string> {
	const rows = db
		.prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
		.all() as SqliteMasterRow[];
	return new Set(rows.map((row) => row.name));
}

export function assertNetflowV2Database(db: ReadonlyDatasetDb, tables = getTableNames(db)) {
	if (verifiedDatabases.has(db)) {
		return;
	}

	const missingTables = Object.keys(REQUIRED_COLUMNS).filter((tableName) => !tables.has(tableName));

	if (missingTables.length > 0) {
		throw new Error(
			`Dataset database is not pipeline v2. Missing tables: ${missingTables.join(', ')}`
		);
	}

	for (const [tableName, requiredColumns] of Object.entries(REQUIRED_COLUMNS)) {
		const columnRows = db.prepare(`PRAGMA table_info(${tableName})`).all() as TableInfoRow[];
		const columns = new Set(columnRows.map((row) => row.name));
		const missingColumns = requiredColumns.filter((columnName) => !columns.has(columnName));
		if (missingColumns.length > 0) {
			throw new Error(
				`Dataset database table ${tableName} is missing v2 columns: ${missingColumns.join(', ')}`
			);
		}
	}

	verifiedDatabases.add(db);
}

export function getNetflowSchemaVersion(db: ReadonlyDatasetDb): NetflowSchemaVersion {
	const cached = schemaVersionCache.get(db);
	if (cached) {
		return cached;
	}

	const tables = getTableNames(db);
	if (tables.has('netflow_stats_v2')) {
		assertNetflowV2Database(db, tables);
		schemaVersionCache.set(db, 'v2');
		return 'v2';
	}

	if (tables.has('netflow_stats')) {
		schemaVersionCache.set(db, 'v1');
		return 'v1';
	}

	throw new Error('Dataset database has neither netflow_stats_v2 nor netflow_stats');
}

export function parseSourceIds(param: string | null): string[] {
	if (!param) return [];
	return param
		.split(',')
		.map((sourceId) => sourceId.trim())
		.filter((sourceId) => sourceId.length > 0);
}

export function parseTimestamp(param: string | null): number | null {
	if (!param) return null;
	const value = Number(param);
	return Number.isFinite(value) ? value : null;
}

export function placeholders(values: unknown[]): string {
	return values.map(() => '?').join(',');
}

export function groupByToGranularity(groupBy: string): IpGranularity {
	if (groupBy === 'date') return '1d';
	if (groupBy === 'hour') return '1h';
	if (groupBy === '30min') return '30m';
	return FIVE_MINUTE_GRANULARITY;
}

export function getBucketStartQuery(columnName: string, groupBy: string): string {
	const granularity = groupByToGranularity(groupBy);
	if (granularity === '5m') {
		return columnName;
	}

	const bucketSize = granularity === '30m' ? 1800 : granularity === '1h' ? 3600 : 86400;
	return `(CAST(strftime('%s', datetime(${columnName}, 'unixepoch', 'localtime', 'start of day', 'utc', printf('+%d seconds', ((CAST(strftime('%s', datetime(${columnName}, 'unixepoch', 'localtime')) AS integer) - CAST(strftime('%s', datetime(${columnName}, 'unixepoch', 'localtime', 'start of day')) AS integer)) / ${bucketSize}) * ${bucketSize}))) AS integer))`;
}

export function normalizeStructurePoints(
	points: RawStructureFunctionPoint[]
): StructureFunctionPoint[] {
	return points.map((point) => ({
		q: point.q,
		tau: point.tau ?? point.tauTilde ?? 0,
		sd: point.sd ?? point.s ?? 0
	}));
}
