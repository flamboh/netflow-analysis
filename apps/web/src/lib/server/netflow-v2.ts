import type { IpGranularity } from '$lib/types/types';
import type { StructureFunctionPoint } from '$lib/types/types';
type RawStructureFunctionPoint = {
	q: number;
	tau?: number;
	tauTilde?: number;
	sd?: number;
	s?: number;
};

export const FIVE_MINUTE_GRANULARITY: IpGranularity = '5m';
export type NetflowSchemaVersion = 'v2';

export function assertNetflowV2Database(): void {
	return;
}

export function getNetflowSchemaVersion(): NetflowSchemaVersion {
	return 'v2';
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
