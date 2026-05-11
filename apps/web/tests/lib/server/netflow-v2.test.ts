import { describe, expect, it } from 'vitest';
import {
	getBucketStartQuery,
	getNetflowSchemaVersion,
	groupByToGranularity,
	normalizeStructurePoints,
	parseSourceIds,
	parseTimestamp
} from '../../../src/lib/server/netflow-v2';

describe('netflow v2 helpers', () => {
	it('is v2-only', () => {
		expect(getNetflowSchemaVersion()).toBe('v2');
	});

	it('parses request primitives', () => {
		expect(parseSourceIds(' r1, r2 ,, ')).toEqual(['r1', 'r2']);
		expect(parseTimestamp('123')).toBe(123);
		expect(parseTimestamp('not-a-number')).toBeNull();
	});

	it('maps groupings to stored granularities', () => {
		expect(groupByToGranularity('date')).toBe('1d');
		expect(groupByToGranularity('hour')).toBe('1h');
		expect(groupByToGranularity('30min')).toBe('30m');
		expect(groupByToGranularity('5min')).toBe('5m');
	});

	it('keeps raw bucket starts for 5 minute requests', () => {
		expect(getBucketStartQuery('bucket_start', '5min')).toBe('bucket_start');
	});

	it('normalizes structure points from MAAD variants', () => {
		expect(normalizeStructurePoints([{ q: 1, tauTilde: 2, s: 3 }])).toEqual([
			{ q: 1, tau: 2, sd: 3 }
		]);
	});
});
