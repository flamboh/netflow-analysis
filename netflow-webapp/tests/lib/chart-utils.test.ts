import { describe, expect, it } from 'vitest';
import {
	clampGroupByToDateRange,
	getMaxAllowedGranularityForDateRange,
	isGranularityAllowedForDateRange
} from '../../src/lib/components/charts/chart-utils';

describe('chart granularity policy', () => {
	it('allows 5 minute granularity for short ranges', () => {
		expect(getMaxAllowedGranularityForDateRange('2026-03-01', '2026-03-03')).toBe('5min');
		expect(isGranularityAllowedForDateRange('5min', '2026-03-01', '2026-03-03')).toBe(true);
	});

	it('disables 5 minute granularity once the range exceeds the adaptive cutoff', () => {
		expect(getMaxAllowedGranularityForDateRange('2026-03-01', '2026-03-05')).toBe('30min');
		expect(isGranularityAllowedForDateRange('5min', '2026-03-01', '2026-03-05')).toBe(false);
		expect(isGranularityAllowedForDateRange('30min', '2026-03-01', '2026-03-05')).toBe(true);
	});

	it('clamps an invalid selection to the finest allowed granularity', () => {
		expect(clampGroupByToDateRange('5min', '2026-03-01', '2026-03-25')).toBe('hour');
		expect(clampGroupByToDateRange('30min', '2026-03-01', '2026-06-15')).toBe('date');
	});
});
