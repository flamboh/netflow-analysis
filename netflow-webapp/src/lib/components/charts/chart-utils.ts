import type { GroupByOption, NetflowDataPoint } from '$lib/components/netflow/types.ts';
import {
	parseLabelToPSTComponents,
	parseLabelToDateForDrilldown,
	epochToPSTComponents,
	getWeekdayName,
	type PSTDateComponents
} from '$lib/utils/timezone';

/** Fixed y-axis width (px) for consistent chart alignment */
export const Y_AXIS_WIDTH = 80;

/**
 * Format labels from NetFlow data points using PST timezone.
 * item.time is now an epoch timestamp (as string) from the API.
 */
export function formatLabels(results: NetflowDataPoint[], groupBy: GroupByOption): string[] {
	return results.map((item) => {
		// Parse epoch timestamp (item.time is a string containing epoch seconds)
		const epoch = parseInt(item.time, 10);
		if (!Number.isFinite(epoch)) {
			return '';
		}

		// Convert to PST components for consistent display
		const pst = epochToPSTComponents(epoch);
		const year = pst.year;
		const month = String(pst.month).padStart(2, '0');
		const day = String(pst.day).padStart(2, '0');
		const hours = String(pst.hours).padStart(2, '0');
		const minutes = String(pst.minutes).padStart(2, '0');

		switch (groupBy) {
			case 'date':
				return `${year}-${month}-${day}`;
			case 'hour':
				return `${year}-${month}-${day} ${hours}:00`;
			case '30min':
			case '5min':
				return `${year}-${month}-${day} ${hours}:${minutes}`;
			default:
				return `${year}-${month}-${day}`;
		}
	});
}

export function getXAxisTitle(groupBy: GroupByOption): string {
	switch (groupBy) {
		case 'date':
			return 'Date';
		case 'hour':
			return 'Hour';
		case '30min':
			return '30 Minutes';
		case '5min':
			return '5 Minutes';
		default:
			return 'Time';
	}
}

export function formatNumber(value: number): string {
	if (value >= 1e15) return (value / 1e15).toFixed(1) + 'P';
	if (value >= 1e12) return (value / 1e12).toFixed(1) + 'T';
	if (value >= 1e9) return (value / 1e9).toFixed(1) + 'B';
	if (value >= 1e6) return (value / 1e6).toFixed(1) + 'M';
	if (value >= 1e3) return (value / 1e3).toFixed(1) + 'K';
	return value.toString();
}

export function generateColors(count: number): string[] {
	const colors = [
		'#FF6384',
		'#36A2EB',
		'#FFCE56',
		'#4BC0C0',
		'#9966FF',
		'#FF9F40',
		'#FF6384',
		'#C9CBCF',
		'#4BC0C0',
		'#FF6384',
		'#36A2EB',
		'#FFCE56',
		'#4BC0C0',
		'#9966FF',
		'#FF9F40',
		'#FF6384',
		'#C9CBCF',
		'#4BC0C0',
		'#FF6384',
		'#36A2EB'
	];
	return colors.slice(0, count);
}

/**
 * Parse a clicked chart label into a Date for drill-down calculations.
 * Returns a Date that represents the PST moment (for time arithmetic).
 */
export function parseClickedLabel(label: string, _groupBy: GroupByOption): Date {
	const date = parseLabelToDateForDrilldown(label);
	return date ?? new Date(NaN);
}

export function generateSlugFromLabel(label: string, groupBy: GroupByOption): string {
	if (groupBy === '5min') {
		const [datePart, timePart] = label.split(' ');
		const [year, month, day] = datePart.split('-');
		const [hour, minute] = timePart.split(':');
		return `${year}${month}${day}${hour}${minute}`;
	}
	return '';
}

/**
 * Parse a label to PST components for tick formatting.
 * Uses timezone-aware parsing to ensure consistent display regardless of viewer's timezone.
 */
function safePSTComponents(label: string): PSTDateComponents | null {
	return parseLabelToPSTComponents(label);
}

export function formatNetflowTick(
	groupBy: GroupByOption,
	label: string | undefined,
	index: number
): string {
	if (!label) return '';
	const pst = safePSTComponents(label);
	if (!pst) return '';
	
	const weekday = getWeekdayName(pst.dayOfWeek);
	const month = pst.month;
	const day = pst.day;
	const hours = pst.hours;
	const minutes = pst.minutes;

	if (groupBy === 'date') {
		return pst.dayOfWeek === 1 ? `${weekday} ${month}/${day}` : '';
	}

	if (groupBy === 'hour') {
		return hours === 0 ? `${weekday} ${month}/${day}` : '';
	}

	if (groupBy === '30min') {
		if (minutes === 0 && (hours === 0 || hours === 12)) {
			return `${weekday} ${month}/${day} ${hours.toString().padStart(2, '0')}:00`;
		}
		return '';
	}

	if (groupBy === '5min') {
		if (minutes === 0) {
			return `${weekday} ${month}/${day} ${hours.toString().padStart(2, '0')}:00`;
		}
		return '';
	}

	return index === 0 ? `${weekday} ${month}/${day}` : '';
}

export function shouldHighlightNetflowGrid(
	groupBy: GroupByOption,
	label: string | undefined,
	index: number
): boolean {
	if (!label) return index === 0;
	const pst = safePSTComponents(label);
	if (!pst) return index === 0;

	const hours = pst.hours;
	const minutes = pst.minutes;

	if (groupBy === 'date') {
		return pst.dayOfWeek === 1;
	}
	if (groupBy === 'hour') {
		return hours === 0;
	}
	if (groupBy === '30min') {
		return minutes === 0 && (hours === 0 || hours === 12);
	}
	if (groupBy === '5min') {
		return minutes === 0;
	}
	return index === 0;
}
