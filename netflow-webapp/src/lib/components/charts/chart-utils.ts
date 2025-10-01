import type { GroupByOption, NetflowDataPoint } from '$lib/components/netflow/types.ts';

export function formatLabels(results: NetflowDataPoint[], groupBy: GroupByOption): string[] {
	switch (groupBy) {
		case 'date':
			return results.map((item) => {
				const year = item.time.slice(0, 4);
				const month = item.time.slice(4, 6);
				const day = item.time.slice(6, 8);
				return `${year}-${month}-${day}`;
			});
		case 'hour':
			return results.map((item) => {
				console.log('item.time', item.time);
				const year = item.time.slice(0, 4);
				const month = item.time.slice(4, 6);
				const day = item.time.slice(6, 8);
				const hour = item.time.slice(9, 11);
				return `${year}-${month}-${day} ${hour}:00`;
			});
		case '30min':
			return results.map((item) => {
				const year = item.time.slice(0, 4);
				const month = item.time.slice(4, 6);
				const day = item.time.slice(6, 8);
				const hour = item.time.slice(9, 11);
				const minute = item.time.slice(12, 14);
				return `${year}-${month}-${day} ${hour}:${minute}`;
			});
		case '5min':
			return results.map((item) => {
				const year = item.time.slice(0, 4);
				const month = item.time.slice(4, 6);
				const day = item.time.slice(6, 8);
				const hour = item.time.slice(9, 11);
				const minute = item.time.slice(12, 14);
				return `${year}-${month}-${day} ${hour}:${minute}`;
			});
		default:
			return [];
	}
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

export function parseClickedLabel(label: string, groupBy: GroupByOption): Date {
	switch (groupBy) {
		case 'date':
			return new Date(label);
		case 'hour':
		case '30min':
		case '5min':
			return new Date(label);
		default:
			return new Date(label);
	}
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
