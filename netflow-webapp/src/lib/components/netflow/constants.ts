import type { DataOption } from '$lib/components/netflow/types.ts';

export const DEFAULT_DATA_OPTIONS: DataOption[] = [
	{ label: 'Flows', index: 0, checked: true },
	{ label: 'Flows TCP', index: 1, checked: true },
	{ label: 'Flows UDP', index: 2, checked: true },
	{ label: 'Flows ICMP', index: 3, checked: true },
	{ label: 'Flows Other', index: 4, checked: true },
	{ label: 'Packets', index: 5, checked: false },
	{ label: 'Packets TCP', index: 6, checked: false },
	{ label: 'Packets UDP', index: 7, checked: false },
	{ label: 'Packets ICMP', index: 8, checked: false },
	{ label: 'Packets Other', index: 9, checked: false },
	{ label: 'Bytes', index: 10, checked: false },
	{ label: 'Bytes TCP', index: 11, checked: false },
	{ label: 'Bytes UDP', index: 12, checked: false },
	{ label: 'Bytes ICMP', index: 13, checked: false },
	{ label: 'Bytes Other', index: 14, checked: false }
];
