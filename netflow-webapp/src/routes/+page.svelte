<script lang="ts">
import NetflowDashboard from '$lib/components/netflow/NetflowDashboard.svelte';
import IPChart from '$lib/components/charts/IPChart.svelte';
import type { ChartState, GroupByOption, RouterConfig } from '$lib/components/netflow/types.ts';
import type { IpGranularity } from '$lib/types/types';

// Shared date range controlled by NetFlow dashboard
const today = new Date().toJSON().slice(0, 10);
let startDate = '2025-01-01';
let endDate = today;
let selectedGroupBy: GroupByOption = 'date';
let selectedRouters: RouterConfig = {};

const GROUP_BY_TO_IP: Record<GroupByOption, IpGranularity> = {
	date: '1d',
	hour: '1h',
	'30min': '30m',
	'5min': '5m'
};

$: netflowInitialState = {
	startDate,
	endDate,
	groupBy: selectedGroupBy,
	chartType: 'stacked',
	dataOptions: [
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
	]
} satisfies Partial<ChartState>;

$: ipGranularity = GROUP_BY_TO_IP[selectedGroupBy];

const dashboards = [
	{ id: 0, kind: 'netflow' as const },
	{ id: 1, kind: 'ip' as const }
];

function handleDateChange(event: CustomEvent<{ startDate: string; endDate: string }>) {
	startDate = event.detail.startDate;
	endDate = event.detail.endDate;
}

function handleGroupByChange(event: CustomEvent<{ groupBy: GroupByOption }>) {
	selectedGroupBy = event.detail.groupBy;
}

function handleRoutersChange(event: CustomEvent<{ routers: RouterConfig }>) {
	selectedRouters = event.detail.routers;
}
</script>

<svelte:head>
	<title>NetFlow Analysis</title>
	<meta name="description" content="NetFlow analysis and visualization tool" />
</svelte:head>

<div class="min-h-screen bg-gray-100">
	<main class="mx-auto flex max-w-7xl flex-col gap-4 px-4 py-8 sm:px-6 lg:px-8">
		{#each dashboards as chart (chart.id)}
			{#if chart.kind === 'netflow'}
				<NetflowDashboard
					initialState={netflowInitialState}
					on:dateChange={handleDateChange}
					on:groupByChange={handleGroupByChange}
					on:routersChange={handleRoutersChange}
				/>
			{:else if chart.kind === 'ip'}
				<IPChart {startDate} {endDate} granularity={ipGranularity} routers={selectedRouters} />
			{/if}
		{/each}
	</main>
</div>
