<script lang="ts">
	import { goto } from '$app/navigation';
	import ChartContainer from '$lib/components/charts/ChartContainer.svelte';
	import ChartControls from '$lib/components/charts/ChartControls.svelte';
	import DateRangeFilter from '$lib/components/filters/DateRangeFilter.svelte';
	import RouterFilter from '$lib/components/filters/RouterFilter.svelte';
	import MetricSelector from '$lib/components/filters/MetricSelector.svelte';
	import type { ChartState, NetflowDataPoint, GroupByOption, ChartTypeOption, DataOption, RouterConfig } from './types.ts';

	interface Props {
		initialState?: Partial<ChartState>;
	}

	let { initialState = {} }: Props = $props();

	// Initialize default state
	const today = new Date().toJSON().slice(0, 10);
	let startDate = $state(initialState.startDate || '2024-03-01');
	let endDate = $state(initialState.endDate || today);
	let routers = $state(initialState.routers || {
		'cc-ir1-gw': true,
		'oh-ir1-gw': true
	});
	let groupBy = $state<GroupByOption>(initialState.groupBy || 'date');
	let chartType = $state<ChartTypeOption>(initialState.chartType || 'stacked');
	let dataOptions = $state<DataOption[]>(initialState.dataOptions || [
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
	]);

	let results = $state<NetflowDataPoint[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	function dataOptionsToBinary(): number {
		return dataOptions.reduce(
			(acc, curr) => acc + (curr.checked ? 1 : 0) * Math.pow(2, curr.index),
			0
		);
	}

	async function loadData() {
		loading = true;
		error = null;

		try {
			const response = await fetch(
				'/api/netflow/stats?startDate=' +
					Math.floor(new Date(startDate).getTime() / 1000) +
					'&endDate=' +
					Math.floor(new Date(endDate).getTime() / 1000) +
					'&routers=' +
					(routers['cc-ir1-gw'] ? 'cc-ir1-gw' : '') +
					',' +
					(routers['oh-ir1-gw'] ? 'oh-ir1-gw' : '') +
					'&dataOptions=' +
					dataOptionsToBinary() +
					'&groupBy=' +
					groupBy,
				{
					method: 'GET',
					headers: {
						'Content-Type': 'application/json'
					}
				}
			);

			if (response.ok) {
				const res = await response.json();
				results = res.result;
			} else {
				error = `Failed to load data: ${response.status} ${response.statusText}`;
			}
		} catch (err) {
			error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`;
		} finally {
			loading = false;
		}
	}

	function handleDrillDown(newGroupBy: GroupByOption, newStartDate: string, newEndDate: string) {
		groupBy = newGroupBy;
		startDate = newStartDate;
		endDate = newEndDate;
	}

	function handleNavigateToFile(slug: string) {
		goto(`/nfcapd/${slug}`);
	}

	function handleReset() {
		groupBy = 'date';
		startDate = '2024-03-01';
		endDate = today;
	}

	function handleStartDateChange(date: string) {
		startDate = date;
	}

	function handleEndDateChange(date: string) {
		endDate = date;
	}

	function handleRouterChange(newRouters: RouterConfig) {
		routers = newRouters;
	}

	function handleGroupByChange(newGroupBy: GroupByOption) {
		groupBy = newGroupBy;
	}

	function handleChartTypeChange(newChartType: ChartTypeOption) {
		chartType = newChartType;
	}

	function handleDataOptionsChange(newDataOptions: DataOption[]) {
		dataOptions = newDataOptions;
	}

	// Load data when state changes
	$effect(() => {
		loadData();
	});
</script>

<div class="netflow-dashboard space-y-6">
	<div class="filters-section space-y-4 p-4 bg-white rounded-lg shadow-sm border">
		<h2 class="text-lg font-semibold text-gray-900">Filters</h2>
		
		<DateRangeFilter 
			{startDate} 
			{endDate} 
			onStartDateChange={handleStartDateChange}
			onEndDateChange={handleEndDateChange}
		/>

		<RouterFilter 
			{routers} 
			onRouterChange={handleRouterChange}
		/>

		<MetricSelector 
			{dataOptions} 
			onDataOptionsChange={handleDataOptionsChange}
		/>
	</div>

	<div class="chart-section bg-white rounded-lg shadow-sm border">
		<div class="p-4 border-b">
			<h2 class="text-lg font-semibold text-gray-900">Network Flow Visualization</h2>
		</div>

		<div class="p-4">
			<ChartControls 
				{groupBy} 
				{chartType}
				onGroupByChange={handleGroupByChange}
				onChartTypeChange={handleChartTypeChange}
				onReset={handleReset}
			/>

			{#if loading}
				<div class="flex items-center justify-center h-96">
					<div class="text-gray-500">Loading data...</div>
				</div>
			{:else if error}
				<div class="flex items-center justify-center h-96">
					<div class="text-red-500">Error: {error}</div>
				</div>
			{:else if results.length === 0}
				<div class="flex items-center justify-center h-96">
					<div class="text-gray-500">No data available for the selected filters</div>
				</div>
			{:else}
				<ChartContainer 
					{results} 
					{groupBy} 
					{chartType}
					onDrillDown={handleDrillDown}
					onNavigateToFile={handleNavigateToFile}
				/>
			{/if}
		</div>
	</div>
</div>