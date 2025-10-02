<script lang="ts">
	import { createEventDispatcher, onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import ChartContainer from '$lib/components/charts/ChartContainer.svelte';
	import ChartControls from '$lib/components/charts/ChartControls.svelte';
	import DateRangeFilter from '$lib/components/filters/DateRangeFilter.svelte';
	import RouterFilter from '$lib/components/filters/RouterFilter.svelte';
	import MetricSelector from '$lib/components/filters/MetricSelector.svelte';
	import type {
		ChartState,
		NetflowDataPoint,
		GroupByOption,
		ChartTypeOption,
		DataOption,
		RouterConfig
	} from './types.ts';

	interface Props {
		initialState?: Partial<ChartState>;
	}

	let { initialState = {} }: Props = $props();

const dispatch = createEventDispatcher<{
	dateChange: { startDate: string; endDate: string };
	groupByChange: { groupBy: GroupByOption };
	routersChange: { routers: RouterConfig };
}>();

	// Initialize default state
	const today = new Date().toJSON().slice(0, 10);
	let startDate = $state(initialState.startDate || '2025-01-01');
	let endDate = $state(initialState.endDate || today);
	let routers = $state<RouterConfig>(initialState.routers || {});
	let groupBy = $state<GroupByOption>(initialState.groupBy || 'date');
	let chartType = $state<ChartTypeOption>(initialState.chartType || 'stacked');
	let dataOptions = $state<DataOption[]>(
		initialState.dataOptions || [
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
	);

let results = $state<NetflowDataPoint[]>([]);
let loading = $state(false);
let error = $state<string | null>(null);

const GROUP_BY_OPTIONS: { value: GroupByOption; label: string }[] = [
	{ value: 'date', label: 'Day' },
	{ value: 'hour', label: 'Hour' },
	{ value: '30min', label: '30 Minutes' },
	{ value: '5min', label: '5 Minutes' }
];

function notifyDateChange() {
	dispatch('dateChange', { startDate, endDate });
}

function notifyGroupByChange() {
	dispatch('groupByChange', { groupBy });
}

function notifyRoutersChange() {
	dispatch('routersChange', { routers });
}

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
			const activeRouters = Object.keys(routers)
				.filter((router) => routers[router])
				.join(',');

			const response = await fetch(
				'/api/netflow/stats?startDate=' +
					Math.floor(new Date(startDate).getTime() / 1000) +
					'&endDate=' +
					Math.floor(new Date(endDate).getTime() / 1000) +
					'&routers=' +
					activeRouters +
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
			notifyDateChange();
		}
	}

function handleDrillDown(newGroupBy: GroupByOption, newStartDate: string, newEndDate: string) {
	groupBy = newGroupBy;
	startDate = newStartDate;
	endDate = newEndDate;
	loadData();
	notifyGroupByChange();
}

	function handleNavigateToFile(slug: string) {
		goto(`/api/netflow/files/${slug}`);
	}

function handleReset() {
	groupBy = 'date';
	startDate = '2025-01-01';
	endDate = today;
	loadData();
	notifyGroupByChange();
}

	function handleStartDateChange(date: string) {
		startDate = date;
		loadData();
	}

	function handleEndDateChange(date: string) {
		endDate = date;
		loadData();
	}

	function handleRouterChange(newRouters: RouterConfig) {
		routers = newRouters;
		loadData();
		notifyRoutersChange();
	}

	function handleGranularitySelect(event: Event) {
		const target = event.currentTarget as HTMLSelectElement;
		const next = target.value as GroupByOption;
		if (next === groupBy) {
			return;
		}
		groupBy = next;
		notifyGroupByChange();
		loadData();
	}

	function handleGroupByChange(newGroupBy: GroupByOption) {
		groupBy = newGroupBy;
		loadData();
		notifyGroupByChange();
	}

	function handleChartTypeChange(newChartType: ChartTypeOption) {
		chartType = newChartType;
		// Chart type doesn't require data reload, just chart update
	}

	function handleDataOptionsChange(newDataOptions: DataOption[]) {
		dataOptions = newDataOptions;
		loadData();
	}

	async function loadRouters() {
		try {
			const response = await fetch('/api/routers');
			if (response.ok) {
				const routerList: string[] = await response.json();

				// Initialize routers with all enabled by default
				if (Object.keys(routers).length === 0) {
					const defaultRouters: RouterConfig = {};
					routerList.forEach((router) => {
						defaultRouters[router] = true;
					});
					routers = defaultRouters;
				}
			}
		} catch (err) {
			console.error('Failed to load routers:', err);
		} finally {
			if (Object.keys(routers).length > 0) {
				notifyRoutersChange();
			}
		}
	}

	// Load routers first, then data
	onMount(async () => {
		await loadRouters();
		if (Object.keys(routers).length > 0) {
			loadData();
		}
		notifyGroupByChange();
	});
</script>

<div class="netflow-dashboard space-y-6">
	<div class="filters-section space-y-4 rounded-lg border bg-white p-4 shadow-sm">
		<div class="flex items-center justify-between">
			<h2 class="text-lg font-semibold text-gray-900">Filters</h2>
			<label class="text-sm text-gray-600">
				Granularity
				<select
					class="ml-2 rounded border border-gray-300 bg-white px-2 py-1 text-sm text-gray-700 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
					value={groupBy}
					onchange={handleGranularitySelect}
				>
					{#each GROUP_BY_OPTIONS as option (option.value)}
						<option value={option.value}>{option.label}</option>
					{/each}
				</select>
			</label>
		</div>

		<DateRangeFilter
			{startDate}
			{endDate}
			onStartDateChange={handleStartDateChange}
			onEndDateChange={handleEndDateChange}
		/>

		<RouterFilter {routers} onRouterChange={handleRouterChange} />

		<MetricSelector {dataOptions} onDataOptionsChange={handleDataOptionsChange} />
	</div>

	<div class="chart-section rounded-lg border bg-white shadow-sm">
		<div class="border-b p-4">
			<h2 class="text-lg font-semibold text-gray-900">NetFlow Visualization</h2>
		</div>

		<div class="p-4">
			<ChartControls
				{groupBy}
				{chartType}
				onGroupByChange={handleGroupByChange}
				onChartTypeChange={handleChartTypeChange}
				onReset={handleReset}
				showGroupBy={false}
			/>

			<div
				class="h-[320px] min-h-[240px] resize-y overflow-auto rounded-md border border-gray-200 bg-white/60"
			>
				{#if loading}
					<div class="flex h-full items-center justify-center">
						<div class="text-gray-500">Loading data...</div>
					</div>
				{:else if error}
					<div class="flex h-full items-center justify-center">
						<div class="text-red-500">Error: {error}</div>
					</div>
				{:else if results.length === 0}
					<div class="flex h-full items-center justify-center">
						<div class="text-gray-500">No data available for the selected filters</div>
					</div>
				{:else}
					<ChartContainer
						{results}
						{groupBy}
						{chartType}
						{dataOptions}
						onDrillDown={handleDrillDown}
						onNavigateToFile={handleNavigateToFile}
					/>
				{/if}
			</div>
		</div>
	</div>
</div>
