<script lang="ts">
	import { createEventDispatcher } from 'svelte';
	import { goto } from '$app/navigation';
	import ChartContainer from '$lib/components/charts/ChartContainer.svelte';
	import ChartControls from '$lib/components/charts/ChartControls.svelte';
	import { dateStringToEpochPST } from '$lib/utils/timezone';
	import type {
		DataOption,
		GroupByOption,
		NetflowDataPoint,
		ChartTypeOption,
		RouterConfig
	} from './types.ts';

	const props = $props<{
		startDate: string;
		endDate: string;
		groupBy: GroupByOption;
		routers: RouterConfig;
		dataOptions: DataOption[];
	}>();

	const dispatch = createEventDispatcher<{
		dateChange: { startDate: string; endDate: string };
		groupByChange: { groupBy: GroupByOption };
	}>();

	const DEFAULT_START_DATE = '2025-02-11';
	const today = new Date().toJSON().slice(0, 10);

	let chartType = $state<ChartTypeOption>('stacked');
	let results = $state<NetflowDataPoint[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	function dataOptionsToBinary(options: DataOption[]): number {
		return options.reduce((acc, curr) => acc + (curr.checked ? 1 : 0) * Math.pow(2, curr.index), 0);
	}

	type FilterInputs = {
		startDate: string;
		endDate: string;
		groupBy: GroupByOption;
		routers: RouterConfig;
		dataOptions: DataOption[];
	};

	let lastFiltersKey = '';
	let requestToken = 0;

	async function loadData(filters: FilterInputs, token: number, activeRouters: string[]) {
		loading = true;
		error = null;

		const params = new URLSearchParams({
			startDate: dateStringToEpochPST(filters.startDate).toString(),
			endDate: dateStringToEpochPST(filters.endDate, true).toString(),
			routers: activeRouters.join(','),
			dataOptions: dataOptionsToBinary(filters.dataOptions).toString(),
			groupBy: filters.groupBy
		});

		try {
			const response = await fetch(`/api/netflow/stats?${params.toString()}`, {
				method: 'GET',
				headers: {
					'Content-Type': 'application/json'
				}
			});

			if (!response.ok) {
				const message = await response.text();
				throw new Error(message || `Failed to load data: ${response.statusText}`);
			}

			const json = await response.json();
			if (token !== requestToken) {
				return;
			}
			results = json.result;
		} catch (err) {
			if (token !== requestToken) {
				return;
			}
			error = `Failed to load data: ${err instanceof Error ? err.message : 'Unknown error'}`;
			results = [];
		} finally {
			if (token === requestToken) {
				loading = false;
			}
		}
	}

	function handleDrillDown(newGroupBy: GroupByOption, newStartDate: string, newEndDate: string) {
		dispatch('groupByChange', { groupBy: newGroupBy });
		dispatch('dateChange', { startDate: newStartDate, endDate: newEndDate });
	}

	function handleNavigateToFile(slug: string) {
		goto(`/api/netflow/files/${slug}`);
	}

	function handleReset() {
		dispatch('groupByChange', { groupBy: 'date' });
		dispatch('dateChange', { startDate: DEFAULT_START_DATE, endDate: today });
	}

	function handleGroupByChange(newGroupBy: GroupByOption) {
		if (newGroupBy === props.groupBy) {
			return;
		}
		dispatch('groupByChange', { groupBy: newGroupBy });
	}

	function handleChartTypeChange(newChartType: ChartTypeOption) {
		chartType = newChartType;
	}

	$effect(() => {
		const filters: FilterInputs = {
			startDate: props.startDate,
			endDate: props.endDate,
			groupBy: props.groupBy,
			routers: props.routers,
			dataOptions: props.dataOptions
		};

		const routerNames = Object.keys(filters.routers);
		if (routerNames.length === 0) {
			return;
		}

		const activeRouters = routerNames.filter((router) => filters.routers[router]);

		if (activeRouters.length === 0) {
			error = 'Select at least one router to view NetFlow statistics';
			results = [];
			loading = false;
			return;
		}

		const normalizedOptions = filters.dataOptions.map((option) => ({
			index: option.index,
			checked: option.checked
		}));

		const nextKey = JSON.stringify({
			startDate: filters.startDate,
			endDate: filters.endDate,
			groupBy: filters.groupBy,
			routers: activeRouters,
			options: normalizedOptions
		});

		if (nextKey === lastFiltersKey) {
			return;
		}

		lastFiltersKey = nextKey;
		const token = ++requestToken;
		loadData(filters, token, activeRouters);
	});
</script>

<div class="surface-card">
	<div class="surface-card-header">
		<div>
			<h2 class="text-lg font-semibold text-slate-900">NetFlow Volume</h2>
			<p class="text-sm text-slate-600">Stacked or line comparison across selected metrics</p>
		</div>
	</div>

	<div class="surface-card-body space-y-4">
		<ChartControls
			groupBy={props.groupBy}
			{chartType}
			onGroupByChange={handleGroupByChange}
			onChartTypeChange={handleChartTypeChange}
			onReset={handleReset}
			showGroupBy={false}
		/>

		<div class="chart-frame">
			{#if loading}
				<div class="status-panel">Loading NetFlow data...</div>
			{:else if error}
				<div class="status-panel status-panel-error">{error}</div>
			{:else if results.length === 0}
				<div class="status-panel">No NetFlow data available for the selected filters.</div>
			{:else}
				<ChartContainer
					{results}
					groupBy={props.groupBy}
					{chartType}
					dataOptions={props.dataOptions}
					onDrillDown={handleDrillDown}
					onNavigateToFile={handleNavigateToFile}
				/>
			{/if}
		</div>
	</div>
</div>
