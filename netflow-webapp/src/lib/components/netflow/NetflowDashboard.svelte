<script lang="ts">
	import { createEventDispatcher } from 'svelte';
	import { goto } from '$app/navigation';
	import ChartContainer from '$lib/components/charts/ChartContainer.svelte';
	import MetricSelector from '$lib/components/filters/MetricSelector.svelte';
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
		dataOptionsChange: { options: DataOption[] };
	}>();

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

	function handleChartTypeChange(newChartType: ChartTypeOption) {
		chartType = newChartType;
	}

	function handleDataOptionsChange(nextOptions: DataOption[]) {
		dispatch('dataOptionsChange', { options: nextOptions });
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

<div class="rounded-lg border bg-white shadow-sm">
	<div
		class="relative cursor-grab border-b p-4 select-none active:cursor-grabbing"
		draggable="true"
		data-drag-handle
	>
		<h2 class="text-lg font-semibold text-gray-900">NetFlow Visualization</h2>
		<span
			class="pointer-events-none absolute inset-0 flex items-start justify-center pt-1 text-gray-400"
			aria-hidden="true"
		>
			<span class="grid grid-cols-3 grid-rows-2 gap-[2px]">
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
			</span>
		</span>
	</div>

	<div class="space-y-4 p-4">
		<MetricSelector
			dataOptions={props.dataOptions}
			onDataOptionsChange={handleDataOptionsChange}
			{chartType}
			onChartTypeChange={handleChartTypeChange}
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
					<div class="text-red-500">{error}</div>
				</div>
			{:else if results.length === 0}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">No data available for the selected filters</div>
				</div>
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
