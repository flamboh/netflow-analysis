<script lang="ts">
	import DragGrip from '$lib/components/common/DragGrip.svelte';
	import { createEventDispatcher } from 'svelte';
	import { goto } from '$app/navigation';
	import ChartContainer from '$lib/components/charts/ChartContainer.svelte';
	import MetricSelector from '$lib/components/filters/MetricSelector.svelte';
	import { dateStringToEpochPST } from '$lib/utils/timezone';
	import {
		ensureCachedWindow,
		getMissingWindowRanges,
		readCachedWindow,
		type TimeRange
	} from '$lib/utils/window-cache';
	import type {
		DataOption,
		GroupByOption,
		NetflowDataPoint,
		ChartTypeOption,
		RouterConfig
	} from './types.ts';
	import type { NetflowStatsResult } from '$lib/types/types';

	const props = $props<{
		dataset: string;
		startDate: string;
		endDate: string;
		groupBy: GroupByOption;
		routers: RouterConfig;
		routersLoaded: boolean;
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

	type FilterInputs = {
		startDate: string;
		endDate: string;
		groupBy: GroupByOption;
		routers: RouterConfig;
	};

	let lastFiltersKey = '';
	let requestToken = 0;

	function deriveSelectedRouters(routers: RouterConfig): string[] {
		return Object.entries(routers)
			.filter(([, enabled]) => enabled)
			.map(([router]) => router.trim())
			.filter((router) => router.length > 0)
			.sort();
	}

	function getCacheKey(filters: FilterInputs, selectedRouters: string[]): string {
		return JSON.stringify({
			chart: 'netflow',
			dataset: props.dataset,
			groupBy: filters.groupBy,
			routers: selectedRouters
		});
	}

	function getRequestedRange(filters: FilterInputs): TimeRange {
		return {
			start: dateStringToEpochPST(filters.startDate),
			end: dateStringToEpochPST(filters.endDate, true)
		};
	}

	function readCachedResults(cacheKey: string, requestedRange: TimeRange): NetflowDataPoint[] {
		return readCachedWindow<NetflowStatsResult>(cacheKey, requestedRange, (record, range) => {
			return record.bucketStart >= range.start && record.bucketStart < range.end;
		});
	}

	async function loadData(
		filters: FilterInputs,
		token: number,
		selectedRouters: string[],
		requestedRange: TimeRange
	) {
		const cacheKey = getCacheKey(filters, selectedRouters);
		const needsFetch = getMissingWindowRanges(cacheKey, requestedRange).length > 0;
		loading = needsFetch;
		error = null;

		const params = new URLSearchParams({
			dataset: props.dataset,
			routers: selectedRouters.join(','),
			groupBy: filters.groupBy
		});

		try {
			await ensureCachedWindow<NetflowStatsResult>({
				key: cacheKey,
				requestedRange,
				fetchRange: async (range) => {
					const response = await fetch(
						`/api/netflow/stats?${new URLSearchParams({
							...Object.fromEntries(params.entries()),
							startDate: range.start.toString(),
							endDate: range.end.toString()
						}).toString()}`,
						{
							method: 'GET',
							headers: {
								'Content-Type': 'application/json'
							}
						}
					);

					if (!response.ok) {
						const message = await response.text();
						throw new Error(message || `Failed to load data: ${response.statusText}`);
					}

					const json = await response.json();
					return json.result as NetflowStatsResult[];
				},
				getRecordKey: (record) => `${record.bucketStart}`,
				compareRecords: (left, right) => left.bucketStart - right.bucketStart
			});

			if (token !== requestToken) {
				return;
			}
			results = readCachedResults(cacheKey, requestedRange);
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
		goto(`/netflow/files/${slug}?dataset=${encodeURIComponent(props.dataset)}`);
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
			routers: props.routers
		};

		const selectedRouters = deriveSelectedRouters(filters.routers);

		if (!props.routersLoaded) {
			error = null;
			results = [];
			loading = true;
			return;
		}

		if (selectedRouters.length === 0) {
			error = 'Select at least one router to view NetFlow statistics';
			results = [];
			loading = false;
			return;
		}

		const nextKey = JSON.stringify({
			dataset: props.dataset,
			startDate: filters.startDate,
			endDate: filters.endDate,
			groupBy: filters.groupBy,
			routers: selectedRouters
		});

		if (nextKey === lastFiltersKey) {
			return;
		}

		lastFiltersKey = nextKey;
		const requestedRange = getRequestedRange(filters);
		const token = ++requestToken;
		loadData(filters, token, selectedRouters, requestedRange);
	});
</script>

<div class="rounded-lg border bg-white shadow-sm">
	<div
		class="relative cursor-grab border-b p-4 select-none active:cursor-grabbing"
		draggable="true"
		data-drag-handle
	>
		<h2 class="text-lg font-semibold text-gray-900">Traffic Overview</h2>
		<DragGrip />
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
					<div class="flex items-center gap-3 text-gray-500">
						<div
							class="h-5 w-5 animate-spin rounded-full border-2 border-gray-300 border-t-gray-500"
							aria-hidden="true"
						></div>
						<div>Loading data...</div>
					</div>
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
