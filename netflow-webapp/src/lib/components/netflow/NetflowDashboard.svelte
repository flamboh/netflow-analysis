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
		dataOptions: DataOption[];
	}>();

	const dispatch = createEventDispatcher<{
		dateChange: { startDate: string; endDate: string };
		groupByChange: { groupBy: GroupByOption };
		dataOptionsChange: { options: DataOption[] };
	}>();

	let chartType = $state<ChartTypeOption>('stacked');
	let rawResults = $state<NetflowStatsResult[]>([]);
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
	let lastAggregationKey = '';
	let requestToken = 0;

	function deriveKnownRouters(routers: RouterConfig): string[] {
		return Object.keys(routers)
			.map((router) => router.trim())
			.filter((router) => router.length > 0)
			.sort();
	}

	function deriveSelectedRouters(routers: RouterConfig): string[] {
		return Object.entries(routers)
			.filter(([, enabled]) => enabled)
			.map(([router]) => router.trim())
			.filter((router) => router.length > 0)
			.sort();
	}

	function createEmptyBucket(bucketStart: number): NetflowDataPoint {
		return {
			bucketStart,
			flows: 0,
			flowsTcp: 0,
			flowsUdp: 0,
			flowsIcmp: 0,
			flowsOther: 0,
			packets: 0,
			packetsTcp: 0,
			packetsUdp: 0,
			packetsIcmp: 0,
			packetsOther: 0,
			bytes: 0,
			bytesTcp: 0,
			bytesUdp: 0,
			bytesIcmp: 0,
			bytesOther: 0
		};
	}

	function aggregateResults(
		rows: NetflowStatsResult[],
		selectedRouters: string[]
	): NetflowDataPoint[] {
		const selected = new Set(selectedRouters);
		const buckets = new Map<number, NetflowDataPoint>();

		rows.forEach((row) => {
			if (!selected.has(row.router)) {
				return;
			}

			const bucket = buckets.get(row.bucketStart) ?? createEmptyBucket(row.bucketStart);
			bucket.flows += row.flows;
			bucket.flowsTcp += row.flowsTcp;
			bucket.flowsUdp += row.flowsUdp;
			bucket.flowsIcmp += row.flowsIcmp;
			bucket.flowsOther += row.flowsOther;
			bucket.packets += row.packets;
			bucket.packetsTcp += row.packetsTcp;
			bucket.packetsUdp += row.packetsUdp;
			bucket.packetsIcmp += row.packetsIcmp;
			bucket.packetsOther += row.packetsOther;
			bucket.bytes += row.bytes;
			bucket.bytesTcp += row.bytesTcp;
			bucket.bytesUdp += row.bytesUdp;
			bucket.bytesIcmp += row.bytesIcmp;
			bucket.bytesOther += row.bytesOther;
			buckets.set(row.bucketStart, bucket);
		});

		return [...buckets.values()].sort((left, right) => left.bucketStart - right.bucketStart);
	}

	function getCacheKey(filters: FilterInputs, knownRouters: string[]): string {
		return JSON.stringify({
			chart: 'netflow',
			dataset: props.dataset,
			groupBy: filters.groupBy,
			routers: knownRouters
		});
	}

	function getRequestedRange(filters: FilterInputs): TimeRange {
		return {
			start: dateStringToEpochPST(filters.startDate),
			end: dateStringToEpochPST(filters.endDate, true)
		};
	}

	function readCachedResults(cacheKey: string, requestedRange: TimeRange): NetflowStatsResult[] {
		return readCachedWindow<NetflowStatsResult>(cacheKey, requestedRange, (record, range) => {
			return record.bucketStart >= range.start && record.bucketStart < range.end;
		});
	}

	async function loadData(
		filters: FilterInputs,
		token: number,
		knownRouters: string[],
		requestedRange: TimeRange
	) {
		const cacheKey = getCacheKey(filters, knownRouters);
		const needsFetch = getMissingWindowRanges(cacheKey, requestedRange).length > 0;
		loading = needsFetch;
		error = null;

		const params = new URLSearchParams({
			dataset: props.dataset,
			routers: knownRouters.join(','),
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
				getRecordKey: (record) => `${record.router}-${record.bucketStart}`,
				compareRecords: (left, right) =>
					left.bucketStart - right.bucketStart || left.router.localeCompare(right.router)
			});

			if (token !== requestToken) {
				return;
			}
			rawResults = readCachedResults(cacheKey, requestedRange);
		} catch (err) {
			if (token !== requestToken) {
				return;
			}
			error = `Failed to load data: ${err instanceof Error ? err.message : 'Unknown error'}`;
			rawResults = [];
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

		const knownRouters = deriveKnownRouters(filters.routers);
		if (knownRouters.length === 0) {
			return;
		}

		const selectedRouters = deriveSelectedRouters(filters.routers);

		if (selectedRouters.length === 0) {
			error = 'Select at least one router to view NetFlow statistics';
			rawResults = [];
			results = [];
			loading = false;
			return;
		}

		const nextKey = JSON.stringify({
			dataset: props.dataset,
			startDate: filters.startDate,
			endDate: filters.endDate,
			groupBy: filters.groupBy,
			routers: knownRouters
		});

		if (nextKey === lastFiltersKey) {
			return;
		}

		lastFiltersKey = nextKey;
		const requestedRange = getRequestedRange(filters);
		const token = ++requestToken;
		loadData(filters, token, knownRouters, requestedRange);
	});

	$effect(() => {
		const selectedRouters = deriveSelectedRouters(props.routers);
		if (
			selectedRouters.length > 0 &&
			error === 'Select at least one router to view NetFlow statistics'
		) {
			error = null;
		}
		const nextKey = JSON.stringify({
			selectedRouters,
			rawResults: rawResults.map((row) => `${row.router}:${row.bucketStart}`)
		});

		if (nextKey === lastAggregationKey) {
			return;
		}

		lastAggregationKey = nextKey;
		results = aggregateResults(rawResults, selectedRouters);
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
