<script lang="ts">
	import { onMount } from 'svelte';
	import PrimaryFilters from '$lib/components/filters/PrimaryFilters.svelte';
	import NetflowDashboard from '$lib/components/netflow/NetflowDashboard.svelte';
	import IPChart from '$lib/components/charts/IPChart.svelte';
	import { DEFAULT_DATA_OPTIONS } from '$lib/components/netflow/constants';
	import type { DataOption, GroupByOption, RouterConfig } from '$lib/components/netflow/types.ts';
	import { IP_METRIC_OPTIONS, type IpGranularity, type IpMetricKey } from '$lib/types/types';

	const DEFAULT_START_DATE = '2025-02-11';
	const today = new Date().toJSON().slice(0, 10);

	let startDate = $state(DEFAULT_START_DATE);
	let endDate = $state(today);
	let selectedGroupBy = $state<GroupByOption>('date');
	let selectedRouters = $state<RouterConfig>({});
	let dataOptions = $state<DataOption[]>(DEFAULT_DATA_OPTIONS.map((option) => ({ ...option })));
	const defaultIpMetrics: IpMetricKey[] = IP_METRIC_OPTIONS.slice(0, 2).map((option) => option.key);
	let ipMetrics = $state<IpMetricKey[]>([...defaultIpMetrics]);

	const GROUP_BY_TO_IP: Record<GroupByOption, IpGranularity> = {
		date: '1d',
		hour: '1h',
		'30min': '30m',
		'5min': '5m'
	};

	type AppState = {
		startDate: string;
		endDate: string;
		groupBy: GroupByOption;
		routers: RouterConfig;
		dataOptions: DataOption[];
		ipMetrics: IpMetricKey[];
	};

	let isRestoringState = $state(false);

	const ipGranularity = $derived(GROUP_BY_TO_IP[selectedGroupBy]);

	function cloneRouters(config: RouterConfig): RouterConfig {
		const next: RouterConfig = {};
		Object.entries(config).forEach(([router, enabled]) => {
			next[router] = enabled;
		});
		return next;
	}

	function cloneDataOptions(options: DataOption[]): DataOption[] {
		return options.map((option) => ({ ...option }));
	}

	function getCurrentState(): AppState {
		return {
			startDate,
			endDate,
			groupBy: selectedGroupBy,
			routers: cloneRouters(selectedRouters),
			dataOptions: cloneDataOptions(dataOptions),
			ipMetrics: [...ipMetrics]
		};
	}

	function pushHistoryEntry() {
		if (typeof window === 'undefined') {
			return;
		}
		const snapshot = getCurrentState();
		const url = `${window.location.pathname}${window.location.search}${window.location.hash}`;
		window.history.pushState(snapshot, '', url);
	}

	function handleDrilldownStart() {
		pushHistoryEntry();
	}

	onMount(async () => {
		try {
			const response = await fetch('/api/routers');
			if (!response.ok) {
				throw new Error(`Failed to load routers: ${response.statusText}`);
			}

			const routerList = (await response.json()) as string[];
			if (routerList.length === 0) {
				return;
			}

			const routerConfig: RouterConfig = {};
			routerList.forEach((router) => {
				routerConfig[router] = true;
			});
			selectedRouters = routerConfig;
		} catch (err) {
			console.error(err);
		}
	});

	onMount(() => {
		if (typeof window === 'undefined') {
			return;
		}

		const handlePopState = (event: PopStateEvent) => {
			const state = event.state as AppState | null;
			if (!state) {
				return;
			}
			isRestoringState = true;
			startDate = state.startDate;
			endDate = state.endDate;
			selectedGroupBy = state.groupBy;
			selectedRouters = cloneRouters(state.routers);
			dataOptions = cloneDataOptions(state.dataOptions);
			ipMetrics = [...state.ipMetrics];
			isRestoringState = false;
		};

		const url = `${window.location.pathname}${window.location.search}${window.location.hash}`;
		window.history.replaceState(getCurrentState(), '', url);
		window.addEventListener('popstate', handlePopState);

		return () => {
			window.removeEventListener('popstate', handlePopState);
		};
	});

	$effect(() => {
		if (typeof window === 'undefined' || isRestoringState) {
			return;
		}
		const snapshot = getCurrentState();
		const url = `${window.location.pathname}${window.location.search}${window.location.hash}`;
		window.history.replaceState(snapshot, '', url);
	});

	function handleStartDateChange(event: CustomEvent<{ startDate: string }>) {
		startDate = event.detail.startDate;
	}

	function handleEndDateChange(event: CustomEvent<{ endDate: string }>) {
		endDate = event.detail.endDate;
	}

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

	function handleDataOptionsChange(event: CustomEvent<{ options: DataOption[] }>) {
		dataOptions = event.detail.options;
	}

	function handleIpMetricsChange(event: CustomEvent<{ metrics: IpMetricKey[] }>) {
		ipMetrics = event.detail.metrics;
	}
</script>

<svelte:head>
	<title>NetFlow Analysis</title>
	<meta name="description" content="NetFlow analysis and visualization tool" />
</svelte:head>

<div class="min-h-screen bg-gray-100">
	<main class="mx-auto flex max-w-[90vw] flex-col gap-2 px-4 py-8 sm:px-2 lg:px-4">
		<PrimaryFilters
			{startDate}
			{endDate}
			groupBy={selectedGroupBy}
			routers={selectedRouters}
			{dataOptions}
			{ipMetrics}
			on:startDateChange={handleStartDateChange}
			on:endDateChange={handleEndDateChange}
			on:groupByChange={handleGroupByChange}
			on:routersChange={handleRoutersChange}
			on:dataOptionsChange={handleDataOptionsChange}
			on:ipMetricsChange={handleIpMetricsChange}
		/>

	<NetflowDashboard
		{startDate}
		{endDate}
		groupBy={selectedGroupBy}
		routers={selectedRouters}
		{dataOptions}
		onDrilldownStart={handleDrilldownStart}
		on:dateChange={handleDateChange}
		on:groupByChange={handleGroupByChange}
	/>

	<IPChart
			{startDate}
			{endDate}
		granularity={ipGranularity}
		routers={selectedRouters}
		activeMetrics={ipMetrics}
		onDrilldownStart={handleDrilldownStart}
		on:dateChange={handleDateChange}
		on:groupByChange={handleGroupByChange}
	/>
	</main>
</div>
