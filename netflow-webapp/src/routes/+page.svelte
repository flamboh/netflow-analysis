<script lang="ts">
	import { onMount } from 'svelte';
	import PrimaryFilters from '$lib/components/filters/PrimaryFilters.svelte';
	import NetflowDashboard from '$lib/components/netflow/NetflowDashboard.svelte';
	import IPChart from '$lib/components/charts/IPChart.svelte';
	import ProtocolChart from '$lib/components/charts/ProtocolChart.svelte';
	import SpectrumStatsChart from '$lib/components/charts/SpectrumStatsChart.svelte';
	import { DEFAULT_DATA_OPTIONS } from '$lib/components/netflow/constants';
	import type { DataOption, GroupByOption, RouterConfig } from '$lib/components/netflow/types.ts';
	import {
		IP_METRIC_OPTIONS,
		type IpGranularity,
		type IpMetricKey,
		type ProtocolMetricKey
	} from '$lib/types/types';
	import { watch } from 'runed';
	import { useSearchParams } from 'runed/kit';
	import { dateRangeSearchSchema } from '$lib/schemas';

	const params = useSearchParams(dateRangeSearchSchema, { noScroll: true });
	let startDate = $state(params.startDate);
	let endDate = $state(params.endDate);
	let selectedGroupBy = $state<GroupByOption>(params.groupBy as GroupByOption);
	let selectedRouters = $state<RouterConfig>({});
	let selectedSpectrumRouter = $state('');
	let selectedSpectrumAddressType = $state<'sa' | 'da'>('sa');
	let dataOptions = $state<DataOption[]>(DEFAULT_DATA_OPTIONS.map((option) => ({ ...option })));
	const defaultIpMetrics: IpMetricKey[] = IP_METRIC_OPTIONS.slice(0, 2).map((option) => option.key);
	let ipMetrics = $state<IpMetricKey[]>([...defaultIpMetrics]);
	let protocolMetrics = $state<ProtocolMetricKey[]>(['uniqueProtocolsIpv4', 'uniqueProtocolsIpv6']);

	const GROUP_BY_TO_IP: Record<GroupByOption, IpGranularity> = {
		date: '1d',
		hour: '1h',
		'30min': '30m',
		'5min': '5m'
	};

	const ipGranularity = $derived(GROUP_BY_TO_IP[selectedGroupBy]);

	function getEnabledRouters(routers: RouterConfig): string[] {
		return Object.entries(routers)
			.filter(([, enabled]) => enabled)
			.map(([router]) => router)
			.sort();
	}

	watch(
		() => params.startDate,
		(next) => {
			if (next !== startDate) {
				startDate = next;
			}
		}
	);

	watch(
		() => params.endDate,
		(next) => {
			if (next !== endDate) {
				endDate = next;
			}
		}
	);

	watch(
		() => params.groupBy,
		(next) => {
			const value = next as GroupByOption;
			if (value !== selectedGroupBy) {
				selectedGroupBy = value;
			}
		}
	);

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
			const enabledRouters = getEnabledRouters(routerConfig);
			if (!selectedSpectrumRouter || !enabledRouters.includes(selectedSpectrumRouter)) {
				selectedSpectrumRouter = enabledRouters[0] ?? '';
			}
		} catch (err) {
			console.error(err);
		}
	});

	function handleStartDateChange(event: CustomEvent<{ startDate: string }>) {
		startDate = event.detail.startDate;
		params.startDate = startDate;
	}

	function handleEndDateChange(event: CustomEvent<{ endDate: string }>) {
		endDate = event.detail.endDate;
		params.endDate = endDate;
	}

	function handleDateChange(event: CustomEvent<{ startDate: string; endDate: string }>) {
		startDate = event.detail.startDate;
		endDate = event.detail.endDate;
		params.update({ startDate, endDate });
	}

	function handleGroupByChange(event: CustomEvent<{ groupBy: GroupByOption }>) {
		if (event.detail.groupBy === params.groupBy) {
			return;
		}
		selectedGroupBy = event.detail.groupBy;
		params.groupBy = selectedGroupBy;
	}

	function handleRoutersChange(event: CustomEvent<{ routers: RouterConfig }>) {
		const nextRouters = event.detail.routers;
		selectedRouters = nextRouters;
		const enabledRouters = getEnabledRouters(nextRouters);
		if (!enabledRouters.includes(selectedSpectrumRouter)) {
			selectedSpectrumRouter = enabledRouters[0] ?? '';
		}
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
			spectrumRouter={selectedSpectrumRouter}
			spectrumAddressType={selectedSpectrumAddressType}
			{dataOptions}
			{ipMetrics}
			{protocolMetrics}
			on:startDateChange={handleStartDateChange}
			on:endDateChange={handleEndDateChange}
			on:groupByChange={handleGroupByChange}
			on:routersChange={handleRoutersChange}
			on:spectrumRouterChange={(event) => {
				selectedSpectrumRouter = event.detail.router;
			}}
			on:spectrumAddressTypeChange={(event) => {
				selectedSpectrumAddressType = event.detail.addressType;
			}}
			on:dataOptionsChange={handleDataOptionsChange}
			on:ipMetricsChange={handleIpMetricsChange}
			on:protocolMetricsChange={(event) => {
				protocolMetrics = event.detail.metrics;
			}}
		/>

		<NetflowDashboard
			{startDate}
			{endDate}
			groupBy={selectedGroupBy}
			routers={selectedRouters}
			{dataOptions}
			on:dateChange={handleDateChange}
			on:groupByChange={handleGroupByChange}
		/>

		<IPChart
			{startDate}
			{endDate}
			granularity={ipGranularity}
			routers={selectedRouters}
			activeMetrics={ipMetrics}
			on:dateChange={handleDateChange}
			on:groupByChange={handleGroupByChange}
		/>

		<ProtocolChart
			{startDate}
			{endDate}
			granularity={ipGranularity}
			routers={selectedRouters}
			activeMetrics={protocolMetrics}
			on:dateChange={handleDateChange}
			on:groupByChange={handleGroupByChange}
		/>

		<SpectrumStatsChart
			{startDate}
			{endDate}
			granularity={ipGranularity}
			router={selectedSpectrumRouter}
			addressType={selectedSpectrumAddressType}
			on:dateChange={handleDateChange}
			on:groupByChange={handleGroupByChange}
		/>
	</main>
</div>
