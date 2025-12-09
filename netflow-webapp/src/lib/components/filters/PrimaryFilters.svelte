<script lang="ts">
	import { createEventDispatcher } from 'svelte';
	import DateRangeFilter from '$lib/components/filters/DateRangeFilter.svelte';
	import RouterFilter from '$lib/components/filters/RouterFilter.svelte';
	import MetricSelector from '$lib/components/filters/MetricSelector.svelte';
	import type { DataOption, GroupByOption, RouterConfig } from '$lib/components/netflow/types.ts';
	import { IP_METRIC_OPTIONS, type IpMetricKey, type ProtocolMetricKey } from '$lib/types/types';

	interface GroupBySelectOption {
		value: GroupByOption;
		label: string;
	}

	const DEFAULT_GROUP_BY_OPTIONS: GroupBySelectOption[] = [
		{ value: 'date', label: 'Day' },
		{ value: 'hour', label: 'Hour' },
		{ value: '30min', label: '30 Minutes' },
		{ value: '5min', label: '5 Minutes' }
	];

	const props = $props<{
		startDate: string;
		endDate: string;
		groupBy: GroupByOption;
		routers: RouterConfig;
		dataOptions: DataOption[];
		ipMetrics: IpMetricKey[];
		protocolMetrics: ProtocolMetricKey[];
		groupByOptions?: GroupBySelectOption[];
	}>();

	const dispatch = createEventDispatcher<{
		startDateChange: { startDate: string };
		endDateChange: { endDate: string };
		groupByChange: { groupBy: GroupByOption };
		routersChange: { routers: RouterConfig };
		dataOptionsChange: { options: DataOption[] };
		ipMetricsChange: { metrics: IpMetricKey[] };
		protocolMetricsChange: { metrics: ProtocolMetricKey[] };
	}>();

	function handleStartDateChange(date: string) {
		dispatch('startDateChange', { startDate: date });
	}

	function handleEndDateChange(date: string) {
		dispatch('endDateChange', { endDate: date });
	}

	function handleGroupBySelect(event: Event) {
		const select = event.currentTarget as HTMLSelectElement;
		const next = select.value as GroupByOption;
		if (next === props.groupBy) {
			return;
		}
		dispatch('groupByChange', { groupBy: next });
	}

	function handleRoutersChange(nextRouters: RouterConfig) {
		dispatch('routersChange', { routers: nextRouters });
	}

	function handleDataOptionsChange(nextOptions: DataOption[]) {
		dispatch('dataOptionsChange', { options: nextOptions });
	}

	function handleIpMetricToggle(metric: IpMetricKey) {
		const current = props.ipMetrics;
		const isActive = current.includes(metric);
		const metrics = isActive
			? current.filter((item: IpMetricKey) => item !== metric)
			: [...current, metric];
		dispatch('ipMetricsChange', { metrics });
	}

	function handleProtocolMetricToggle(metric: ProtocolMetricKey) {
		const current = props.protocolMetrics;
		const isActive = current.includes(metric);
		const metrics = isActive
			? current.filter((item: ProtocolMetricKey) => item !== metric)
			: [...current, metric];
		dispatch('protocolMetricsChange', { metrics });
	}
</script>

<div class="space-y-4 rounded-lg border bg-white p-4 shadow-sm">
	<div class="flex items-center justify-between">
		<h2 class="text-lg font-semibold text-gray-900">Filters</h2>
		<label class="text-sm text-gray-600">
			Granularity
			<select
				class="ml-2 rounded border border-gray-300 bg-white px-2 py-1 text-sm text-gray-700 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
				value={props.groupBy}
				onchange={handleGroupBySelect}
				aria-label="Select aggregation granularity"
			>
				{#each props.groupByOptions ?? DEFAULT_GROUP_BY_OPTIONS as option (option.value)}
					<option value={option.value}>{option.label}</option>
				{/each}
			</select>
		</label>
	</div>

	<DateRangeFilter
		startDate={props.startDate}
		endDate={props.endDate}
		onStartDateChange={handleStartDateChange}
		onEndDateChange={handleEndDateChange}
	/>

	<RouterFilter routers={props.routers} onRouterChange={handleRoutersChange} />

	<div class="space-y-2">
		<h3 class="text-base font-semibold text-gray-900">NetFlow Metrics</h3>
		<MetricSelector dataOptions={props.dataOptions} onDataOptionsChange={handleDataOptionsChange} />
	</div>

	<div class="space-y-2">
		<h3 class="text-base font-semibold text-gray-900">IP Metrics</h3>
		<div class="flex flex-wrap items-center gap-4">
			{#each IP_METRIC_OPTIONS as option (option.key)}
				<label class="flex cursor-pointer items-center gap-2">
					<input
						type="checkbox"
						checked={props.ipMetrics.includes(option.key)}
						onchange={() => handleIpMetricToggle(option.key)}
						class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
					/>
					<span class="text-sm text-gray-700">{option.label}</span>
				</label>
			{/each}
		</div>
	</div>

	<div class="space-y-2">
		<h3 class="text-base font-semibold text-gray-900">Protocol Metrics</h3>
		<div class="flex flex-wrap items-center gap-4">
			{#each ['uniqueProtocolsIpv4', 'uniqueProtocolsIpv6'] as const as key (key)}
				<label class="flex cursor-pointer items-center gap-2">
					<input
						type="checkbox"
						checked={props.protocolMetrics.includes(key)}
						onchange={() => handleProtocolMetricToggle(key)}
						class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
					/>
					<span class="text-sm text-gray-700">
						{key === 'uniqueProtocolsIpv4' ? 'Unique Protocols IPv4' : 'Unique Protocols IPv6'}
					</span>
				</label>
			{/each}
		</div>
	</div>
</div>
