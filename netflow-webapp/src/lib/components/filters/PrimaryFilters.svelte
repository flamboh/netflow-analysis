<script lang="ts">
	import { createEventDispatcher } from 'svelte';
	import DateRangeFilter from '$lib/components/filters/DateRangeFilter.svelte';
	import RouterFilter from '$lib/components/filters/RouterFilter.svelte';
	import type { GroupByOption, RouterConfig } from '$lib/components/netflow/types.ts';

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
		groupByOptions?: GroupBySelectOption[];
	}>();

	const dispatch = createEventDispatcher<{
		startDateChange: { startDate: string };
		endDateChange: { endDate: string };
		groupByChange: { groupBy: GroupByOption };
		routersChange: { routers: RouterConfig };
		resetView: Record<string, never>;
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

	function handleResetView() {
		dispatch('resetView', {});
	}
</script>

<div class="space-y-4 rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
	<div class="flex items-center justify-between">
		<h2 class="text-sm font-semibold text-gray-900">Global Controls</h2>
		<div class="flex items-center gap-3">
			<label class="text-sm text-gray-500">
				Granularity
				<select
					class="focus:border-cisco-blue focus:ring-cisco-blue/30 ml-2 rounded-md border border-gray-300 bg-white px-2 py-1.5 text-sm text-gray-700 transition-colors focus:ring-1 focus:outline-none"
					value={props.groupBy}
					onchange={handleGroupBySelect}
					aria-label="Select aggregation granularity"
				>
					{#each props.groupByOptions ?? DEFAULT_GROUP_BY_OPTIONS as option (option.value)}
						<option value={option.value}>{option.label}</option>
					{/each}
				</select>
			</label>

			<button
				type="button"
				onclick={handleResetView}
				class="bg-cisco-blue hover:bg-cisco-blue/90 focus:ring-cisco-blue/40 rounded-md px-3 py-1.5 text-sm font-medium text-white transition-colors focus:ring-2 focus:outline-none"
			>
				Reset View
			</button>
		</div>
	</div>

	<p class="text-sm text-gray-500">
		<span class="font-medium text-gray-600">Navigation:</span>
		Click chart to drill down. Drag across chart to drill into a date range.
	</p>

	<DateRangeFilter
		startDate={props.startDate}
		endDate={props.endDate}
		onStartDateChange={handleStartDateChange}
		onEndDateChange={handleEndDateChange}
	/>

	<RouterFilter routers={props.routers} onRouterChange={handleRoutersChange} />
</div>
