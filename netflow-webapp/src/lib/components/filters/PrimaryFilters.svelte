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

<div class="border-border bg-surface space-y-4 rounded-xl border p-4 shadow-sm">
	<div class="flex items-center justify-between">
		<h2 class="text-text-primary text-lg font-semibold">Global Controls</h2>
		<div class="flex items-center gap-4">
			<label class="text-text-secondary text-sm">
				Granularity
				<select
					class="border-border bg-surface text-text-primary focus:border-cisco-blue focus:ring-cisco-blue ml-2 rounded-lg border px-2 py-1 text-sm focus:ring-1 focus:outline-none"
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
				class="bg-cisco-blue hover:bg-cisco-blue-dark focus:ring-cisco-blue rounded-lg px-4 py-1 text-sm font-medium text-white transition-colors focus:ring-2 focus:outline-none"
			>
				Reset View
			</button>
		</div>
	</div>

	<div class="text-text-secondary text-sm">
		<span class="font-medium">Navigation:</span>
		<span class="ml-1"
			>Click chart to drill down. Drag across chart to drill into a date range.</span
		>
	</div>

	<DateRangeFilter
		startDate={props.startDate}
		endDate={props.endDate}
		onStartDateChange={handleStartDateChange}
		onEndDateChange={handleEndDateChange}
	/>

	<RouterFilter routers={props.routers} onRouterChange={handleRoutersChange} />
</div>
