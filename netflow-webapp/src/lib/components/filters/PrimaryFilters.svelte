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
		{ value: '30min', label: '30 min' },
		{ value: '5min', label: '5 min' }
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

	function handleRoutersChange(nextRouters: RouterConfig) {
		dispatch('routersChange', { routers: nextRouters });
	}

	function handleResetView() {
		dispatch('resetView', {});
	}

	const navigationTip = 'Click chart to drill down. Drag across chart to drill into a date range.';
	const groupByOptions = $derived(props.groupByOptions ?? DEFAULT_GROUP_BY_OPTIONS);
	const selectedGroupByIndex = $derived(
		Math.max(
			0,
			groupByOptions.findIndex((option: GroupBySelectOption) => option.value === props.groupBy)
		)
	);
</script>

<div class="space-y-3 rounded-lg border bg-white p-3 shadow-sm">
	<div class="flex items-center gap-2">
		<h2 class="text-sm font-semibold text-gray-900">Controls</h2>
		<div class="group relative">
			<button
				type="button"
				class="inline-flex h-5 w-5 items-center justify-center rounded-full border border-gray-300 text-[11px] font-semibold text-gray-500 hover:border-blue-500 hover:text-blue-600 focus:ring-2 focus:ring-blue-500 focus:outline-none"
				aria-label="Show navigation tip"
				title={navigationTip}
			>
				?
			</button>
			<div
				class="pointer-events-none absolute left-0 z-10 mt-2 hidden w-64 rounded-md border border-gray-200 bg-white p-2 text-xs leading-5 text-gray-600 shadow-lg group-focus-within:block group-hover:block"
				role="tooltip"
			>
				{navigationTip}
			</div>
		</div>
	</div>

	<div class="flex flex-wrap items-center gap-3">
		<DateRangeFilter
			startDate={props.startDate}
			endDate={props.endDate}
			onStartDateChange={handleStartDateChange}
			onEndDateChange={handleEndDateChange}
		/>

		<div class="hidden h-6 w-px bg-gray-200 sm:block" aria-hidden="true"></div>

		<div
			class="relative inline-grid min-w-[17rem] grid-cols-4 rounded-md border border-gray-200 bg-gray-50 p-1"
		>
			<div
				class="pointer-events-none absolute top-1 bottom-1 rounded bg-blue-600 shadow-sm transition-transform duration-200 ease-out will-change-transform motion-reduce:transition-none"
				style={`left: 0.25rem; width: calc((100% - 0.5rem) / 4); transform: translateX(${selectedGroupByIndex * 100}%);`}
				aria-hidden="true"
			></div>
			{#each groupByOptions as option (option.value)}
				<button
					type="button"
					onclick={() =>
						props.groupBy !== option.value && dispatch('groupByChange', { groupBy: option.value })}
					class={`relative z-10 rounded px-3 py-1 text-sm transition-colors focus:ring-2 focus:ring-blue-500 focus:outline-none ${
						props.groupBy === option.value ? 'text-white' : 'text-gray-600 hover:text-gray-900'
					}`}
					aria-pressed={props.groupBy === option.value}
				>
					{option.label}
				</button>
			{/each}
		</div>

		<div class="hidden h-6 w-px bg-gray-200 sm:block" aria-hidden="true"></div>

		<RouterFilter routers={props.routers} onRouterChange={handleRoutersChange} />

		<div class="hidden h-6 w-px bg-gray-200 sm:ml-auto sm:block" aria-hidden="true"></div>

		<button
			type="button"
			onclick={handleResetView}
			class="rounded-md bg-blue-600 px-4 py-1 text-sm text-white hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:outline-none sm:ml-0"
		>
			Reset View
		</button>
	</div>
</div>
