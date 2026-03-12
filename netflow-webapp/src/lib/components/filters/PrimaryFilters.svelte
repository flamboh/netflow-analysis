<script lang="ts">
	import { createEventDispatcher } from 'svelte';
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

	function handleStartDateChange(event: Event) {
		dispatch('startDateChange', { startDate: (event.target as HTMLInputElement).value });
	}

	function handleEndDateChange(event: Event) {
		dispatch('endDateChange', { endDate: (event.target as HTMLInputElement).value });
	}

	function handleRoutersChange(nextRouters: RouterConfig) {
		dispatch('routersChange', { routers: nextRouters });
	}

	function handleResetView() {
		dispatch('resetView', {});
	}
</script>

<div
	class="border-border bg-surface flex flex-wrap items-center gap-x-4 gap-y-2 rounded-xl border px-4 py-3 shadow-sm"
>
	<input
		type="date"
		value={props.startDate}
		onchange={handleStartDateChange}
		aria-label="Start date"
		class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover h-8 rounded-lg border px-2 text-sm focus:ring-1 focus:outline-none"
	/>
	<span class="text-text-muted text-xs">to</span>
	<input
		type="date"
		value={props.endDate}
		onchange={handleEndDateChange}
		aria-label="End date"
		class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover h-8 rounded-lg border px-2 text-sm focus:ring-1 focus:outline-none"
	/>

	<span class="bg-border h-5 w-px" aria-hidden="true"></span>

	<div class="flex items-center gap-1.5">
		{#each props.groupByOptions ?? DEFAULT_GROUP_BY_OPTIONS as option (option.value)}
			<button
				type="button"
				onclick={() => {
					if (option.value !== props.groupBy) dispatch('groupByChange', { groupBy: option.value });
				}}
				class={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
					option.value === props.groupBy
						? 'bg-cisco-blue text-white'
						: 'text-text-secondary hover:bg-surface-hover hover:text-text-primary'
				}`}
			>
				{option.label}
			</button>
		{/each}
	</div>

	<span class="bg-border h-5 w-px" aria-hidden="true"></span>

	<RouterFilter routers={props.routers} onRouterChange={handleRoutersChange} />

	<div class="ml-auto">
		<button
			type="button"
			onclick={handleResetView}
			class="text-text-muted hover:bg-surface-hover hover:text-text-primary rounded-lg px-3 py-1 text-xs font-medium transition-colors"
		>
			Reset
		</button>
	</div>
</div>
