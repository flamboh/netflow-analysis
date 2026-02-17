<script lang="ts">
	import type { GroupByOption, ChartTypeOption } from '$lib/components/netflow/types.ts';

	interface Props {
		groupBy: GroupByOption;
		chartType: ChartTypeOption;
		onGroupByChange?: (groupBy: GroupByOption) => void;
		onChartTypeChange?: (chartType: ChartTypeOption) => void;
		onReset?: () => void;
		showGroupBy?: boolean;
	}

	let {
		groupBy,
		chartType,
		onGroupByChange,
		onChartTypeChange,
		onReset,
		showGroupBy = true
	}: Props = $props();

	function handleGroupByChange(event: Event) {
		const target = event.target as HTMLSelectElement;
		onGroupByChange?.(target.value as GroupByOption);
	}

	function handleChartTypeChange(event: Event) {
		const target = event.target as HTMLSelectElement;
		onChartTypeChange?.(target.value as ChartTypeOption);
	}

	function handleReset() {
		onReset?.();
	}
</script>

<div
	class="chart-controls mb-4 flex flex-wrap items-center gap-3 rounded-lg border border-slate-200/70 bg-slate-50/70 p-3"
>
	{#if showGroupBy}
		<div class="flex items-center gap-2">
			<label for="groupBy" class="text-sm font-medium text-slate-700">Group by</label>
			<select id="groupBy" value={groupBy} onchange={handleGroupByChange} class="control-input">
				<option value="date">Date</option>
				<option value="hour">Hour</option>
				<option value="30min">30 Minutes</option>
				<option value="5min">5 Minutes</option>
			</select>
		</div>
	{/if}

	<div class="flex items-center gap-2">
		<label for="chartType" class="text-sm font-medium text-slate-700">Chart type</label>
		<select id="chartType" value={chartType} onchange={handleChartTypeChange} class="control-input">
			<option value="stacked">Stacked Area</option>
			<option value="line">Line Chart</option>
		</select>
	</div>

	<button type="button" onclick={handleReset} class="btn-primary"> Reset View </button>

	<div class="flex items-center text-sm text-slate-600">
		<span class="font-medium">Tip:</span>
		<span class="ml-1">click any chart position to drill down ({groupBy} view)</span>
	</div>
</div>
