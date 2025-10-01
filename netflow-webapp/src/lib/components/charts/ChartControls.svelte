<script lang="ts">
	import type { GroupByOption, ChartTypeOption } from '$lib/components/netflow/types.ts';

	interface Props {
		groupBy: GroupByOption;
		chartType: ChartTypeOption;
		onGroupByChange?: (groupBy: GroupByOption) => void;
		onChartTypeChange?: (chartType: ChartTypeOption) => void;
		onReset?: () => void;
	}

	let { groupBy, chartType, onGroupByChange, onChartTypeChange, onReset }: Props = $props();

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

<div class="chart-controls mb-4 flex flex-wrap gap-4 rounded-lg bg-gray-50 p-4">
	<div class="flex items-center gap-2">
		<label for="groupBy" class="text-sm font-medium text-gray-700">Group by:</label>
		<select
			id="groupBy"
			value={groupBy}
			onchange={handleGroupByChange}
			class="rounded-md border border-gray-300 px-3 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
		>
			<option value="date">Date</option>
			<option value="hour">Hour</option>
			<option value="30min">30 Minutes</option>
			<option value="5min">5 Minutes</option>
		</select>
	</div>

	<div class="flex items-center gap-2">
		<label for="chartType" class="text-sm font-medium text-gray-700">Chart type:</label>
		<select
			id="chartType"
			value={chartType}
			onchange={handleChartTypeChange}
			class="rounded-md border border-gray-300 px-3 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
		>
			<option value="stacked">Stacked Area</option>
			<option value="line">Line Chart</option>
		</select>
	</div>

	<button
		type="button"
		onclick={handleReset}
		class="rounded-md bg-blue-600 px-4 py-1 text-sm text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
	>
		Reset View
	</button>

	<div class="flex items-center text-sm text-gray-600">
		<span class="font-medium">Navigation:</span>
		<span class="ml-1">Click data points to drill down • {groupBy} view</span>
	</div>
</div>
