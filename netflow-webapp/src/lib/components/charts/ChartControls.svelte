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

<div class="chart-controls flex flex-wrap gap-4 mb-4 p-4 bg-gray-50 rounded-lg">
	<div class="flex items-center gap-2">
		<label for="groupBy" class="text-sm font-medium text-gray-700">Group by:</label>
		<select 
			id="groupBy"
			value={groupBy} 
			onchange={handleGroupByChange}
			class="px-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
		>
			<option value="month">Month</option>
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
			class="px-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
		>
			<option value="stacked">Stacked Area</option>
			<option value="line">Line Chart</option>
		</select>
	</div>

	<button 
		type="button"
		onclick={handleReset}
		class="px-4 py-1 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
	>
		Reset View
	</button>

	<div class="text-sm text-gray-600 flex items-center">
		<span class="font-medium">Navigation:</span>
		<span class="ml-1">Click data points to drill down • {groupBy} view</span>
	</div>
</div>