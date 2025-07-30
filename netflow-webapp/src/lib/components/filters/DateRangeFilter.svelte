<script lang="ts">
	interface Props {
		startDate: string;
		endDate: string;
		onStartDateChange?: (date: string) => void;
		onEndDateChange?: (date: string) => void;
	}

	let { startDate, endDate, onStartDateChange, onEndDateChange }: Props = $props();

	function handleStartDateChange(event: Event) {
		const target = event.target as HTMLInputElement;
		onStartDateChange?.(target.value);
	}

	function handleEndDateChange(event: Event) {
		const target = event.target as HTMLInputElement;
		onEndDateChange?.(target.value);
	}
</script>

<div class="date-range-filter flex flex-wrap gap-4 items-center">
	<div class="flex items-center gap-2">
		<label for="startDate" class="text-sm font-medium text-gray-700">Start Date:</label>
		<input 
			id="startDate"
			type="date" 
			value={startDate}
			onchange={handleStartDateChange}
			class="px-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
		/>
	</div>

	<div class="flex items-center gap-2">
		<label for="endDate" class="text-sm font-medium text-gray-700">End Date:</label>
		<input 
			id="endDate"
			type="date" 
			value={endDate}
			onchange={handleEndDateChange}
			class="px-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
		/>
	</div>
</div>