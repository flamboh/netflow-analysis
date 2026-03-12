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

<div class="date-range-filter flex flex-wrap items-center gap-4">
	<div class="flex items-center gap-2">
		<label for="startDate" class="text-sm font-medium text-gray-600">Start Date:</label>
		<input
			id="startDate"
			type="date"
			value={startDate}
			onchange={handleStartDateChange}
			class="focus:border-cisco-blue focus:ring-cisco-blue/30 rounded-md border border-gray-300 px-3 py-1.5 text-sm transition-colors focus:ring-1 focus:outline-none"
		/>
	</div>

	<div class="flex items-center gap-2">
		<label for="endDate" class="text-sm font-medium text-gray-600">End Date:</label>
		<input
			id="endDate"
			type="date"
			value={endDate}
			onchange={handleEndDateChange}
			class="focus:border-cisco-blue focus:ring-cisco-blue/30 rounded-md border border-gray-300 px-3 py-1.5 text-sm transition-colors focus:ring-1 focus:outline-none"
		/>
	</div>
</div>
