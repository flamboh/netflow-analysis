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
		<label for="startDate" class="text-text-secondary text-sm font-medium">Start Date:</label>
		<input
			id="startDate"
			type="date"
			value={startDate}
			onchange={handleStartDateChange}
			class="border-border bg-surface text-text-primary focus:ring-cisco-blue rounded-lg border px-3 py-1 text-sm focus:ring-2 focus:outline-none"
		/>
	</div>

	<div class="flex items-center gap-2">
		<label for="endDate" class="text-text-secondary text-sm font-medium">End Date:</label>
		<input
			id="endDate"
			type="date"
			value={endDate}
			onchange={handleEndDateChange}
			class="border-border bg-surface text-text-primary focus:ring-cisco-blue rounded-lg border px-3 py-1 text-sm focus:ring-2 focus:outline-none"
		/>
	</div>
</div>
