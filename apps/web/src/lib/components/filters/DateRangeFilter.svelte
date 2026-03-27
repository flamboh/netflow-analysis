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

<div class="date-range-filter flex flex-wrap items-center gap-2">
	<div class="flex items-center gap-2">
		<label for="startDate" class="sr-only">Start Date</label>
		<input
			id="startDate"
			type="date"
			value={startDate}
			onchange={handleStartDateChange}
			class="dark:border-dark-border dark:bg-dark-subtle min-w-[9.5rem] rounded-md border border-gray-300 px-3 py-1 text-sm focus:ring-2 focus:ring-blue-500 focus:outline-none dark:text-gray-100 dark:[color-scheme:dark]"
		/>
	</div>

	<span class="text-sm text-gray-500 dark:text-gray-400">to</span>

	<div class="flex items-center gap-2">
		<label for="endDate" class="sr-only">End Date</label>
		<input
			id="endDate"
			type="date"
			value={endDate}
			onchange={handleEndDateChange}
			class="dark:border-dark-border dark:bg-dark-subtle min-w-[9.5rem] rounded-md border border-gray-300 px-3 py-1 text-sm focus:ring-2 focus:ring-blue-500 focus:outline-none dark:text-gray-100 dark:[color-scheme:dark]"
		/>
	</div>
</div>
