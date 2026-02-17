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

<div class="date-range-filter">
	<h3 class="mb-3 text-sm font-semibold tracking-[0.08em] text-slate-700 uppercase">Date Window</h3>
	<div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
		<div class="flex flex-col gap-1.5">
			<label for="startDate" class="text-xs font-medium tracking-[0.06em] text-slate-600 uppercase"
				>Start Date</label
			>
			<input
				id="startDate"
				type="date"
				value={startDate}
				onchange={handleStartDateChange}
				class="control-input"
			/>
		</div>

		<div class="flex flex-col gap-1.5">
			<label for="endDate" class="text-xs font-medium tracking-[0.06em] text-slate-600 uppercase"
				>End Date</label
			>
			<input
				id="endDate"
				type="date"
				value={endDate}
				onchange={handleEndDateChange}
				class="control-input"
			/>
		</div>
	</div>
</div>
