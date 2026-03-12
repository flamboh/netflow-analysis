<script lang="ts">
	import type { ChartTypeOption, DataOption } from '$lib/components/netflow/types.ts';

	interface Props {
		dataOptions: DataOption[];
		onDataOptionsChange?: (dataOptions: DataOption[]) => void;
		chartType?: ChartTypeOption;
		onChartTypeChange?: (chartType: ChartTypeOption) => void;
	}

	let { dataOptions, onDataOptionsChange, chartType, onChartTypeChange }: Props = $props();

	function handleMetricToggle(index: number) {
		const newDataOptions = dataOptions.map((option, i) =>
			i === index ? { ...option, checked: !option.checked } : option
		);
		onDataOptionsChange?.(newDataOptions);
	}

	function handleQuickSelect(type: 'flows' | 'packets' | 'bytes') {
		const newDataOptions = dataOptions.map((option) => {
			const isSelected =
				type === 'flows'
					? option.label.toLowerCase().includes('flow')
					: type === 'packets'
						? option.label.toLowerCase().includes('packet')
						: option.label.toLowerCase().includes('byte');
			return { ...option, checked: isSelected };
		});
		onDataOptionsChange?.(newDataOptions);
	}

	function handleSelectAll() {
		const newDataOptions = dataOptions.map((option) => ({ ...option, checked: true }));
		onDataOptionsChange?.(newDataOptions);
	}

	function handleSelectNone() {
		const newDataOptions = dataOptions.map((option) => ({ ...option, checked: false }));
		onDataOptionsChange?.(newDataOptions);
	}

	function handleChartTypeChange(event: Event) {
		if (!onChartTypeChange) {
			return;
		}
		const target = event.currentTarget as HTMLSelectElement;
		onChartTypeChange(target.value as ChartTypeOption);
	}
</script>

<div class="metric-selector">
	<div class="mb-4 flex flex-wrap items-center justify-between gap-3">
		<div class="flex flex-wrap items-center gap-2">
			<span class="text-text-secondary text-sm font-medium">Quick Select:</span>
			<button
				type="button"
				onclick={() => handleQuickSelect('flows')}
				class="rounded-lg bg-emerald-100 px-3 py-1 text-xs font-medium text-emerald-800 transition-colors hover:bg-emerald-200 focus:ring-2 focus:ring-emerald-500 focus:outline-none dark:bg-emerald-900/40 dark:text-emerald-300 dark:hover:bg-emerald-900/60"
			>
				All Flows
			</button>
			<button
				type="button"
				onclick={() => handleQuickSelect('packets')}
				class="bg-cisco-blue/10 text-cisco-blue-dark hover:bg-cisco-blue/20 focus:ring-cisco-blue dark:text-cisco-blue-light rounded-lg px-3 py-1 text-xs font-medium transition-colors focus:ring-2 focus:outline-none"
			>
				All Packets
			</button>
			<button
				type="button"
				onclick={() => handleQuickSelect('bytes')}
				class="rounded-lg bg-purple-100 px-3 py-1 text-xs font-medium text-purple-800 transition-colors hover:bg-purple-200 focus:ring-2 focus:ring-purple-500 focus:outline-none dark:bg-purple-900/40 dark:text-purple-300 dark:hover:bg-purple-900/60"
			>
				All Bytes
			</button>
			<button
				type="button"
				onclick={handleSelectAll}
				class="bg-surface-hover text-text-primary hover:bg-border focus:ring-border-strong rounded-lg px-3 py-1 text-xs font-medium transition-colors focus:ring-2 focus:outline-none"
			>
				Select All
			</button>
			<button
				type="button"
				onclick={handleSelectNone}
				class="bg-surface-hover text-text-primary hover:bg-border focus:ring-border-strong rounded-lg px-3 py-1 text-xs font-medium transition-colors focus:ring-2 focus:outline-none"
			>
				Select None
			</button>
		</div>

		{#if chartType}
			<div class="flex items-center">
				<label class="text-text-secondary text-sm">
					Chart type
					<select
						value={chartType}
						onchange={handleChartTypeChange}
						class="border-border bg-surface text-text-primary focus:border-cisco-blue focus:ring-cisco-blue ml-2 rounded-lg border px-2 py-1 text-sm focus:ring-1 focus:outline-none"
						aria-label="Select NetFlow chart type"
					>
						<option value="stacked">Stacked Area</option>
						<option value="line">Line Chart</option>
					</select>
				</label>
			</div>
		{/if}
	</div>

	<div class="grid grid-cols-2 gap-2 sm:grid-cols-3 md:grid-cols-5">
		{#each dataOptions as option, index (index)}
			<label
				class="hover:bg-surface-hover flex cursor-pointer items-center gap-2 rounded-lg p-2 transition-colors"
			>
				<input
					type="checkbox"
					checked={option.checked}
					onchange={() => handleMetricToggle(index)}
					class="border-border-strong text-cisco-blue focus:ring-cisco-blue h-4 w-4 rounded"
				/>
				<span class="text-text-primary text-sm select-none">{option.label}</span>
			</label>
		{/each}
	</div>
</div>
