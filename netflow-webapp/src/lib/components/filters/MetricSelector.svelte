<script lang="ts">
	import type { ChartTypeOption, DataOption } from '$lib/components/netflow/types.ts';

	interface Props {
		dataOptions: DataOption[];
		onDataOptionsChange?: (dataOptions: DataOption[]) => void;
		chartType?: ChartTypeOption;
		onChartTypeChange?: (chartType: ChartTypeOption) => void;
	}

	let { dataOptions, onDataOptionsChange, chartType, onChartTypeChange }: Props = $props();
	let expanded = $state(false);

	const selectedCount = $derived(dataOptions.filter((o) => o.checked).length);
	const selectedSummary = $derived(
		dataOptions
			.filter((o) => o.checked)
			.map((o) => o.label)
			.join(', ')
	);

	function handleMetricToggle(index: number) {
		const next = dataOptions.map((o, i) => (i === index ? { ...o, checked: !o.checked } : o));
		onDataOptionsChange?.(next);
	}

	function handleQuickSelect(type: 'flows' | 'packets' | 'bytes') {
		const next = dataOptions.map((o) => {
			const match =
				type === 'flows'
					? o.label.toLowerCase().includes('flow')
					: type === 'packets'
						? o.label.toLowerCase().includes('packet')
						: o.label.toLowerCase().includes('byte');
			return { ...o, checked: match };
		});
		onDataOptionsChange?.(next);
	}

	function handleChartTypeChange(event: Event) {
		onChartTypeChange?.((event.currentTarget as HTMLSelectElement).value as ChartTypeOption);
	}
</script>

<div class="metric-selector">
	<div class="flex items-center gap-3">
		<button
			type="button"
			onclick={() => (expanded = !expanded)}
			class="text-text-secondary hover:bg-surface-hover hover:text-text-primary flex items-center gap-2 rounded-lg px-2 py-1 text-sm font-medium transition-colors"
		>
			<svg
				class="h-4 w-4 transition-transform {expanded ? 'rotate-90' : ''}"
				fill="none"
				viewBox="0 0 24 24"
				stroke="currentColor"
				stroke-width="2"
			>
				<path d="M9 5l7 7-7 7" />
			</svg>
			Metrics
			<span class="bg-cisco-blue/10 text-cisco-blue rounded-full px-1.5 py-0.5 text-xs">
				{selectedCount}
			</span>
		</button>

		{#if !expanded}
			<span class="text-text-muted truncate text-xs">{selectedSummary}</span>
		{/if}

		<div class="ml-auto flex items-center gap-2">
			{#if chartType}
				<select
					value={chartType}
					onchange={handleChartTypeChange}
					class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover rounded-lg border px-2 py-1 text-xs focus:ring-1 focus:outline-none"
					aria-label="Chart type"
				>
					<option value="stacked">Stacked</option>
					<option value="line">Line</option>
				</select>
			{/if}
		</div>
	</div>

	{#if expanded}
		<div class="mt-3 space-y-3">
			<div class="flex flex-wrap items-center gap-2">
				<button
					type="button"
					onclick={() => handleQuickSelect('flows')}
					class="rounded-md bg-emerald-100 px-2.5 py-0.5 text-xs font-medium text-emerald-800 transition-colors hover:bg-emerald-200 dark:bg-emerald-900/40 dark:text-emerald-300 dark:hover:bg-emerald-900/60"
				>
					Flows
				</button>
				<button
					type="button"
					onclick={() => handleQuickSelect('packets')}
					class="bg-cisco-blue/10 text-cisco-blue-dark hover:bg-cisco-blue/20 dark:text-cisco-blue-light rounded-md px-2.5 py-0.5 text-xs font-medium transition-colors"
				>
					Packets
				</button>
				<button
					type="button"
					onclick={() => handleQuickSelect('bytes')}
					class="rounded-md bg-purple-100 px-2.5 py-0.5 text-xs font-medium text-purple-800 transition-colors hover:bg-purple-200 dark:bg-purple-900/40 dark:text-purple-300 dark:hover:bg-purple-900/60"
				>
					Bytes
				</button>
			</div>

			<div class="grid grid-cols-2 gap-1 sm:grid-cols-3 md:grid-cols-5">
				{#each dataOptions as option, index (index)}
					<label
						class="hover:bg-surface-hover flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 transition-colors"
					>
						<input
							type="checkbox"
							checked={option.checked}
							onchange={() => handleMetricToggle(index)}
							class="border-border-strong text-cisco-blue focus:ring-cisco-blue h-3.5 w-3.5 rounded"
						/>
						<span class="text-text-primary text-xs select-none">{option.label}</span>
					</label>
				{/each}
			</div>
		</div>
	{/if}
</div>
