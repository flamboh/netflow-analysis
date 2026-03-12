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
	<div class="mb-3 flex flex-wrap items-center justify-between gap-3">
		<div class="flex flex-wrap items-center gap-1.5">
			<span class="mr-1 text-sm font-medium text-gray-600">Quick Select:</span>
			<button
				type="button"
				onclick={() => handleQuickSelect('flows')}
				class="focus:ring-cisco-blue/30 rounded-md border border-gray-200 bg-gray-50 px-2.5 py-1 text-xs font-medium text-gray-700 transition-colors hover:border-gray-300 hover:bg-gray-100 focus:ring-1 focus:outline-none"
			>
				Flows
			</button>
			<button
				type="button"
				onclick={() => handleQuickSelect('packets')}
				class="focus:ring-cisco-blue/30 rounded-md border border-gray-200 bg-gray-50 px-2.5 py-1 text-xs font-medium text-gray-700 transition-colors hover:border-gray-300 hover:bg-gray-100 focus:ring-1 focus:outline-none"
			>
				Packets
			</button>
			<button
				type="button"
				onclick={() => handleQuickSelect('bytes')}
				class="focus:ring-cisco-blue/30 rounded-md border border-gray-200 bg-gray-50 px-2.5 py-1 text-xs font-medium text-gray-700 transition-colors hover:border-gray-300 hover:bg-gray-100 focus:ring-1 focus:outline-none"
			>
				Bytes
			</button>
			<button
				type="button"
				onclick={handleSelectAll}
				class="focus:ring-cisco-blue/30 rounded-md border border-gray-200 bg-gray-50 px-2.5 py-1 text-xs font-medium text-gray-700 transition-colors hover:border-gray-300 hover:bg-gray-100 focus:ring-1 focus:outline-none"
			>
				All
			</button>
			<button
				type="button"
				onclick={handleSelectNone}
				class="focus:ring-cisco-blue/30 rounded-md border border-gray-200 bg-gray-50 px-2.5 py-1 text-xs font-medium text-gray-700 transition-colors hover:border-gray-300 hover:bg-gray-100 focus:ring-1 focus:outline-none"
			>
				None
			</button>
		</div>

		{#if chartType}
			<div class="flex items-center">
				<label class="text-sm text-gray-500">
					Chart type
					<select
						value={chartType}
						onchange={handleChartTypeChange}
						class="focus:border-cisco-blue focus:ring-cisco-blue/30 ml-2 rounded-md border border-gray-300 bg-white px-2 py-1.5 text-sm text-gray-700 transition-colors focus:ring-1 focus:outline-none"
						aria-label="Select NetFlow chart type"
					>
						<option value="stacked">Stacked Area</option>
						<option value="line">Line Chart</option>
					</select>
				</label>
			</div>
		{/if}
	</div>

	<div class="grid grid-cols-2 gap-1.5 sm:grid-cols-3 md:grid-cols-5">
		{#each dataOptions as option, index (index)}
			<label
				class="flex cursor-pointer items-center gap-2 rounded-md p-2 transition-colors hover:bg-gray-50"
			>
				<input
					type="checkbox"
					checked={option.checked}
					onchange={() => handleMetricToggle(index)}
					class="text-cisco-blue focus:ring-cisco-blue/40 h-4 w-4 rounded border-gray-300"
				/>
				<span class="text-sm text-gray-700 select-none">{option.label}</span>
			</label>
		{/each}
	</div>
</div>
