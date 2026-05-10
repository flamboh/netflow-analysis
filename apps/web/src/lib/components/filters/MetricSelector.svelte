<script lang="ts">
	import type { ChartTypeOption, DataOption } from '$lib/components/netflow/types.ts';
	import type { NetflowIpFamily } from '$lib/types/types';

	type QuickSelectOption = 'flows' | 'packets' | 'bytes' | 'all' | 'none';
	type IpFamilyOption = { value: NetflowIpFamily; label: string; disabled?: boolean };

	interface Props {
		dataOptions: DataOption[];
		onDataOptionsChange?: (dataOptions: DataOption[]) => void;
		chartType?: ChartTypeOption;
		onChartTypeChange?: (chartType: ChartTypeOption) => void;
		ipFamilyOptions?: IpFamilyOption[];
		selectedIpFamily?: NetflowIpFamily;
		onIpFamilyChange?: (ipFamily: NetflowIpFamily) => void;
	}

	let {
		dataOptions,
		onDataOptionsChange,
		chartType,
		onChartTypeChange,
		ipFamilyOptions = [],
		selectedIpFamily = 'all',
		onIpFamilyChange
	}: Props = $props();
	const QUICK_SELECT_OPTIONS: Array<{ value: QuickSelectOption; label: string }> = [
		{ value: 'flows', label: 'Flows' },
		{ value: 'packets', label: 'Packets' },
		{ value: 'bytes', label: 'Bytes' },
		{ value: 'all', label: 'Select All' },
		{ value: 'none', label: 'Select None' }
	];

	function getMetricFamily(label: string): 'flows' | 'packets' | 'bytes' | null {
		const normalized = label.toLowerCase();
		if (normalized.includes('flow')) return 'flows';
		if (normalized.includes('packet')) return 'packets';
		if (normalized.includes('byte')) return 'bytes';
		return null;
	}

	function matchesQuickSelect(option: DataOption, selection: QuickSelectOption): boolean {
		if (selection === 'all') return true;
		if (selection === 'none') return false;
		return getMetricFamily(option.label) === selection;
	}

	function handleMetricToggle(index: number) {
		const newDataOptions = dataOptions.map((option, i) =>
			i === index ? { ...option, checked: !option.checked } : option
		);
		onDataOptionsChange?.(newDataOptions);
	}

	function handleQuickSelect(selection: QuickSelectOption) {
		const newDataOptions = dataOptions.map((option) => {
			return { ...option, checked: matchesQuickSelect(option, selection) };
		});
		onDataOptionsChange?.(newDataOptions);
	}

	function handleChartTypeChange(event: Event) {
		if (!onChartTypeChange) {
			return;
		}
		const target = event.currentTarget as HTMLSelectElement;
		onChartTypeChange(target.value as ChartTypeOption);
	}

	const selectedQuickSelectIndex = $derived.by(() => {
		for (const [index, option] of QUICK_SELECT_OPTIONS.entries()) {
			const matches = dataOptions.every(
				(metricOption) => metricOption.checked === matchesQuickSelect(metricOption, option.value)
			);
			if (matches) {
				return index;
			}
		}

		return null;
	});

	const selectedIpFamilyIndex = $derived.by(() => {
		return ipFamilyOptions.findIndex((option) => option.value === selectedIpFamily);
	});
</script>

<div class="metric-selector">
	<div class="mb-4 flex flex-wrap items-center justify-between gap-3">
		<div class="flex flex-wrap items-center gap-3">
			<div
				class="dark:border-dark-border dark:bg-dark-subtle relative inline-grid min-w-[24rem] grid-cols-5 rounded-md border border-gray-200 bg-gray-50 p-1"
			>
				<div
					class={`pointer-events-none absolute top-1 bottom-1 rounded bg-blue-600 shadow-sm transition-transform duration-200 ease-out will-change-transform motion-reduce:transition-none ${
						selectedQuickSelectIndex === null ? 'opacity-0' : 'opacity-100'
					}`}
					style={`left: 0.25rem; width: calc((100% - 0.5rem) / 5); transform: translateX(${(selectedQuickSelectIndex ?? 0) * 100}%);`}
					aria-hidden="true"
				></div>
				{#each QUICK_SELECT_OPTIONS as option (option.value)}
					<button
						type="button"
						onclick={() => handleQuickSelect(option.value)}
						class={`relative z-10 flex items-center justify-center rounded px-3 py-1 text-center text-xs transition-colors focus:ring-2 focus:ring-blue-500 focus:outline-none ${
							selectedQuickSelectIndex !== null &&
							QUICK_SELECT_OPTIONS[selectedQuickSelectIndex]?.value === option.value
								? 'text-white'
								: 'text-gray-700 hover:text-gray-900 dark:text-gray-300 dark:hover:text-gray-100'
						}`}
						aria-pressed={selectedQuickSelectIndex !== null &&
							QUICK_SELECT_OPTIONS[selectedQuickSelectIndex]?.value === option.value}
					>
						{option.label}
					</button>
				{/each}
			</div>

			{#if ipFamilyOptions.length > 0}
				<div
					class="dark:border-dark-border dark:bg-dark-subtle relative inline-grid min-w-[12rem] rounded-md border border-gray-200 bg-gray-50 p-1"
					style={`grid-template-columns: repeat(${ipFamilyOptions.length}, minmax(0, 1fr));`}
				>
					<div
						class={`pointer-events-none absolute top-1 bottom-1 rounded bg-blue-600 shadow-sm transition-transform duration-200 ease-out will-change-transform motion-reduce:transition-none ${
							selectedIpFamilyIndex < 0 ? 'opacity-0' : 'opacity-100'
						}`}
						style={`left: 0.25rem; width: calc((100% - 0.5rem) / ${ipFamilyOptions.length}); transform: translateX(${Math.max(selectedIpFamilyIndex, 0) * 100}%);`}
						aria-hidden="true"
					></div>
					{#each ipFamilyOptions as option (option.value)}
						<button
							type="button"
							onclick={() => onIpFamilyChange?.(option.value)}
							class={`relative z-10 flex items-center justify-center rounded px-3 py-1 text-center text-xs transition-colors focus:ring-2 focus:ring-blue-500 focus:outline-none ${
								selectedIpFamily === option.value
									? 'text-white'
									: 'text-gray-700 hover:text-gray-900 dark:text-gray-300 dark:hover:text-gray-100'
							} ${option.disabled ? 'pointer-events-none opacity-50' : ''}`}
							aria-pressed={selectedIpFamily === option.value}
							disabled={option.disabled}
						>
							{option.label}
						</button>
					{/each}
				</div>
			{/if}
		</div>

		{#if chartType}
			<div class="flex items-center">
				<select
					value={chartType}
					onchange={handleChartTypeChange}
					class="dark:border-dark-border dark:bg-dark-subtle rounded border border-gray-300 bg-white px-2 py-1 text-sm text-gray-700 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none dark:text-gray-300"
					aria-label="Select NetFlow chart type"
				>
					<option value="stacked">Stacked Area</option>
					<option value="line">Line Chart</option>
				</select>
			</div>
		{/if}
	</div>

	<div class="grid grid-cols-4 gap-2">
		{#each dataOptions as option, index (index)}
			<label
				class="dark:hover:bg-dark-subtle flex min-h-14 cursor-pointer items-center gap-2 rounded-md p-2 hover:bg-gray-50"
			>
				<input
					type="checkbox"
					checked={option.checked}
					onchange={() => handleMetricToggle(index)}
					class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
				/>
				<span class="text-sm text-gray-700 select-none dark:text-gray-300">{option.label}</span>
			</label>
		{/each}
	</div>
</div>
