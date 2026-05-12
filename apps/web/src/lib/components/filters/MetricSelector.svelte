<script lang="ts">
	import type { ChartTypeOption, DataOption } from '$lib/components/netflow/types.ts';
	import type { NetflowIpFamily } from '$lib/types/types';

	type QuickSelectOption = 'flows' | 'packets' | 'bytes' | 'all' | 'none';
	type MetricFamily = 'flows' | 'packets' | 'bytes';
	type ProtocolFamily = 'tcp' | 'udp' | 'icmp' | 'other';
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
	const METRIC_ROWS: Array<{ value: MetricFamily; label: string }> = [
		{ value: 'flows', label: 'Flows' },
		{ value: 'packets', label: 'Packets' },
		{ value: 'bytes', label: 'Bytes' }
	];
	const PROTOCOL_COLUMNS: Array<{ value: ProtocolFamily; label: string }> = [
		{ value: 'tcp', label: 'TCP' },
		{ value: 'udp', label: 'UDP' },
		{ value: 'icmp', label: 'ICMP' },
		{ value: 'other', label: 'Other' }
	];
	const CHART_TYPE_OPTIONS: Array<{ value: ChartTypeOption; label: string }> = [
		{ value: 'stacked', label: 'Stacked Area' },
		{ value: 'line', label: 'Line Chart' }
	];
	const CONTROL_GROUP_CLASS =
		'dark:border-dark-border dark:bg-dark-subtle grid w-full gap-0.5 rounded-md border border-gray-200 bg-gray-50 p-1 sm:w-fit';
	const CONTROL_BUTTON_CLASS =
		'flex min-h-7 items-center justify-center rounded px-2.5 py-0.5 text-center text-xs font-medium transition-colors focus:ring-2 focus:ring-blue-500 focus:outline-none sm:min-w-20';
	const CONTROL_BUTTON_INACTIVE_CLASS =
		'text-gray-700 hover:text-gray-900 dark:text-gray-300 dark:hover:text-gray-100';
	const CONTROL_BUTTON_ACTIVE_CLASS = 'bg-blue-600 text-white shadow-sm';

	function getMetricFamily(label: string): MetricFamily | null {
		const normalized = label.toLowerCase();
		if (normalized.includes('flow')) return 'flows';
		if (normalized.includes('packet')) return 'packets';
		if (normalized.includes('byte')) return 'bytes';
		return null;
	}

	function getProtocolFamily(label: string): ProtocolFamily | null {
		const normalized = label.toLowerCase();
		if (normalized.includes('tcp')) return 'tcp';
		if (normalized.includes('udp')) return 'udp';
		if (normalized.includes('icmp')) return 'icmp';
		if (normalized.includes('other')) return 'other';
		return null;
	}

	function matchesQuickSelect(option: DataOption, selection: QuickSelectOption): boolean {
		if (selection === 'all') return true;
		if (selection === 'none') return false;
		return getMetricFamily(option.label) === selection;
	}

	function handleMetricToggle(optionIndex: number) {
		const newDataOptions = dataOptions.map((option) =>
			option.index === optionIndex ? { ...option, checked: !option.checked } : option
		);
		onDataOptionsChange?.(newDataOptions);
	}

	function handleQuickSelect(selection: QuickSelectOption) {
		const newDataOptions = dataOptions.map((option) => {
			return { ...option, checked: matchesQuickSelect(option, selection) };
		});
		onDataOptionsChange?.(newDataOptions);
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

	const metricMatrix = $derived.by(() => {
		return METRIC_ROWS.map((metric) => ({
			...metric,
			options: PROTOCOL_COLUMNS.map((protocol) => ({
				...protocol,
				option: dataOptions.find(
					(dataOption) =>
						getMetricFamily(dataOption.label) === metric.value &&
						getProtocolFamily(dataOption.label) === protocol.value
				)
			}))
		}));
	});
</script>

<div class="metric-selector">
	<div class="mb-4 flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
		<div class="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center">
			<div class={`${CONTROL_GROUP_CLASS} grid-cols-3 sm:grid-cols-5`}>
				{#each QUICK_SELECT_OPTIONS as option (option.value)}
					<button
						type="button"
						onclick={() => handleQuickSelect(option.value)}
						class={`${CONTROL_BUTTON_CLASS} ${
							selectedQuickSelectIndex !== null &&
							QUICK_SELECT_OPTIONS[selectedQuickSelectIndex]?.value === option.value
								? CONTROL_BUTTON_ACTIVE_CLASS
								: CONTROL_BUTTON_INACTIVE_CLASS
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
					class={CONTROL_GROUP_CLASS}
					style={`grid-template-columns: repeat(${ipFamilyOptions.length}, minmax(0, 1fr));`}
				>
					{#each ipFamilyOptions as option (option.value)}
						<button
							type="button"
							onclick={() => onIpFamilyChange?.(option.value)}
							class={`${CONTROL_BUTTON_CLASS} ${
								selectedIpFamily === option.value
									? CONTROL_BUTTON_ACTIVE_CLASS
									: CONTROL_BUTTON_INACTIVE_CLASS
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
			<div
				class={`${CONTROL_GROUP_CLASS} grid-cols-2`}
				role="group"
				aria-label="Select NetFlow chart type"
			>
				{#each CHART_TYPE_OPTIONS as option (option.value)}
					<button
						type="button"
						onclick={() => onChartTypeChange?.(option.value)}
						class={`${CONTROL_BUTTON_CLASS} ${
							chartType === option.value
								? CONTROL_BUTTON_ACTIVE_CLASS
								: CONTROL_BUTTON_INACTIVE_CLASS
						}`}
						aria-pressed={chartType === option.value}
					>
						{option.label}
					</button>
				{/each}
			</div>
		{/if}
	</div>

	<div role="group" aria-label="NetFlow metric series">
		<div class="grid grid-cols-[minmax(4rem,0.95fr)_repeat(4,minmax(3rem,1fr))]">
			<div class="px-2 py-2 text-xs font-medium text-gray-500 dark:text-gray-400">Metric</div>
			{#each PROTOCOL_COLUMNS as protocol (protocol.value)}
				<div class="px-1 py-2 text-center text-xs font-medium text-gray-500 dark:text-gray-400">
					{protocol.label}
				</div>
			{/each}
		</div>
		{#each metricMatrix as metric (metric.value)}
			<div class="grid grid-cols-[minmax(4rem,0.95fr)_repeat(4,minmax(3rem,1fr))]">
				<div
					class="flex items-center px-2 py-2 text-sm font-medium text-gray-700 dark:text-gray-300"
				>
					{metric.label}
				</div>
				{#each metric.options as cell (cell.value)}
					<label
						class="flex min-h-11 cursor-pointer items-center justify-center"
						aria-label={cell.option?.label ?? `${metric.label} ${cell.label}`}
					>
						{#if cell.option}
							{@const option = cell.option}
							<input
								type="checkbox"
								checked={option.checked}
								onchange={() => handleMetricToggle(option.index)}
								class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
							/>
							<span class="sr-only">{option.label}</span>
						{/if}
					</label>
				{/each}
			</div>
		{/each}
	</div>
</div>
