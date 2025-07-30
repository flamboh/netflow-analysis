<script lang="ts">
	import type { DataOption } from '$lib/components/netflow/types.ts';

	interface Props {
		dataOptions: DataOption[];
		onDataOptionsChange?: (dataOptions: DataOption[]) => void;
	}

	let { dataOptions, onDataOptionsChange }: Props = $props();

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
</script>

<div class="metric-selector">
	<div class="mb-4 flex flex-wrap gap-2">
		<span class="text-sm font-medium text-gray-700">Quick Select:</span>
		<button
			type="button"
			onclick={() => handleQuickSelect('flows')}
			class="rounded-md bg-green-100 px-3 py-1 text-xs text-green-800 hover:bg-green-200 focus:outline-none focus:ring-2 focus:ring-green-500"
		>
			All Flows
		</button>
		<button
			type="button"
			onclick={() => handleQuickSelect('packets')}
			class="rounded-md bg-blue-100 px-3 py-1 text-xs text-blue-800 hover:bg-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
		>
			All Packets
		</button>
		<button
			type="button"
			onclick={() => handleQuickSelect('bytes')}
			class="rounded-md bg-purple-100 px-3 py-1 text-xs text-purple-800 hover:bg-purple-200 focus:outline-none focus:ring-2 focus:ring-purple-500"
		>
			All Bytes
		</button>
		<button
			type="button"
			onclick={handleSelectAll}
			class="rounded-md bg-gray-100 px-3 py-1 text-xs text-gray-800 hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-500"
		>
			Select All
		</button>
		<button
			type="button"
			onclick={handleSelectNone}
			class="rounded-md bg-gray-100 px-3 py-1 text-xs text-gray-800 hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-500"
		>
			Select None
		</button>
	</div>

	<div class="grid grid-cols-2 gap-2 sm:grid-cols-3 md:grid-cols-5">
		{#each dataOptions as option, index}
			<label class="flex cursor-pointer items-center gap-2 rounded-md p-2 hover:bg-gray-50">
				<input
					type="checkbox"
					checked={option.checked}
					onchange={() => handleMetricToggle(index)}
					class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
				/>
				<span class="select-none text-sm text-gray-700">{option.label}</span>
			</label>
		{/each}
	</div>
</div>
