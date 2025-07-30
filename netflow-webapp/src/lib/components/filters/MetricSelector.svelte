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
			const isSelected = type === 'flows' ? option.label.toLowerCase().includes('flow') :
							   type === 'packets' ? option.label.toLowerCase().includes('packet') :
							   option.label.toLowerCase().includes('byte');
			return { ...option, checked: isSelected };
		});
		onDataOptionsChange?.(newDataOptions);
	}

	function handleSelectAll() {
		const newDataOptions = dataOptions.map(option => ({ ...option, checked: true }));
		onDataOptionsChange?.(newDataOptions);
	}

	function handleSelectNone() {
		const newDataOptions = dataOptions.map(option => ({ ...option, checked: false }));
		onDataOptionsChange?.(newDataOptions);
	}
</script>

<div class="metric-selector">
	<div class="flex flex-wrap gap-2 mb-4">
		<span class="text-sm font-medium text-gray-700">Quick Select:</span>
		<button 
			type="button"
			onclick={() => handleQuickSelect('flows')}
			class="px-3 py-1 bg-green-100 text-green-800 text-xs rounded-md hover:bg-green-200 focus:outline-none focus:ring-2 focus:ring-green-500"
		>
			All Flows
		</button>
		<button 
			type="button"
			onclick={() => handleQuickSelect('packets')}
			class="px-3 py-1 bg-blue-100 text-blue-800 text-xs rounded-md hover:bg-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
		>
			All Packets
		</button>
		<button 
			type="button"
			onclick={() => handleQuickSelect('bytes')}
			class="px-3 py-1 bg-purple-100 text-purple-800 text-xs rounded-md hover:bg-purple-200 focus:outline-none focus:ring-2 focus:ring-purple-500"
		>
			All Bytes
		</button>
		<button 
			type="button"
			onclick={handleSelectAll}
			class="px-3 py-1 bg-gray-100 text-gray-800 text-xs rounded-md hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-500"
		>
			Select All
		</button>
		<button 
			type="button"
			onclick={handleSelectNone}
			class="px-3 py-1 bg-gray-100 text-gray-800 text-xs rounded-md hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-500"
		>
			Select None
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-2">
		{#each dataOptions as option, index}
			<label class="flex items-center gap-2 cursor-pointer p-2 rounded-md hover:bg-gray-50">
				<input 
					type="checkbox" 
					checked={option.checked}
					onchange={() => handleMetricToggle(index)}
					class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
				/>
				<span class="text-sm text-gray-700 select-none">{option.label}</span>
			</label>
		{/each}
	</div>
</div>