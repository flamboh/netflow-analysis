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

	const selectedCount = $derived(dataOptions.filter((option) => option.checked).length);
</script>

<div class="metric-selector">
	<div class="mb-3 flex flex-wrap items-center justify-between gap-2">
		<h4 class="text-sm font-semibold tracking-[0.08em] text-slate-700 uppercase">
			Quick Select ({selectedCount}/{dataOptions.length})
		</h4>
		<div class="flex flex-wrap gap-2">
			<button type="button" onclick={handleSelectAll} class="btn-muted">All</button>
			<button type="button" onclick={handleSelectNone} class="btn-muted">None</button>
		</div>
	</div>

	<div class="mb-4 flex flex-wrap gap-2">
		<button type="button" onclick={() => handleQuickSelect('flows')} class="btn-muted">
			Flows
		</button>
		<button type="button" onclick={() => handleQuickSelect('packets')} class="btn-muted">
			Packets
		</button>
		<button type="button" onclick={() => handleQuickSelect('bytes')} class="btn-muted">
			Bytes
		</button>
	</div>

	<div class="grid grid-cols-2 gap-2 sm:grid-cols-3 md:grid-cols-5">
		{#each dataOptions as option, index (index)}
			<label
				class="flex cursor-pointer items-center gap-2 rounded-md border border-slate-200/70 bg-white px-2 py-2 hover:bg-slate-50"
			>
				<input
					type="checkbox"
					checked={option.checked}
					onchange={() => handleMetricToggle(index)}
					class="h-4 w-4 rounded border-slate-300 text-cyan-700 focus:ring-cyan-700"
				/>
				<span class="text-sm text-slate-700 select-none">{option.label}</span>
			</label>
		{/each}
	</div>
</div>
