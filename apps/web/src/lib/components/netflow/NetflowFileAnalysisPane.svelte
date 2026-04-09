<script lang="ts">
	import SpectrumChart from '$lib/components/charts/SpectrumChart.svelte';
	import StructureFunctionChart from '$lib/components/charts/StructureFunctionChart.svelte';
	import type { FileDetailResourceView } from './file-detail-loader.svelte';
	import type { SpectrumData, StructureFunctionData } from '$lib/types/types';

	type AnalysisKind = 'structure' | 'spectrum';

	let {
		kind,
		sideLabel,
		slot
	}: {
		kind: AnalysisKind;
		sideLabel: 'source' | 'destination';
		slot: FileDetailResourceView<StructureFunctionData | SpectrumData>;
	} = $props();

	const kindLabel = $derived(kind === 'structure' ? 'structure' : 'spectrum');
	const loadingLabel = $derived(`Loading ${sideLabel} ${kindLabel}...`);
	const emptyLabel = $derived(`No ${sideLabel} ${kindLabel} data.`);
	const errorLabel = $derived(`Error loading ${sideLabel} ${kindLabel}:`);
</script>

{#if slot.loading && slot.data === null}
	<div class="flex items-center justify-center py-6">
		<div class="text-gray-600 dark:text-gray-400">{loadingLabel}</div>
	</div>
{:else if slot.error && slot.data === null}
	<div
		class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
	>
		<p>{errorLabel} {slot.error}</p>
		<button
			class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
			onclick={slot.refresh}
		>
			Retry
		</button>
	</div>
{:else if slot.data}
	<div class="space-y-3">
		{#if slot.loading}
			<div class="text-sm text-gray-500 dark:text-gray-400">
				Refreshing {sideLabel}
				{kindLabel}...
			</div>
		{/if}
		{#if slot.error}
			<div
				class="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
			>
				<p>{errorLabel} {slot.error}</p>
				<button
					class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
					onclick={slot.refresh}
				>
					Retry
				</button>
			</div>
		{/if}
		{#if kind === 'structure'}
			<StructureFunctionChart data={slot.data as StructureFunctionData} />
		{:else}
			<SpectrumChart data={slot.data as SpectrumData} />
		{/if}
	</div>
{:else}
	<div class="space-y-3">
		<div class="text-sm text-gray-500 dark:text-gray-400">{emptyLabel}</div>
		<button
			class="rounded bg-blue-600 px-3 py-1 text-sm text-white hover:bg-blue-700"
			onclick={slot.refresh}
		>
			Reload
		</button>
	</div>
{/if}
