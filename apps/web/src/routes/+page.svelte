<script lang="ts">
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import { onMount } from 'svelte';
	import {
		getCachedDatasetSummaries,
		loadDatasetSummaries,
		type DatasetSummary
	} from '$lib/datasets';

	const initialDatasets = getCachedDatasetSummaries();
	let datasets = $state<DatasetSummary[]>(initialDatasets ?? []);
	let loading = $state(initialDatasets === null);
	let error = $state('');

	function openDataset(datasetId: string) {
		goto(resolve('/datasets/[dataset]', { dataset: datasetId }));
	}

	onMount(async () => {
		if (datasets.length > 0) {
			return;
		}

		try {
			datasets = await loadDatasetSummaries();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load datasets';
		} finally {
			loading = false;
		}
	});
</script>

<svelte:head>
	<title>NetFlow Datasets</title>
	<meta name="description" content="Select a NetFlow dataset dashboard" />
</svelte:head>

<main class="mx-auto flex max-w-[90vw] flex-col gap-4 px-4 py-8 sm:px-2 lg:px-4">
	{#if loading}
		<section class="rounded-lg border bg-white p-6 text-gray-500 shadow-sm">
			Loading datasets...
		</section>
	{:else if error}
		<section class="rounded-lg border border-red-200 bg-red-50 p-6 text-red-700 shadow-sm">
			{error}
		</section>
	{:else if datasets.length === 0}
		<section class="rounded-lg border bg-white p-6 text-gray-500 shadow-sm">
			No datasets are configured.
		</section>
	{:else}
		<div class="grid gap-4 md:grid-cols-2">
			{#each datasets as dataset (dataset.datasetId)}
				<button
					type="button"
					class="cursor-pointer rounded-lg border bg-white p-5 text-left shadow-sm transition hover:border-blue-400 hover:shadow"
					onclick={() => openDataset(dataset.datasetId)}
				>
					<h1 class="text-xl font-semibold text-gray-900">{dataset.label}</h1>
					<p class="mt-3 text-sm text-gray-600">
						<span class="font-mono text-gray-500">{dataset.datasetId}</span>
						·
						{dataset.sourceCount} source{dataset.sourceCount === 1 ? '' : 's'}
						·
						{dataset.discoveryMode}
					</p>
				</button>
			{/each}
		</div>
	{/if}
</main>
