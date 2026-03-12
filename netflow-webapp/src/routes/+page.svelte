<script lang="ts">
	import { goto } from '$app/navigation';
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
		goto(`/datasets/${datasetId}`);
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
		<section class="border-border bg-surface text-text-secondary rounded-xl border p-6 shadow-sm">
			Loading datasets...
		</section>
	{:else if error}
		<section
			class="rounded-xl border border-red-300 bg-red-50 p-6 text-red-700 shadow-sm dark:border-red-800 dark:bg-red-950 dark:text-red-400"
		>
			{error}
		</section>
	{:else if datasets.length === 0}
		<section class="border-border bg-surface text-text-secondary rounded-xl border p-6 shadow-sm">
			No datasets are configured.
		</section>
	{:else}
		<div class="grid gap-4 md:grid-cols-2">
			{#each datasets as dataset (dataset.datasetId)}
				<button
					type="button"
					class="group border-border bg-surface hover:border-cisco-blue cursor-pointer rounded-xl border p-5 text-left shadow-sm transition-all hover:shadow-md"
					onclick={() => openDataset(dataset.datasetId)}
				>
					<h1
						class="text-text-primary group-hover:text-cisco-blue text-xl font-semibold transition-colors"
					>
						{dataset.label}
					</h1>
					<p class="text-text-secondary mt-3 text-sm">
						<span class="text-text-muted font-mono">{dataset.datasetId}</span>
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
