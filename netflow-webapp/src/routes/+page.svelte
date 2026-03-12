<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import {
		getCachedDatasetSummaries,
		loadDatasetSummaries,
		resolveDefaultDatasetId,
		type DatasetSummary
	} from '$lib/datasets';

	const initialDatasets = getCachedDatasetSummaries();
	let datasets = $state<DatasetSummary[]>(initialDatasets ?? []);
	let loading = $state(initialDatasets === null);
	let error = $state('');

	let fileDate = $state('');
	let fileTime = $state('00:00');
	let fileDataset = $state('');
	let fileError = $state('');

	function openDataset(datasetId: string) {
		goto(`/datasets/${datasetId}`);
	}

	function navigateToFile() {
		fileError = '';
		if (!fileDate) {
			fileError = 'Select a date';
			return;
		}
		if (!fileDataset) {
			fileError = 'Select a dataset';
			return;
		}
		const [h, m] = fileTime.split(':');
		const slug = fileDate.replace(/-/g, '') + h.padStart(2, '0') + m.padStart(2, '0');
		goto(`/netflow/files/${slug}?dataset=${encodeURIComponent(fileDataset)}`);
	}

	const timeOptions = $derived(
		Array.from({ length: 288 }, (_, i) => {
			const h = Math.floor(i / 12);
			const m = (i % 12) * 5;
			return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
		})
	);

	onMount(async () => {
		if (datasets.length > 0) {
			fileDataset = resolveDefaultDatasetId(datasets);
			return;
		}
		try {
			datasets = await loadDatasetSummaries();
			fileDataset = resolveDefaultDatasetId(datasets);
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load datasets';
		} finally {
			loading = false;
		}
	});
</script>

<svelte:head>
	<title>NetFlow Analysis</title>
	<meta name="description" content="NetFlow analysis and visualization" />
</svelte:head>

<main class="mx-auto flex max-w-4xl flex-col gap-10 px-4 py-10 sm:px-6">
	<section class="text-center">
		<h1 class="text-text-primary text-3xl font-bold tracking-tight sm:text-4xl">
			NetFlow Analysis
		</h1>
		<p class="text-text-secondary mx-auto mt-3 max-w-xl text-base">
			Explore traffic patterns, IP distributions, protocol breakdowns, and multifractal spectrum
			analysis across network capture datasets.
		</p>
	</section>

	<section>
		<h2 class="text-text-muted mb-3 text-sm font-semibold tracking-wider uppercase">Datasets</h2>
		{#if loading}
			<div class="grid gap-4 sm:grid-cols-2">
				{#each [0, 1] as i (i)}
					<div class="border-border bg-surface animate-pulse rounded-xl border p-5">
						<div class="bg-surface-hover mb-3 h-5 w-32 rounded"></div>
						<div class="bg-surface-hover h-4 w-48 rounded"></div>
					</div>
				{/each}
			</div>
		{:else if error}
			<div
				class="rounded-xl border border-red-300 bg-red-50 p-5 text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-400"
			>
				{error}
			</div>
		{:else if datasets.length === 0}
			<div class="border-border bg-surface text-text-secondary rounded-xl border p-5">
				No datasets configured. Check your environment variables.
			</div>
		{:else}
			<div class="grid gap-4 sm:grid-cols-2">
				{#each datasets as dataset (dataset.datasetId)}
					<button
						type="button"
						class="group border-border bg-surface hover:border-cisco-blue flex cursor-pointer flex-col rounded-xl border p-5 text-left shadow-sm transition-all hover:shadow-md"
						onclick={() => openDataset(dataset.datasetId)}
					>
						<div class="flex items-start justify-between">
							<h3
								class="text-text-primary group-hover:text-cisco-blue text-lg font-semibold transition-colors"
							>
								{dataset.label}
							</h3>
							{#if dataset.isDefault}
								<span
									class="bg-cisco-blue/10 text-cisco-blue rounded-full px-2 py-0.5 text-xs font-medium"
								>
									Default
								</span>
							{/if}
						</div>
						<div class="text-text-muted mt-3 flex flex-wrap items-center gap-2 text-xs">
							<span class="bg-surface-alt dark:bg-surface-hover rounded-md px-2 py-0.5 font-mono">
								{dataset.datasetId}
							</span>
							<span class="bg-surface-alt dark:bg-surface-hover rounded-md px-2 py-0.5">
								{dataset.sourceCount} source{dataset.sourceCount === 1 ? '' : 's'}
							</span>
							<span class="bg-surface-alt dark:bg-surface-hover rounded-md px-2 py-0.5">
								{dataset.discoveryMode}
							</span>
						</div>
						<p class="text-text-muted mt-3 text-xs">
							Data from <span class="text-text-secondary font-medium"
								>{dataset.defaultStartDate}</span
							>
						</p>
					</button>
				{/each}
			</div>
		{/if}
	</section>

	<section>
		<h2 class="text-text-muted mb-3 text-sm font-semibold tracking-wider uppercase">File Lookup</h2>
		<div class="border-border bg-surface rounded-xl border p-5 shadow-sm">
			<p class="text-text-secondary mb-4 text-sm">
				Jump to a specific 5-minute capture window to view its MAAD analysis, spectrum, and
				singularities.
			</p>
			<div class="flex flex-wrap items-end gap-3">
				<div class="w-44">
					<label for="home-dataset" class="text-text-muted mb-1 block text-xs font-medium"
						>Dataset</label
					>
					<select
						id="home-dataset"
						bind:value={fileDataset}
						class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover w-full rounded-lg border px-3 py-2 text-sm focus:ring-1 focus:outline-none"
					>
						{#each datasets as ds (ds.datasetId)}
							<option value={ds.datasetId}>{ds.label}</option>
						{/each}
					</select>
				</div>
				<div class="w-40">
					<label for="home-date" class="text-text-muted mb-1 block text-xs font-medium">Date</label>
					<input
						id="home-date"
						type="date"
						bind:value={fileDate}
						class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover w-full rounded-lg border px-3 py-2 text-sm focus:ring-1 focus:outline-none"
					/>
				</div>
				<div class="w-28">
					<label for="home-time" class="text-text-muted mb-1 block text-xs font-medium">Time</label>
					<select
						id="home-time"
						bind:value={fileTime}
						class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover w-full rounded-lg border px-3 py-2 text-sm focus:ring-1 focus:outline-none"
					>
						{#each timeOptions as t (t)}
							<option value={t}>{t}</option>
						{/each}
					</select>
				</div>
				<button
					onclick={navigateToFile}
					class="bg-cisco-blue hover:bg-cisco-blue-dark rounded-lg px-5 py-2 text-sm font-medium text-white transition-colors"
				>
					Go
				</button>
			</div>
			{#if fileError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{fileError}</p>
			{/if}
		</div>
	</section>
</main>
