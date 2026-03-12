<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import {
		getCachedDatasetSummaries,
		loadDatasetSummaries,
		resolveDefaultDatasetId,
		type DatasetSummary
	} from '$lib/datasets';

	let fileDate = $state('');
	let fileTime = $state('00:00');
	let datasets = $state<DatasetSummary[]>(getCachedDatasetSummaries() ?? []);
	let selectedDataset = $state('');
	let error = $state('');

	const timeOptions = $derived(
		Array.from({ length: 288 }, (_, i) => {
			const h = Math.floor(i / 12);
			const m = (i % 12) * 5;
			return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
		})
	);

	function navigateToFile() {
		error = '';
		if (!fileDate) {
			error = 'Select a date';
			return;
		}
		if (!selectedDataset) {
			error = 'Select a dataset';
			return;
		}
		const [h, m] = fileTime.split(':');
		const slug = fileDate.replace(/-/g, '') + h.padStart(2, '0') + m.padStart(2, '0');
		goto(`/netflow/files/${slug}?dataset=${encodeURIComponent(selectedDataset)}`);
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter') navigateToFile();
	}

	onMount(async () => {
		try {
			if (datasets.length === 0) {
				datasets = await loadDatasetSummaries();
			}
			const requestedDataset = new URL(window.location.href).searchParams.get('dataset')?.trim();
			const fallback = resolveDefaultDatasetId(datasets);
			selectedDataset =
				requestedDataset && datasets.some((d) => d.datasetId === requestedDataset)
					? requestedDataset
					: fallback;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load datasets';
		}
	});
</script>

<div class="mx-auto max-w-xl px-4 py-12 sm:px-6">
	<h1 class="text-text-primary text-2xl font-bold">File Lookup</h1>
	<p class="text-text-secondary mt-2 text-sm">
		Jump to a specific 5-minute capture window to view MAAD analysis, spectrum, and singularities
		for that interval.
	</p>

	<div class="border-border bg-surface mt-6 rounded-xl border p-5 shadow-sm">
		<div class="grid gap-4">
			<div>
				<label for="dataset" class="text-text-muted mb-1 block text-xs font-medium">Dataset</label>
				<select
					id="dataset"
					bind:value={selectedDataset}
					class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover w-full rounded-lg border px-3 py-2 text-sm focus:ring-1 focus:outline-none"
				>
					{#if !selectedDataset}
						<option value="">Select a dataset</option>
					{/if}
					{#each datasets as dataset (dataset.datasetId)}
						<option value={dataset.datasetId}>{dataset.label}</option>
					{/each}
				</select>
			</div>

			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="file-date" class="text-text-muted mb-1 block text-xs font-medium">Date</label>
					<input
						id="file-date"
						type="date"
						bind:value={fileDate}
						onkeydown={handleKeydown}
						class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover w-full rounded-lg border px-3 py-2 text-sm focus:ring-1 focus:outline-none"
					/>
				</div>
				<div>
					<label for="file-time" class="text-text-muted mb-1 block text-xs font-medium"
						>Time (5-min interval)</label
					>
					<select
						id="file-time"
						bind:value={fileTime}
						class="border-border bg-surface-alt text-text-primary focus:border-cisco-blue focus:ring-cisco-blue dark:bg-surface-hover w-full rounded-lg border px-3 py-2 text-sm focus:ring-1 focus:outline-none"
					>
						{#each timeOptions as t (t)}
							<option value={t}>{t}</option>
						{/each}
					</select>
				</div>
			</div>

			{#if error}
				<p class="text-sm text-red-600 dark:text-red-400">{error}</p>
			{/if}

			<button
				onclick={navigateToFile}
				class="bg-cisco-blue hover:bg-cisco-blue-dark focus:ring-cisco-blue w-full rounded-lg py-2 text-sm font-medium text-white transition-colors focus:ring-2 focus:ring-offset-2 focus:outline-none"
			>
				Go to File
			</button>
		</div>
	</div>
</div>
