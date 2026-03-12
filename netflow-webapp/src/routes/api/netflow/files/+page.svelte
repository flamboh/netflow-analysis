<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import {
		getCachedDatasetSummaries,
		loadDatasetSummaries,
		resolveDefaultDatasetId,
		type DatasetSummary
	} from '$lib/datasets';

	let timestamp = $state('');
	let datasets = $state<DatasetSummary[]>(getCachedDatasetSummaries() ?? []);
	let selectedDataset = $state('');
	let error = $state('');

	function resolveSelectedDataset(availableDatasets: DatasetSummary[]): string {
		const requestedDataset = new URL(window.location.href).searchParams.get('dataset')?.trim();
		const fallbackDatasetId = resolveDefaultDatasetId(availableDatasets);

		return requestedDataset &&
			availableDatasets.some((dataset) => dataset.datasetId === requestedDataset)
			? requestedDataset
			: fallbackDatasetId;
	}

	function navigateToFile() {
		error = '';

		if (!timestamp) {
			error = 'Please enter a timestamp';
			return;
		}

		if (timestamp.length !== 12 || !/^\d{12}$/.test(timestamp)) {
			error = 'Invalid format. Expected 12 digits (YYYYMMDDHHmm)';
			return;
		}

		if (!selectedDataset) {
			error = 'Please choose a dataset';
			return;
		}

		goto(`/netflow/files/${timestamp}?dataset=${encodeURIComponent(selectedDataset)}`);
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter') {
			navigateToFile();
		}
	}

	onMount(async () => {
		try {
			if (datasets.length === 0) {
				datasets = await loadDatasetSummaries();
			}
			selectedDataset = resolveSelectedDataset(datasets);
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load datasets';
		}
	});
</script>

<div class="mx-auto max-w-[90vw] px-4 py-8 sm:px-2 lg:px-4">
	<h1 class="text-text-primary mb-4 text-2xl font-semibold">NetFlow Files</h1>

	<div
		class="border-cisco-blue/30 bg-cisco-blue/5 dark:border-cisco-blue/20 dark:bg-cisco-blue/10 mb-6 rounded-xl border p-4"
	>
		<h2 class="text-text-primary mb-3 text-lg font-semibold">Navigate to File by Timestamp</h2>
		<div class="flex items-end space-x-3">
			<div class="w-56">
				<label for="dataset" class="text-text-secondary mb-1 block text-sm font-medium"
					>Dataset</label
				>
				<select
					id="dataset"
					bind:value={selectedDataset}
					class="border-border bg-surface text-text-primary focus:border-cisco-blue focus:ring-cisco-blue w-full rounded-lg border px-3 py-2 focus:ring-1 focus:outline-none"
				>
					{#if !selectedDataset}
						<option value="">Select a dataset</option>
					{/if}
					{#each datasets as dataset (dataset.datasetId)}
						<option value={dataset.datasetId}>{dataset.label}</option>
					{/each}
				</select>
			</div>
			<div class="flex-1">
				<label for="timestamp" class="text-text-secondary mb-1 block text-sm font-medium">
					File Timestamp (YYYYMMDDHHmm)
				</label>
				<input
					id="timestamp"
					type="text"
					bind:value={timestamp}
					onkeydown={handleKeydown}
					placeholder="202601011200"
					class="border-border bg-surface text-text-primary focus:border-cisco-blue focus:ring-cisco-blue w-full rounded-lg border px-3 py-2 focus:ring-1 focus:outline-none"
					maxlength="12"
				/>
				{#if error}
					<div class="mt-1 text-sm text-red-600 dark:text-red-400">{error}</div>
				{/if}
			</div>
			<button
				onclick={navigateToFile}
				class="bg-cisco-blue hover:bg-cisco-blue-dark focus:ring-cisco-blue rounded-lg px-4 py-2 font-medium text-white transition-colors focus:ring-2 focus:ring-offset-2 focus:outline-none"
			>
				Go to File
			</button>
		</div>
		<p class="text-text-muted mt-2 text-sm">
			Choose a dataset, then enter the exact 12-digit timestamp from NetFlow filenames (e.g.,
			`nfcapd.202501011200`).
		</p>
	</div>
</div>
