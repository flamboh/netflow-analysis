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
		// Clear previous error
		error = '';

		// Validate timestamp format
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

		// Navigate to the file page
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
	<h1 class="mb-4 text-xl font-semibold text-gray-900">NetFlow Files</h1>

	<div class="rounded-lg border border-gray-200 bg-white p-5 shadow-sm">
		<h2 class="mb-3 text-sm font-semibold text-gray-900">Navigate to File by Timestamp</h2>
		<div class="flex items-end gap-3">
			<div class="w-56">
				<label for="dataset" class="mb-1 block text-sm font-medium text-gray-600">Dataset</label>
				<select
					id="dataset"
					bind:value={selectedDataset}
					class="focus:border-cisco-blue focus:ring-cisco-blue/30 w-full rounded-md border border-gray-300 px-3 py-2 text-sm transition-colors focus:ring-1 focus:outline-none"
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
				<label for="timestamp" class="mb-1 block text-sm font-medium text-gray-600">
					File Timestamp (YYYYMMDDHHmm)
				</label>
				<input
					id="timestamp"
					type="text"
					bind:value={timestamp}
					onkeydown={handleKeydown}
					placeholder="202601011200"
					class="focus:border-cisco-blue focus:ring-cisco-blue/30 w-full rounded-md border border-gray-300 px-3 py-2 text-sm transition-colors focus:ring-1 focus:outline-none"
					maxlength="12"
				/>
				{#if error}
					<p class="mt-1 text-sm text-red-600">{error}</p>
				{/if}
			</div>
			<button
				onclick={navigateToFile}
				class="bg-cisco-blue hover:bg-cisco-blue/90 focus:ring-cisco-blue/40 rounded-md px-4 py-2 text-sm font-medium text-white transition-colors focus:ring-2 focus:outline-none"
			>
				Go to File
			</button>
		</div>
		<p class="mt-3 text-sm text-gray-500">
			Choose a dataset, then enter the exact 12-digit timestamp from NetFlow filenames (e.g.,
			<code class="rounded bg-gray-100 px-1 py-0.5 text-xs text-gray-600">nfcapd.202501011200</code
			>).
		</p>
	</div>
</div>
