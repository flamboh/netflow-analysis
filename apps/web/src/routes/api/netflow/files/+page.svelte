<script lang="ts">
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
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
		goto(resolve(`/netflow/files/${timestamp}?dataset=${encodeURIComponent(selectedDataset)}`));
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
	<h1 class="mb-4 text-2xl text-gray-900 dark:text-gray-100">NetFlow Files</h1>

	<div class="mb-6 rounded-lg border bg-blue-50 p-4 dark:border-blue-900 dark:bg-blue-950">
		<h2 class="mb-3 text-lg font-semibold dark:text-gray-100">Navigate to File by Timestamp</h2>
		<div class="grid gap-3 lg:grid-cols-[14rem_minmax(0,1fr)_auto]">
			<div>
				<label for="dataset" class="mb-1 block text-sm font-medium text-gray-700 dark:text-gray-300"
					>Dataset</label
				>
				<select
					id="dataset"
					bind:value={selectedDataset}
					class="dark:border-dark-border dark:bg-dark-subtle w-full rounded border border-gray-300 px-3 py-2 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none dark:text-gray-100"
				>
					{#if !selectedDataset}
						<option value="">Select a dataset</option>
					{/if}
					{#each datasets as dataset (dataset.datasetId)}
						<option value={dataset.datasetId}>{dataset.label}</option>
					{/each}
				</select>
			</div>
			<div class="min-w-0">
				<label
					for="timestamp"
					class="mb-1 block text-sm font-medium text-gray-700 dark:text-gray-300"
				>
					File Timestamp (YYYYMMDDHHmm)
				</label>
				<input
					id="timestamp"
					type="text"
					bind:value={timestamp}
					onkeydown={handleKeydown}
					placeholder="202601011200"
					class="dark:border-dark-border dark:bg-dark-subtle w-full rounded border border-gray-300 px-3 py-2 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none dark:text-gray-100 dark:placeholder-gray-500"
					maxlength="12"
				/>
				<div
					class={`mt-1 min-h-6 text-sm ${error ? 'text-red-600' : 'text-transparent'}`}
					aria-live="polite"
				>
					{error || ' '}
				</div>
			</div>
			<div class="flex items-start lg:pt-6">
				<button
					onclick={navigateToFile}
					class="w-full rounded bg-blue-600 px-4 py-2 text-white hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 focus:outline-none lg:w-auto"
				>
					Go to File
				</button>
			</div>
		</div>
		<p class="mt-2 text-sm text-gray-600 dark:text-gray-400">
			Choose a dataset, then enter the exact 12-digit timestamp from NetFlow filenames (e.g.,
			`nfcapd.202601011200`).
		</p>
	</div>
</div>
