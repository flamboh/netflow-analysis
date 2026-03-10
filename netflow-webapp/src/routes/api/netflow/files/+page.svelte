<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';

	const defaultDatasetId = 'uoregon';

	let timestamp = $state('');
	let datasets = $state<Array<{ datasetId: string; label: string }>>([]);
	let selectedDataset = $state('');
	let error = $state('');
	let loadingDatasets = $state(true);

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
		goto(`/api/netflow/files/${timestamp}?dataset=${encodeURIComponent(selectedDataset)}`);
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter') {
			navigateToFile();
		}
	}

	onMount(async () => {
		try {
			const response = await fetch('/api/datasets');
			if (!response.ok) {
				throw new Error(`Failed to load datasets: ${response.statusText}`);
			}

			const result = (await response.json()) as Array<{
				datasetId: string;
				label: string;
			}>;
			datasets = result;

			const fromUrl = new URL(window.location.href).searchParams.get('dataset');
			const fallbackDatasetId =
				result.find((dataset) => dataset.datasetId === defaultDatasetId)?.datasetId ??
				result[0]?.datasetId ??
				'';
			selectedDataset =
				fromUrl && result.some((dataset) => dataset.datasetId === fromUrl)
					? fromUrl
					: fallbackDatasetId;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load datasets';
		} finally {
			loadingDatasets = false;
		}
	});
</script>

<div class="mx-auto max-w-[90vw] px-4 py-8 sm:px-2 lg:px-4">
	<h1 class="mb-4 text-2xl text-black">NetFlow Files</h1>

	<div class="mb-6 rounded-lg border bg-blue-50 p-4">
		<h2 class="mb-3 text-lg font-semibold">Navigate to File by Timestamp</h2>
		<div class="flex items-end space-x-3">
			<div class="w-56">
				<label for="dataset" class="mb-1 block text-sm font-medium text-gray-700">Dataset</label>
				<select
					id="dataset"
					bind:value={selectedDataset}
					class="w-full rounded border border-gray-300 px-3 py-2 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none"
					disabled={loadingDatasets}
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
				<label for="timestamp" class="mb-1 block text-sm font-medium text-gray-700">
					File Timestamp (YYYYMMDDHHmm)
				</label>
				<input
					id="timestamp"
					type="text"
					bind:value={timestamp}
					onkeydown={handleKeydown}
					placeholder="202501011200"
					class="w-full rounded border border-gray-300 px-3 py-2 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none"
					maxlength="12"
				/>
				{#if error}
					<div class="mt-1 text-sm text-red-600">{error}</div>
				{/if}
			</div>
			<button
				onclick={navigateToFile}
				class="rounded bg-blue-600 px-4 py-2 text-white hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 focus:outline-none"
			>
				Go to File
			</button>
		</div>
		<p class="mt-2 text-sm text-gray-600">
			Choose a dataset, then enter the exact 12-digit timestamp from NetFlow filenames (e.g.,
			`nfcapd.202501011200`).
		</p>
	</div>
</div>
