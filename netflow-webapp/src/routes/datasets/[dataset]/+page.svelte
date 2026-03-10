<script lang="ts">
	import { onMount } from 'svelte';
	import type { PageProps } from './$types';
	import DatasetDashboardPage from '$lib/components/netflow/DatasetDashboardPage.svelte';
	import { getCachedDatasetSummaries, loadDatasetSummaries } from '$lib/datasets';

	let { data }: PageProps = $props();
	let title = $state(data.dataset);

	onMount(async () => {
		try {
			const datasets = getCachedDatasetSummaries() ?? (await loadDatasetSummaries());
			title = datasets.find((dataset) => dataset.datasetId === data.dataset)?.label ?? data.dataset;
		} catch {
			title = data.dataset;
		}
	});
</script>

<DatasetDashboardPage dataset={data.dataset} {title} />
