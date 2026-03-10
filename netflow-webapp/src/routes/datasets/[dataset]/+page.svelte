<script lang="ts">
	import type { PageProps } from './$types';
	import DatasetDashboardPage from '$lib/components/netflow/DatasetDashboardPage.svelte';

	let { data }: PageProps = $props();
</script>

<DatasetDashboardPage
	dataset={data.dataset}
	title={data.title}
	defaultStartDate={data.defaultStartDate}
/>
