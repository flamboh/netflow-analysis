<script lang="ts">
	import NetflowDashboard from '$lib/components/netflow/NetflowDashboard.svelte';
	import type { ChartState } from '$lib/components/netflow/types.ts';
	import { onMount } from 'svelte';

	// Optional: Pass initial state to the dashboard
	const today = new Date().toJSON().slice(0, 10);
	const initialState: Partial<ChartState> = {
		startDate: '2024-03-01',
		endDate: today,
		groupBy: 'date',
		chartType: 'stacked',
		dataOptions: [
			{ label: 'Flows', index: 0, checked: true },
			{ label: 'Flows TCP', index: 1, checked: true },
			{ label: 'Flows UDP', index: 2, checked: true },
			{ label: 'Flows ICMP', index: 3, checked: true },
			{ label: 'Flows Other', index: 4, checked: true },
			{ label: 'Packets', index: 5, checked: false },
			{ label: 'Packets TCP', index: 6, checked: false },
			{ label: 'Packets UDP', index: 7, checked: false },
			{ label: 'Packets ICMP', index: 8, checked: false },
			{ label: 'Packets Other', index: 9, checked: false },
			{ label: 'Bytes', index: 10, checked: false },
			{ label: 'Bytes TCP', index: 11, checked: false },
			{ label: 'Bytes UDP', index: 12, checked: false },
			{ label: 'Bytes ICMP', index: 13, checked: false },
			{ label: 'Bytes Other', index: 14, checked: false }
		]
	};

	let numCharts = $state(1);

	function addChart(e: MouseEvent) {
		e.preventDefault();
		e.stopPropagation();
		numCharts++;
	}

	function removeChart(e: MouseEvent) {
		e.preventDefault();
		e.stopPropagation();
		if (numCharts > 1) {
			numCharts--;
		}
	}
</script>

<svelte:head>
	<title>NetFlow Analysis</title>
	<meta name="description" content="NetFlow analysis and visualization tool" />
</svelte:head>

<div class="min-h-screen bg-gray-100">
	<main class="mx-auto flex max-w-7xl flex-col gap-4 px-4 py-8 sm:px-6 lg:px-8">
		{#each Array(numCharts) as _}
			<NetflowDashboard {initialState} />
		{/each}
		<div class="flex justify-center gap-4">
			{#if numCharts > 1}
				<button
					type="button"
					onclick={removeChart}
					class="flex h-12 w-12 items-center justify-center rounded-lg border bg-gray-50 font-bold"
					>-</button
				>
			{/if}
			<button
				type="button"
				onclick={addChart}
				class="flex h-12 w-12 items-center justify-center rounded-lg border bg-gray-50 font-bold"
				>+</button
			>
		</div>
	</main>
</div>
