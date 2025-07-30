<script lang="ts">
	import NetflowDashboard from '$lib/components/netflow/NetflowDashboard.svelte';
	import type { ChartState } from '$lib/components/netflow/types.ts';

	// Optional: Pass initial state to the dashboard
	const today = new Date().toJSON().slice(0, 10);
	const initialState: Partial<ChartState> = {
		startDate: '2024-03-01',
		endDate: today,
		routers: {
			'cc-ir1-gw': true,
			'oh-ir1-gw': true
		},
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
</script>

<svelte:head>
	<title>NetFlow Analysis</title>
	<meta name="description" content="Network flow analysis and visualization tool" />
</svelte:head>

<div class="min-h-screen bg-gray-100">
	<header class="border-b bg-white shadow-sm">
		<div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
			<div class="flex items-center justify-between py-6">
				<div>
					<h1 class="text-3xl font-bold text-gray-900">NetFlow Analysis</h1>
					<p class="mt-1 text-sm text-gray-600">
						University of Oregon Network Traffic Visualization
					</p>
				</div>
			</div>
		</div>
	</header>

	<main class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
		<NetflowDashboard {initialState} />
	</main>
</div>
