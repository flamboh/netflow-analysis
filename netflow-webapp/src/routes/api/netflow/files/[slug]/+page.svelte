<script lang="ts">
	import type { PageProps } from './$types';
	import StructureFunctionChart from '$lib/components/charts/StructureFunctionChart.svelte';
	import { onMount } from 'svelte';

	let { data }: PageProps = $props();
	let structureFunctionData = $state(new Map());
	let loading = $state(new Map());
	let errors = $state(new Map());

	onMount(async () => {
		// Load structure function data for each router
		for (const record of data.summary) {
			await loadStructureFunctionData(record.router);
		}
	});

	async function loadStructureFunctionData(router: string) {
		loading.set(router, true);
		loading = new Map(loading);
		errors.set(router, '');
		errors = new Map(errors);
		try {
			const response = await fetch(
				`/api/netflow/files/${data.slug}/structure-function?router=${encodeURIComponent(router)}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load structure function data: ${response.statusText}`);
			}
			const result = await response.json();
			console.log(`Structure function data loaded for ${router}:`, result);
			structureFunctionData.set(router, result);
			// Trigger reactivity by creating a new Map
			structureFunctionData = new Map(structureFunctionData);
		} catch (e) {
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			errors.set(router, errorMessage);
			errors = new Map(errors);
			console.error(`Failed to load structure function data for ${router}:`, e);
		} finally {
			loading.set(router, false);
			loading = new Map(loading);
		}
	}
</script>

<div class="container mx-auto p-6">
	<h1 class="mb-4 text-2xl text-black">
		NetFlow File: {data.fileInfo.filename}
	</h1>

	<div class="mb-6 rounded-lg border bg-blue-100 p-4">
		<h2 class="mb-2 text-lg font-semibold">File Information</h2>
		<div class="grid grid-cols-3 gap-4">
			<div>Date: {data.fileInfo.year}-{data.fileInfo.month}-{data.fileInfo.day}</div>
			<div>Time: {data.fileInfo.hour}:{data.fileInfo.minute}</div>
			<div>
				Processed in DB: {new Date(data.summary[0].processed_at).toLocaleString()}
			</div>
		</div>
	</div>

	<div class="space-y-6">
		{#each data.summary as record}
			<div class="rounded-lg border bg-white shadow-sm">
				<!-- Router Data Summary -->
				<div class="bg-cisco-blue rounded-t-lg p-4">
					<h3 class="mb-2 text-lg font-semibold">Router: {record.router}</h3>
					<h3 class="text-md mb-2 font-semibold">
						Absolute Path: <br />
						{record.file_path}
					</h3>
					<div class="grid grid-cols-4 gap-4 text-sm">
						<div>
							<h4 class="font-medium">Flows</h4>
							<p>Total: {record.flows.toLocaleString()}</p>
							<p>TCP: {record.flows_tcp.toLocaleString()}</p>
							<p>UDP: {record.flows_udp.toLocaleString()}</p>
							<p>ICMP: {record.flows_icmp.toLocaleString()}</p>
							<p>Other: {record.flows_other.toLocaleString()}</p>
						</div>
						<div>
							<h4 class="font-medium">Packets</h4>
							<p>Total: {record.packets.toLocaleString()}</p>
							<p>TCP: {record.packets_tcp.toLocaleString()}</p>
							<p>UDP: {record.packets_udp.toLocaleString()}</p>
							<p>ICMP: {record.packets_icmp.toLocaleString()}</p>
							<p>Other: {record.packets_other.toLocaleString()}</p>
						</div>
						<div>
							<h4 class="font-medium">Bytes</h4>
							<p>Total: {record.bytes.toLocaleString()}</p>
							<p>TCP: {record.bytes_tcp.toLocaleString()}</p>
							<p>UDP: {record.bytes_udp.toLocaleString()}</p>
							<p>ICMP: {record.bytes_icmp.toLocaleString()}</p>
							<p>Other: {record.bytes_other.toLocaleString()}</p>
						</div>
						<div>
							<h4 class="font-medium">Timestamps & Metrics</h4>
							<p>First: {new Date(record.first_timestamp * 1000).toLocaleString()}</p>
							<p>Last: {new Date(record.last_timestamp * 1000).toLocaleString()}</p>
							<p>First ms: {record.msec_first}</p>
							<p>Last ms: {record.msec_last}</p>
							<p>Seq failures: {record.sequence_failures.toLocaleString()}</p>
						</div>
					</div>
				</div>

				<!-- MAAD Structure Function Analysis for this Router -->
				<div class="rounded-b-lg bg-purple-50 p-4">
					<h4 class="text-md mb-3 font-semibold text-gray-800">
						MAAD - Multifractal Structure Function Analysis
					</h4>
					{#if loading.get(record.router)}
						<div class="flex items-center justify-center py-6">
							<div class="text-gray-600">Loading structure function analysis...</div>
						</div>
					{:else if errors.get(record.router)}
						<div class="rounded border border-red-200 bg-red-50 p-4 text-red-700">
							<p>Error loading structure function: {errors.get(record.router)}</p>
							<button
								class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
								onclick={() => loadStructureFunctionData(record.router)}
							>
								Retry
							</button>
						</div>
					{:else if structureFunctionData.get(record.router)}
						<div class="mb-2 text-xs text-green-600">
							Debug: Chart data loaded for {record.router} - {structureFunctionData.get(
								record.router
							)?.structureFunction?.length || 0} points
						</div>
						<StructureFunctionChart data={structureFunctionData.get(record.router)} />
					{/if}
				</div>
			</div>
		{/each}
	</div>
</div>
