<script lang="ts">
	import type { SingularitiesData } from '$lib/types/types';

	let { data }: { data: SingularitiesData } = $props();

	// Separate top and bottom singularities
	const topSingularities = $derived(
		data ? data.singularities.filter((s) => s.rank.startsWith('top')) : []
	);
	const bottomSingularities = $derived(
		data ? data.singularities.filter((s) => s.rank.startsWith('bottom')) : []
	);
</script>

{#if data}
	<div class="w-full space-y-4">
		{#if topSingularities.length > 0}
			<div class="rounded-md border border-gray-200 bg-white p-4">
				<h4 class="mb-2 text-sm font-semibold text-gray-900">Top Singularities (Most Anomalous)</h4>
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-gray-200">
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">Rank</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">IP Address</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">α (Alpha)</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">Intercept</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">R²</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">Points</th>
							</tr>
						</thead>
						<tbody>
							{#each topSingularities as singularity (singularity.rank)}
								<tr class="border-b border-gray-100">
									<td class="px-2 py-1.5 text-gray-700">{singularity.rank}</td>
									<td class="px-2 py-1.5 font-mono text-gray-700">{singularity.address}</td>
									<td class="px-2 py-1.5 text-gray-700">{singularity.alpha.toFixed(4)}</td>
									<td class="px-2 py-1.5 text-gray-700">{singularity.intercept.toFixed(4)}</td>
									<td class="px-2 py-1.5 text-gray-700">{singularity.r2.toFixed(4)}</td>
									<td class="px-2 py-1.5 text-gray-700">{singularity.nPls}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{/if}

		{#if bottomSingularities.length > 0}
			<div class="rounded-md border border-gray-200 bg-white p-4">
				<h4 class="mb-2 text-sm font-semibold text-gray-900">
					Bottom Singularities (Least Anomalous)
				</h4>
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-gray-200">
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">Rank</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">IP Address</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">α (Alpha)</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">Intercept</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">R²</th>
								<th class="px-2 py-1.5 text-left text-xs font-medium text-gray-500">Points</th>
							</tr>
						</thead>
						<tbody>
							{#each bottomSingularities as singularity (singularity.rank)}
								<tr class="border-b border-gray-100">
									<td class="px-2 py-1.5 text-gray-700">{singularity.rank}</td>
									<td class="px-2 py-1.5 font-mono text-gray-700">{singularity.address}</td>
									<td class="px-2 py-1.5 text-gray-700">{singularity.alpha.toFixed(4)}</td>
									<td class="px-2 py-1.5 text-gray-700">{singularity.intercept.toFixed(4)}</td>
									<td class="px-2 py-1.5 text-gray-700">{singularity.r2.toFixed(4)}</td>
									<td class="px-2 py-1.5 text-gray-700">{singularity.nPls}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{/if}

		{#if topSingularities.length === 0 && bottomSingularities.length === 0}
			<div class="rounded-md border border-gray-200 bg-gray-50 p-4">
				<p class="text-sm text-gray-500">No singularities data available.</p>
			</div>
		{/if}
	</div>
{:else}
	<div class="w-full">
		<div class="rounded-md border border-gray-200 bg-gray-50 p-4">
			<p class="text-sm text-gray-400">Loading singularities data...</p>
		</div>
	</div>
{/if}
