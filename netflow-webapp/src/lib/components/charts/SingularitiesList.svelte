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
		<div
			class="mb-2 rounded-lg border border-slate-200/70 bg-slate-50/65 p-3 text-sm text-slate-600"
		>
			<p>
				Data Source: {data.metadata.dataSource} | Address Type: {data.metadata.addressType}
			</p>
			<p>Point Count: {data.metadata.pointCount}</p>
		</div>

		{#if topSingularities.length > 0}
			<div class="rounded-lg border border-emerald-200 bg-emerald-50/65 p-4">
				<h4 class="mb-2 font-semibold text-emerald-900">Top Singularities (Most Anomalous)</h4>
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-emerald-200">
								<th class="px-2 py-1 text-left">Rank</th>
								<th class="px-2 py-1 text-left">IP Address</th>
								<th class="px-2 py-1 text-left">α (Alpha)</th>
								<th class="px-2 py-1 text-left">Intercept</th>
								<th class="px-2 py-1 text-left">R²</th>
								<th class="px-2 py-1 text-left">Points</th>
							</tr>
						</thead>
						<tbody>
							{#each topSingularities as singularity (singularity.rank)}
								<tr class="border-b border-emerald-100">
									<td class="px-2 py-1">{singularity.rank}</td>
									<td class="px-2 py-1 font-mono">{singularity.address}</td>
									<td class="px-2 py-1">{singularity.alpha.toFixed(4)}</td>
									<td class="px-2 py-1">{singularity.intercept.toFixed(4)}</td>
									<td class="px-2 py-1">{singularity.r2.toFixed(4)}</td>
									<td class="px-2 py-1">{singularity.nPls}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{/if}

		{#if bottomSingularities.length > 0}
			<div class="rounded-lg border border-rose-200 bg-rose-50/70 p-4">
				<h4 class="mb-2 font-semibold text-rose-900">Bottom Singularities (Least Anomalous)</h4>
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-rose-200">
								<th class="px-2 py-1 text-left">Rank</th>
								<th class="px-2 py-1 text-left">IP Address</th>
								<th class="px-2 py-1 text-left">α (Alpha)</th>
								<th class="px-2 py-1 text-left">Intercept</th>
								<th class="px-2 py-1 text-left">R²</th>
								<th class="px-2 py-1 text-left">Points</th>
							</tr>
						</thead>
						<tbody>
							{#each bottomSingularities as singularity (singularity.rank)}
								<tr class="border-b border-rose-100">
									<td class="px-2 py-1">{singularity.rank}</td>
									<td class="px-2 py-1 font-mono">{singularity.address}</td>
									<td class="px-2 py-1">{singularity.alpha.toFixed(4)}</td>
									<td class="px-2 py-1">{singularity.intercept.toFixed(4)}</td>
									<td class="px-2 py-1">{singularity.r2.toFixed(4)}</td>
									<td class="px-2 py-1">{singularity.nPls}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{/if}

		{#if topSingularities.length === 0 && bottomSingularities.length === 0}
			<div class="rounded-lg border border-slate-200 bg-slate-50 p-4">
				<p class="text-slate-600">No singularities data available.</p>
			</div>
		{/if}
	</div>
{:else}
	<div class="w-full">
		<div class="rounded-lg border border-slate-200 bg-slate-50 p-4">
			<p class="text-slate-600">Loading singularities data...</p>
		</div>
	</div>
{/if}
