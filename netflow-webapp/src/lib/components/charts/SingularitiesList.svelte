<script lang="ts">
	import type { SingularitiesData } from '$lib/types/types';

	let { data }: { data: SingularitiesData } = $props();

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
			<div
				class="rounded-xl border border-emerald-200 bg-emerald-50 p-4 dark:border-emerald-800 dark:bg-emerald-950/50"
			>
				<h4 class="text-text-primary mb-2 font-semibold">Top Singularities (Most Anomalous)</h4>
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-emerald-200 dark:border-emerald-800">
								<th class="text-text-secondary px-2 py-1 text-left">Rank</th>
								<th class="text-text-secondary px-2 py-1 text-left">IP Address</th>
								<th class="text-text-secondary px-2 py-1 text-left">α (Alpha)</th>
								<th class="text-text-secondary px-2 py-1 text-left">Intercept</th>
								<th class="text-text-secondary px-2 py-1 text-left">R²</th>
								<th class="text-text-secondary px-2 py-1 text-left">Points</th>
							</tr>
						</thead>
						<tbody>
							{#each topSingularities as singularity (singularity.rank)}
								<tr class="border-b border-emerald-100 dark:border-emerald-900">
									<td class="text-text-primary px-2 py-1">{singularity.rank}</td>
									<td class="text-text-primary px-2 py-1 font-mono">{singularity.address}</td>
									<td class="text-text-primary px-2 py-1">{singularity.alpha.toFixed(4)}</td>
									<td class="text-text-primary px-2 py-1">{singularity.intercept.toFixed(4)}</td>
									<td class="text-text-primary px-2 py-1">{singularity.r2.toFixed(4)}</td>
									<td class="text-text-primary px-2 py-1">{singularity.nPls}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{/if}

		{#if bottomSingularities.length > 0}
			<div
				class="rounded-xl border border-red-200 bg-red-50 p-4 dark:border-red-800 dark:bg-red-950/50"
			>
				<h4 class="text-text-primary mb-2 font-semibold">Bottom Singularities (Least Anomalous)</h4>
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-red-200 dark:border-red-800">
								<th class="text-text-secondary px-2 py-1 text-left">Rank</th>
								<th class="text-text-secondary px-2 py-1 text-left">IP Address</th>
								<th class="text-text-secondary px-2 py-1 text-left">α (Alpha)</th>
								<th class="text-text-secondary px-2 py-1 text-left">Intercept</th>
								<th class="text-text-secondary px-2 py-1 text-left">R²</th>
								<th class="text-text-secondary px-2 py-1 text-left">Points</th>
							</tr>
						</thead>
						<tbody>
							{#each bottomSingularities as singularity (singularity.rank)}
								<tr class="border-b border-red-100 dark:border-red-900">
									<td class="text-text-primary px-2 py-1">{singularity.rank}</td>
									<td class="text-text-primary px-2 py-1 font-mono">{singularity.address}</td>
									<td class="text-text-primary px-2 py-1">{singularity.alpha.toFixed(4)}</td>
									<td class="text-text-primary px-2 py-1">{singularity.intercept.toFixed(4)}</td>
									<td class="text-text-primary px-2 py-1">{singularity.r2.toFixed(4)}</td>
									<td class="text-text-primary px-2 py-1">{singularity.nPls}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{/if}

		{#if topSingularities.length === 0 && bottomSingularities.length === 0}
			<div class="border-border bg-surface rounded-xl border p-4">
				<p class="text-text-secondary">No singularities data available.</p>
			</div>
		{/if}
	</div>
{:else}
	<div class="w-full">
		<div class="border-border bg-surface rounded-xl border p-4">
			<p class="text-text-secondary">Loading singularities data...</p>
		</div>
	</div>
{/if}
