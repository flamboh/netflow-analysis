<script lang="ts">
	import SingularitiesList from '$lib/components/charts/SingularitiesList.svelte';
	import type { SingularitiesData } from '$lib/types/types';

	type SingularitiesSlot = {
		requested: boolean;
		data: SingularitiesData | null;
		loading: boolean;
		error: string | null;
		requestToken: number;
	};

	let {
		dataset,
		slug,
		router
	}: {
		dataset: string;
		slug: string;
		router: string;
	} = $props();

	let sourceSlot = $state<SingularitiesSlot>({
		requested: false,
		data: null,
		loading: false,
		error: null,
		requestToken: 0
	});
	let destinationSlot = $state<SingularitiesSlot>({
		requested: false,
		data: null,
		loading: false,
		error: null,
		requestToken: 0
	});

	async function readErrorMessage(response: Response, fallback: string) {
		try {
			const payload = (await response.json()) as { error?: string };
			if (payload?.error) {
				return payload.error;
			}
		} catch {
			// Keep fallback.
		}

		return fallback;
	}

	function getSlot(source: boolean) {
		return source ? sourceSlot : destinationSlot;
	}

	async function loadSingularities(source: boolean) {
		const slot = getSlot(source);
		const requestToken = slot.requestToken + 1;

		slot.requested = true;
		slot.requestToken = requestToken;
		slot.loading = true;
		slot.error = null;

		try {
			const response = await fetch(
				`/api/netflow/files/${slug}/singularities?${new URLSearchParams({
					dataset,
					router,
					source: String(source)
				}).toString()}`
			);

			if (requestToken !== slot.requestToken) {
				return;
			}

			if (response.status === 404 || response.status === 422) {
				slot.data = null;
				slot.loading = false;
				slot.error = null;
				return;
			}

			if (!response.ok) {
				throw new Error(await readErrorMessage(response, 'Failed to load singularities data'));
			}

			slot.data = (await response.json()) as SingularitiesData;
			slot.loading = false;
			slot.error = null;
		} catch (error) {
			if (requestToken !== slot.requestToken) {
				return;
			}

			slot.loading = false;
			slot.error = error instanceof Error ? error.message : 'Unknown error occurred';
		}
	}
</script>

<div class="space-y-3">
	<h6 class="text-md font-medium text-gray-700 dark:text-gray-300">Singularities</h6>
	<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
		<div>
			{#if !sourceSlot.requested}
				<button
					class="rounded bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700"
					onclick={() => loadSingularities(true)}
				>
					Load source singularities
				</button>
			{:else if sourceSlot.loading}
				<div class="flex items-center justify-center py-6">
					<div class="text-gray-600 dark:text-gray-400">Loading source singularities...</div>
				</div>
			{:else if sourceSlot.error}
				<div
					class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
				>
					<p>Error loading source singularities: {sourceSlot.error}</p>
					<button
						class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
						onclick={() => loadSingularities(true)}
					>
						Retry
					</button>
				</div>
			{:else if sourceSlot.data}
				<SingularitiesList data={sourceSlot.data} />
			{:else}
				<div class="text-sm text-gray-500 dark:text-gray-400">No source singularities data.</div>
			{/if}
		</div>
		<div>
			{#if !destinationSlot.requested}
				<button
					class="rounded bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700"
					onclick={() => loadSingularities(false)}
				>
					Load destination singularities
				</button>
			{:else if destinationSlot.loading}
				<div class="flex items-center justify-center py-6">
					<div class="text-gray-600 dark:text-gray-400">Loading destination singularities...</div>
				</div>
			{:else if destinationSlot.error}
				<div
					class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
				>
					<p>Error loading destination singularities: {destinationSlot.error}</p>
					<button
						class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
						onclick={() => loadSingularities(false)}
					>
						Retry
					</button>
				</div>
			{:else if destinationSlot.data}
				<SingularitiesList data={destinationSlot.data} />
			{:else}
				<div class="text-sm text-gray-500 dark:text-gray-400">
					No destination singularities data.
				</div>
			{/if}
		</div>
	</div>
</div>
