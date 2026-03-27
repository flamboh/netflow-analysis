<script lang="ts">
	let { count }: { count: number } = $props();
	const skeletons = $derived(Array.from({ length: count }, (_, index) => index));
</script>

<div class="space-y-2">
	{#each skeletons as skeletonId (`summary-skeleton-${skeletonId}`)}
		<div
			class="dark:border-dark-border dark:bg-dark-surface animate-pulse rounded-lg border bg-white shadow-sm"
		>
			<div class="bg-cisco-blue dark:bg-dark-subtle rounded-t-lg p-4">
				<div class="mb-2 h-7 w-56 rounded bg-white/70"></div>
				<div class="mb-4 h-5 w-[34rem] max-w-full rounded bg-white/60"></div>
				<div class="grid grid-cols-1 gap-2 text-sm md:grid-cols-2">
					<div>
						<div class="mb-2 h-5 w-40 rounded bg-white/60"></div>
						<div class="mb-1 h-4 w-20 rounded bg-white/60"></div>
						<div class="h-4 w-20 rounded bg-white/60"></div>
					</div>
					<div>
						<div class="mb-2 h-5 w-44 rounded bg-white/60"></div>
						<div class="mb-1 h-4 w-20 rounded bg-white/60"></div>
						<div class="h-4 w-20 rounded bg-white/60"></div>
					</div>
				</div>
				<div class="mt-4 grid grid-cols-4 gap-4 text-sm">
					{#each [0, 1, 2, 3] as columnIndex (`summary-column-${skeletonId}-${columnIndex}`)}
						<div>
							<div class="mb-2 h-5 w-28 rounded bg-white/60"></div>
							<div class="mb-1 h-4 w-24 rounded bg-white/60"></div>
							<div class="mb-1 h-4 w-20 rounded bg-white/60"></div>
							<div class="mb-1 h-4 w-24 rounded bg-white/60"></div>
							<div class="h-4 w-20 rounded bg-white/60"></div>
						</div>
					{/each}
				</div>
			</div>
			<div class="space-y-4 p-4">
				<div class="dark:bg-dark-border h-6 w-36 rounded bg-gray-200"></div>
				<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
					<div class="space-y-3">
						<div class="dark:bg-dark-border h-5 w-44 rounded bg-gray-200"></div>
						<div class="dark:bg-dark-subtle h-4 w-96 max-w-full rounded bg-gray-100"></div>
						<div class="dark:bg-dark-subtle h-96 rounded bg-gray-100"></div>
					</div>
					<div class="space-y-3">
						<div class="dark:bg-dark-border h-5 w-52 rounded bg-gray-200"></div>
						<div class="dark:bg-dark-subtle h-4 w-96 max-w-full rounded bg-gray-100"></div>
						<div class="dark:bg-dark-subtle h-96 rounded bg-gray-100"></div>
					</div>
				</div>
				<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
					<div class="space-y-3">
						<div class="dark:bg-dark-border h-5 w-44 rounded bg-gray-200"></div>
						<div class="dark:bg-dark-subtle h-4 w-96 max-w-full rounded bg-gray-100"></div>
						<div class="dark:bg-dark-subtle h-96 rounded bg-gray-100"></div>
					</div>
					<div class="space-y-3">
						<div class="dark:bg-dark-border h-5 w-52 rounded bg-gray-200"></div>
						<div class="dark:bg-dark-subtle h-4 w-96 max-w-full rounded bg-gray-100"></div>
						<div class="dark:bg-dark-subtle h-96 rounded bg-gray-100"></div>
					</div>
				</div>
				<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
					<div class="space-y-3">
						<div class="dark:bg-dark-border h-5 w-48 rounded bg-gray-200"></div>
						<div class="dark:bg-dark-subtle h-56 rounded bg-gray-100"></div>
					</div>
					<div class="space-y-3">
						<div class="dark:bg-dark-border h-5 w-56 rounded bg-gray-200"></div>
						<div class="dark:bg-dark-subtle h-56 rounded bg-gray-100"></div>
					</div>
				</div>
			</div>
		</div>
	{/each}
</div>
