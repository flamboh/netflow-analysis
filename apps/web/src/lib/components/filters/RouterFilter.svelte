<script lang="ts">
	import type { RouterConfig } from '$lib/components/netflow/types.ts';

	interface Props {
		routers: RouterConfig;
		onRouterChange?: (routers: RouterConfig) => void;
	}

	let { routers, onRouterChange }: Props = $props();
	const routerNames = $derived(Object.keys(routers));

	function handleRouterToggle(routerName: string) {
		const newRouters = {
			...routers,
			[routerName]: !routers[routerName]
		};
		onRouterChange?.(newRouters);
	}
</script>

<div class="router-filter flex flex-wrap items-center gap-3">
	<span class="text-sm font-medium text-gray-700 dark:text-gray-300">Routers:</span>

	<div class="flex min-h-0 flex-wrap items-center gap-3">
		{#if routerNames.length === 0}
			{#each Array(4) as _, index (index)}
				<span class="inline-block h-4 w-24 animate-pulse rounded bg-gray-200 dark:bg-dark-border" aria-hidden="true"
				></span>
			{/each}
		{:else}
			{#each routerNames as routerName (routerName)}
				<label class="flex cursor-pointer items-center gap-2 rounded-md px-1 py-1 hover:bg-gray-50 dark:hover:bg-dark-subtle">
					<input
						type="checkbox"
						checked={routers[routerName]}
						onchange={() => handleRouterToggle(routerName)}
						class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
					/>
					<span class="text-sm text-gray-700 dark:text-gray-300">{routerName}</span>
				</label>
			{/each}
		{/if}
	</div>
</div>
