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

<div class="router-filter flex flex-wrap items-center gap-4">
	<span class="text-text-secondary text-sm font-medium">Routers:</span>

	<div class="flex min-h-6 flex-wrap items-center gap-4">
		{#if routerNames.length === 0}
			{#each Array(4) as _, index (index)}
				<span
					class="bg-surface-hover inline-block h-4 w-24 animate-pulse rounded"
					aria-hidden="true"
				></span>
			{/each}
		{:else}
			{#each routerNames as routerName (routerName)}
				<label class="flex cursor-pointer items-center gap-2">
					<input
						type="checkbox"
						checked={routers[routerName]}
						onchange={() => handleRouterToggle(routerName)}
						class="border-border-strong text-cisco-blue focus:ring-cisco-blue h-4 w-4 rounded"
					/>
					<span class="text-text-primary text-sm">{routerName}</span>
				</label>
			{/each}
		{/if}
	</div>
</div>
