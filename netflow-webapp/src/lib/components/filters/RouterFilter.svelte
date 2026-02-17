<script lang="ts">
	import type { RouterConfig } from '$lib/components/netflow/types.ts';

	interface Props {
		routers: RouterConfig;
		onRouterChange?: (routers: RouterConfig) => void;
	}

	let { routers, onRouterChange }: Props = $props();

	function handleRouterToggle(routerName: string) {
		const newRouters = {
			...routers,
			[routerName]: !routers[routerName]
		};
		onRouterChange?.(newRouters);
	}

	function setAllRouters(enabled: boolean) {
		const newRouters = Object.fromEntries(
			Object.keys(routers).map((routerName) => [routerName, enabled])
		);
		onRouterChange?.(newRouters);
	}

	const selectedCount = $derived(Object.values(routers).filter(Boolean).length);
</script>

<div class="router-filter">
	<div class="mb-3 flex flex-wrap items-center justify-between gap-2">
		<h3 class="text-sm font-semibold tracking-[0.08em] text-slate-700 uppercase">
			Routers ({selectedCount}/{Object.keys(routers).length})
		</h3>
		<div class="flex items-center gap-2">
			<button type="button" class="btn-muted" onclick={() => setAllRouters(true)}>All</button>
			<button type="button" class="btn-muted" onclick={() => setAllRouters(false)}>None</button>
		</div>
	</div>

	{#if Object.keys(routers).length === 0}
		<p class="text-sm text-slate-500">Loading router list...</p>
	{:else}
		<div class="flex flex-wrap items-center gap-2">
			{#each Object.keys(routers) as routerName (routerName)}
				<label
					class="flex cursor-pointer items-center gap-2 rounded-lg border border-slate-200/80 bg-white px-3 py-2"
				>
					<input
						type="checkbox"
						checked={routers[routerName]}
						onchange={() => handleRouterToggle(routerName)}
						class="h-4 w-4 rounded border-slate-300 text-cyan-700 focus:ring-cyan-700"
					/>
					<span class="text-sm text-slate-700">{routerName}</span>
				</label>
			{/each}
		</div>
	{/if}
</div>
