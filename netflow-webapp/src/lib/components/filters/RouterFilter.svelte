<script lang="ts">
	import type { RouterConfig } from '$lib/components/netflow/types.ts';

	interface Props {
		routers: RouterConfig;
		onRouterChange?: (routers: RouterConfig) => void;
	}

	let { routers, onRouterChange }: Props = $props();

	function handleRouterToggle(routerName: keyof RouterConfig) {
		const newRouters = {
			...routers,
			[routerName]: !routers[routerName]
		};
		onRouterChange?.(newRouters);
	}

	function handleCcIr1GwChange() {
		handleRouterToggle('cc-ir1-gw');
	}

	function handleOhIr1GwChange() {
		handleRouterToggle('oh-ir1-gw');
	}
</script>

<div class="router-filter flex flex-wrap items-center gap-4">
	<span class="text-sm font-medium text-gray-700">Routers:</span>

	<label class="flex cursor-pointer items-center gap-2">
		<input
			type="checkbox"
			checked={routers['cc-ir1-gw']}
			onchange={handleCcIr1GwChange}
			class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
		/>
		<span class="text-sm text-gray-700">cc-ir1-gw</span>
	</label>

	<label class="flex cursor-pointer items-center gap-2">
		<input
			type="checkbox"
			checked={routers['oh-ir1-gw']}
			onchange={handleOhIr1GwChange}
			class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
		/>
		<span class="text-sm text-gray-700">oh-ir1-gw</span>
	</label>
</div>
