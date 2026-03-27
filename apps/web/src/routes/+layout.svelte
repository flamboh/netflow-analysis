<script lang="ts">
	import { resolve } from '$app/paths';
	import '../app.css';
	import { onMount } from 'svelte';
	import { Sun, Moon } from '@lucide/svelte';
	import { theme } from '$lib/stores/theme.svelte';

	let { children } = $props();

	onMount(() => {
		theme.syncFromDom();
	});
</script>

<div
	class="font-body dark:bg-dark-bg flex h-dvh flex-col overflow-hidden bg-gray-100 text-gray-900 dark:text-gray-100"
>
	<header
		class="dark:border-dark-border dark:bg-dark-surface shrink-0 border-b bg-white shadow-sm dark:shadow-none"
	>
		<div class="mx-auto max-w-[90vw] px-4 sm:px-2 lg:px-4">
			<div class="flex items-center justify-between py-6">
				<div>
					<h1 class="text-3xl font-bold text-gray-900 hover:underline dark:text-gray-100">
						<a href={resolve('/')}>NetFlow Analysis</a>
					</h1>
					<p class="mt-1 text-sm text-gray-600 dark:text-gray-400">
						An Oregon Networking Research Group Project
					</p>
				</div>
				<div class="flex items-center gap-4">
					<a
						href={resolve('/')}
						class="text-gray-500 hover:text-gray-900 dark:text-gray-400 dark:hover:text-gray-100"
						>Home</a
					>
					<a
						href={resolve('/netflow/files')}
						class="text-gray-500 hover:text-gray-900 dark:text-gray-400 dark:hover:text-gray-100"
						>Files</a
					>
					<button
						type="button"
						onclick={() => theme.toggle()}
						class="dark:hover:bg-dark-subtle rounded-md p-2 text-gray-500 hover:bg-gray-100 hover:text-gray-900 focus:ring-2 focus:ring-blue-500 focus:outline-none dark:text-gray-400 dark:hover:text-gray-100"
						aria-label={theme.dark ? 'Switch to light mode' : 'Switch to dark mode'}
					>
						{#if theme.dark}
							<Sun size={20} />
						{:else}
							<Moon size={20} />
						{/if}
					</button>
				</div>
			</div>
		</div>
	</header>

	<main class="app-shell__main min-h-0 flex-1 overflow-y-auto">
		{@render children()}
	</main>
</div>
