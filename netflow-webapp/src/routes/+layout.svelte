<script lang="ts">
	import '../app.css';
	import { page } from '$app/state';
	import ThemeToggle from '$lib/components/common/ThemeToggle.svelte';

	let { children } = $props();

	const datasetId = $derived(
		page.url.pathname.startsWith('/datasets/') ? (page.url.pathname.split('/')[2] ?? '') : ''
	);
</script>

<div class="font-body bg-bg text-text-primary flex h-dvh flex-col overflow-hidden">
	<header class="border-border bg-surface shrink-0 border-b">
		<div class="mx-auto max-w-[90vw] px-4 sm:px-2 lg:px-4">
			<div class="flex h-12 items-center justify-between">
				<div class="flex items-center gap-6">
					<a href="/" class="flex items-center gap-2.5 transition-opacity hover:opacity-80">
						<div class="bg-cisco-blue flex h-7 w-7 items-center justify-center rounded-md">
							<svg
								class="h-4 w-4 text-white"
								fill="none"
								viewBox="0 0 24 24"
								stroke="currentColor"
								stroke-width="2.5"
							>
								<path d="M13 10V3L4 14h7v7l9-11h-7z" />
							</svg>
						</div>
						<span class="text-text-primary text-sm font-semibold">NetFlow</span>
					</a>

					{#if datasetId}
						<nav class="text-text-muted flex items-center gap-1 text-xs">
							<span>/</span>
							<span class="text-text-secondary font-medium">{decodeURIComponent(datasetId)}</span>
						</nav>
					{/if}
				</div>

				<div class="flex items-center gap-1">
					<a
						href="/netflow/files"
						class="text-text-secondary hover:bg-surface-hover hover:text-text-primary rounded-md px-2.5 py-1 text-xs font-medium transition-colors"
					>
						File Lookup
					</a>
					<ThemeToggle />
				</div>
			</div>
		</div>
	</header>

	<main class="app-shell__main min-h-0 flex-1 overflow-y-auto">
		{@render children()}
	</main>
</div>
