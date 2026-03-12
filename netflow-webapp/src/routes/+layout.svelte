<script lang="ts">
	import '../app.css';
	import ThemeToggle from '$lib/components/common/ThemeToggle.svelte';

	let { children } = $props();
</script>

<div class="font-body bg-bg text-text-primary flex h-dvh flex-col overflow-hidden">
	<header class="border-border bg-surface shrink-0 border-b shadow-sm">
		<div class="mx-auto max-w-[90vw] px-4 sm:px-2 lg:px-4">
			<div class="flex items-center justify-between py-4">
				<div class="flex items-center gap-3">
					<div class="bg-cisco-blue flex h-9 w-9 items-center justify-center rounded-lg">
						<svg
							class="h-5 w-5 text-white"
							fill="none"
							viewBox="0 0 24 24"
							stroke="currentColor"
							stroke-width="2"
						>
							<path d="M13 10V3L4 14h7v7l9-11h-7z" />
						</svg>
					</div>
					<div>
						<h1 class="text-text-primary hover:text-cisco-blue text-xl font-bold transition-colors">
							<a href="/">NetFlow Analysis</a>
						</h1>
						<p class="text-text-muted text-xs">Oregon Networking Research Group</p>
					</div>
				</div>
				<div class="flex items-center gap-1">
					<a
						href="/"
						class="text-text-secondary hover:bg-surface-hover hover:text-text-primary rounded-lg px-3 py-1.5 text-sm font-medium transition-colors"
					>
						Home
					</a>
					<a
						href="/netflow/files"
						class="text-text-secondary hover:bg-surface-hover hover:text-text-primary rounded-lg px-3 py-1.5 text-sm font-medium transition-colors"
					>
						Files
					</a>
					<div class="border-border ml-2 border-l pl-2">
						<ThemeToggle />
					</div>
				</div>
			</div>
		</div>
	</header>

	<main class="app-shell__main min-h-0 flex-1 overflow-y-auto">
		{@render children()}
	</main>
</div>
