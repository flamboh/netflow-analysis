<script lang="ts">
	import '../app.css';

	let { children } = $props();
</script>

<div class="font-body bg-surface flex h-dvh flex-col overflow-hidden text-gray-900">
	<header class="shrink-0 border-b border-gray-200 bg-white shadow-sm">
		<div class="mx-auto max-w-[90vw] px-4 sm:px-2 lg:px-4">
			<div class="flex items-center justify-between py-5">
				<div>
					<h1 class="text-2xl font-semibold text-gray-900">
						<a href="/" class="hover:text-cisco-blue transition-colors">NetFlow Analysis</a>
					</h1>
					<p class="mt-0.5 text-sm text-gray-500">An Oregon Networking Research Group Project</p>
				</div>
				<nav class="flex items-center gap-1">
					<a
						href="/"
						class="rounded-md px-3 py-1.5 text-sm font-medium text-gray-600 transition-colors hover:bg-gray-100 hover:text-gray-900"
					>
						Home
					</a>
					<a
						href="/netflow/files"
						class="rounded-md px-3 py-1.5 text-sm font-medium text-gray-600 transition-colors hover:bg-gray-100 hover:text-gray-900"
					>
						Files
					</a>
				</nav>
			</div>
		</div>
	</header>

	<main class="app-shell__main min-h-0 flex-1 overflow-y-auto">
		{@render children()}
	</main>
</div>
