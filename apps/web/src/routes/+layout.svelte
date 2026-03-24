<script lang="ts">
	import '../app.css';

	let { children } = $props();
</script>

<div class="font-body flex h-dvh flex-col overflow-hidden bg-gray-100 text-gray-900">
	<header class="shrink-0 border-b bg-white shadow-sm">
		<div class="mx-auto max-w-[90vw] px-4 sm:px-2 lg:px-4">
			<div class="flex items-center justify-between py-6">
				<div>
					<h1 class="text-3xl font-bold text-gray-900 hover:underline">
						<a href="/">NetFlow Analysis</a>
					</h1>
					<p class="mt-1 text-sm text-gray-600">An Oregon Networking Research Group Project</p>
				</div>
				<div class="flex items-center gap-4">
					<a href="/" class="text-gray-500 hover:text-gray-900">Home</a>
					<a href="/netflow/files" class="text-gray-500 hover:text-gray-900">Files</a>
				</div>
			</div>
		</div>
	</header>

	<main class="app-shell__main min-h-0 flex-1 overflow-y-auto">
		{@render children()}
	</main>
</div>
