<script lang="ts">
	import '../app.css';

	let { children } = $props();
</script>

<header class="border-b bg-white shadow-sm">
	<div class="mx-auto max-w-[90vw] px-4 sm:px-2 lg:px-4">
		<div class="flex items-center justify-between py-6">
			<div>
				<h1 class="text-3xl font-bold text-gray-900 hover:underline">
					<a href="/">NetFlow Analysis</a>
				</h1>
				<p class="mt-1 text-sm text-gray-600">An Oregon Network Research Group Project</p>
			</div>
			<div class="flex items-center gap-4">
				<a href="/" class="text-gray-500 hover:text-gray-900">Home</a>
				<a href="/api/netflow/files" class="text-gray-500 hover:text-gray-900">Files</a>
			</div>
		</div>
	</div>
</header>

<div class="font-body">
	{@render children()}
</div>
