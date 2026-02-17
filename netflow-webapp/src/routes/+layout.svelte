<script lang="ts">
	import { page } from '$app/state';
	import '../app.css';

	let { children } = $props();
</script>

<header class="sticky top-0 z-20 border-b border-slate-200/70 bg-white/85 backdrop-blur">
	<div class="mx-auto w-full max-w-7xl px-4 py-4 sm:px-6 lg:px-8">
		<div class="flex flex-wrap items-center justify-between gap-4">
			<div>
				<h1 class="text-2xl font-bold text-slate-900 sm:text-3xl">
					<a href="/">NetFlow Analysis</a>
				</h1>
				<p class="mt-1 text-sm text-slate-600">Oregon Network Research Group</p>
			</div>
			<div
				class="flex items-center gap-2 rounded-full border border-slate-200/70 bg-slate-50/85 p-1"
			>
				<a href="/" class={`nav-link ${page.url.pathname === '/' ? 'nav-link-active' : ''}`}>Home</a
				>
				<a
					href="/api/netflow/files"
					class={`nav-link ${page.url.pathname.startsWith('/api/netflow/files') ? 'nav-link-active' : ''}`}
				>
					Files
				</a>
			</div>
		</div>
	</div>
</header>

<div class="font-body">
	{@render children()}
</div>
