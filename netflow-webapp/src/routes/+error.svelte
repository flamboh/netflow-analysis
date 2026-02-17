<script lang="ts">
	import { page } from '$app/state';

	function getErrorTitle(status: number): string {
		switch (status) {
			case 400:
				return 'Bad Request';
			case 404:
				return 'Page Not Found';
			case 500:
				return 'Internal Server Error';
			default:
				return 'Error';
		}
	}
</script>

<div class="app-content-shell">
	<div class="surface-card mx-auto max-w-2xl">
		<div class="surface-card-body text-center">
			<p class="text-sm font-semibold tracking-[0.12em] text-cyan-800/75 uppercase">
				Request Failed
			</p>
			<h1 class="mt-1 text-5xl font-bold text-slate-900">{page.status}</h1>
			<h2 class="mt-1 text-2xl font-semibold text-slate-700">{getErrorTitle(page.status)}</h2>

			{#if page.error?.message}
				<div class="mt-5 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-900">
					{page.error.message}
				</div>
			{/if}

			<div class="mt-6 flex flex-wrap justify-center gap-3">
				<button onclick={() => window.history.back()} class="btn-muted">Go Back</button>
				<a href="/" class="btn-primary">Home</a>
			</div>

			{#if page.status === 404}
				<p class="mt-5 text-sm text-slate-600">
					Looking for a specific NetFlow file? Use timestamp search on the
					<a href="/api/netflow/files" class="font-semibold text-cyan-800 hover:underline">
						Files page
					</a>.
				</p>
			{/if}
		</div>
	</div>
</div>
