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

<div class="container mx-auto p-6">
	<div class="mx-auto max-w-2xl text-center">
		<div class="mb-6">
			<h1 class="mb-2 text-4xl font-bold text-gray-900">{page.status}</h1>
			<h2 class="mb-4 text-2xl font-semibold text-gray-700">{getErrorTitle(page.status)}</h2>
		</div>

		{#if page.error?.message}
			<div class="mb-6 rounded-lg border border-red-200 bg-red-50 p-6">
				<div class="text-sm text-red-900">
					{page.error.message}
				</div>
			</div>
		{/if}

		<div class="space-y-3">
			<div class="flex justify-center space-x-4">
				<button
					onclick={() => window.history.back()}
					class="rounded bg-gray-600 px-4 py-2 text-white hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
				>
					Go Back
				</button>
				<a
					href="/"
					class="rounded bg-blue-600 px-4 py-2 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
				>
					Home
				</a>
			</div>

			{#if page.status === 404}
				<div class="mt-4 text-sm text-gray-600">
					<p>
						If you're looking for a specific NetFlow file, try using the timestamp search on the <a
							href="/api/netflow/files"
							class="text-blue-600 hover:underline">Files page</a
						>.
					</p>
				</div>
			{/if}
		</div>
	</div>
</div>
