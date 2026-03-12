<script lang="ts">
	import { page } from '$app/state';
	import { t } from '$lib/i18n';

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

<div class="mx-auto max-w-2xl px-4 py-12">
	<div class="text-center">
		<div class="mb-6">
			<h1 class="mb-2 text-4xl font-semibold text-gray-900">{page.status}</h1>
			<h2 class="mb-4 text-xl font-medium text-gray-600">{getErrorTitle(page.status)}</h2>
		</div>

		{#if page.error?.message}
			<div class="mb-6 rounded-lg border border-red-200 bg-red-50 p-5">
				<p class="text-sm text-red-800">
					{page.error.message}
				</p>
			</div>
		{/if}

		<div class="space-y-3">
			<div class="flex justify-center gap-3">
				<button
					onclick={() => window.history.back()}
					class="focus:ring-cisco-blue/40 rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 focus:ring-2 focus:outline-none"
				>
					Go Back
				</button>
				<a
					href="/"
					class="bg-cisco-blue hover:bg-cisco-blue/90 focus:ring-cisco-blue/40 rounded-md px-4 py-2 text-sm font-medium text-white transition-colors focus:ring-2 focus:outline-none"
				>
					Home
				</a>
			</div>

			{#if page.status === 404}
				<p class="mt-4 text-sm text-gray-500">
					{t('error.404.return_to_prefix')}
					<a href="/" class="text-cisco-blue hover:underline">{t('error.404.dataset_index')}</a>
					{t('error.404.or_use_prefix')}
					<a href="/netflow/files" class="text-cisco-blue hover:underline">
						{t('error.404.file_lookup')}
					</a>
					{t('error.404.timestamp_hint')}
				</p>
			{/if}
		</div>
	</div>
</div>
