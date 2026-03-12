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

<div class="container mx-auto p-6">
	<div class="mx-auto max-w-2xl text-center">
		<div class="mb-6">
			<h1 class="text-text-primary mb-2 text-4xl font-bold">{page.status}</h1>
			<h2 class="text-text-secondary mb-4 text-2xl font-semibold">{getErrorTitle(page.status)}</h2>
		</div>

		{#if page.error?.message}
			<div
				class="mb-6 rounded-xl border border-red-300 bg-red-50 p-6 dark:border-red-800 dark:bg-red-950"
			>
				<div class="text-sm text-red-900 dark:text-red-400">
					{page.error.message}
				</div>
			</div>
		{/if}

		<div class="space-y-3">
			<div class="flex justify-center space-x-4">
				<button
					onclick={() => window.history.back()}
					class="bg-surface-hover text-text-primary hover:bg-border focus:ring-cisco-blue rounded-lg px-4 py-2 transition-colors focus:ring-2 focus:ring-offset-2 focus:outline-none"
				>
					Go Back
				</button>
				<a
					href="/"
					class="bg-cisco-blue hover:bg-cisco-blue-dark focus:ring-cisco-blue rounded-lg px-4 py-2 text-white transition-colors focus:ring-2 focus:ring-offset-2 focus:outline-none"
				>
					Home
				</a>
			</div>

			{#if page.status === 404}
				<div class="text-text-secondary mt-4 text-sm">
					<p>
						{t('error.404.return_to_prefix')}
						<a href="/" class="text-cisco-blue hover:underline">{t('error.404.dataset_index')}</a>
						{t('error.404.or_use_prefix')}
						<a href="/netflow/files" class="text-cisco-blue hover:underline">
							{t('error.404.file_lookup')}
						</a>
						{t('error.404.timestamp_hint')}
					</p>
				</div>
			{/if}
		</div>
	</div>
</div>
