<script lang="ts">
	import '../app.css';
	import { fade } from 'svelte/transition';

	let { children } = $props();

	let isUpdating = $state(false);
	let updateMessage = $state('');
	let showMessage = $state(false);

	async function updateDatabase() {
		if (isUpdating) return;

		isUpdating = true;
		updateMessage = '';
		showMessage = false;

		try {
			const response = await fetch('/api/database/netflow', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				}
			});

			const result = await response.json();

			if (result.success) {
				updateMessage = result.message + (result.logFile ? ` (Logs at: ./logs/flowStats)` : '');
				showMessage = true;
				// Hide success message after 5 seconds
				setTimeout(() => {
					showMessage = false;
				}, 5000);
			} else {
				updateMessage =
					(result.message || 'Update failed') +
					(result.logFile ? ` (Logs at: ./logs/flowStats)` : '');
				showMessage = true;
				// Hide error message after 10 seconds
				setTimeout(() => {
					showMessage = false;
				}, 10000);
			}
		} catch {
			updateMessage = 'Network error during database update';
			showMessage = true;
			setTimeout(() => {
				showMessage = false;
			}, 10000);
		} finally {
			isUpdating = false;
		}
	}
</script>

<header class="border-b bg-white shadow-sm">
	<div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
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
				<button
					onclick={updateDatabase}
					disabled={isUpdating}
					class="rounded-md bg-blue-600 px-3 py-1 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-gray-400"
				>
					{isUpdating ? 'Updating...' : 'Update DB'}
				</button>
			</div>
		</div>
	</div>
</header>

{#if showMessage}
	<div
		class="fixed left-1/2 top-4 z-50 w-full max-w-md -translate-x-1/2 transform px-4"
		transition:fade={{ duration: 300 }}
	>
		<div
			class="rounded-md p-4 shadow-lg {updateMessage.includes('success') ||
			updateMessage.includes('completed')
				? 'border border-green-200 bg-green-50 text-green-800'
				: 'border border-red-200 bg-red-50 text-red-800'}"
		>
			{updateMessage}
		</div>
	</div>
{/if}

<div class="font-body">
	{@render children()}
</div>
