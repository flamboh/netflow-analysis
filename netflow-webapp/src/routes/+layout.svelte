<script lang="ts">
	import '../app.css';

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
			const response = await fetch('/api/database', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				}
			});

			const result = await response.json();

			if (result.success) {
				updateMessage = result.message + (result.logFile ? ` (Log: ${result.logFile})` : '');
				showMessage = true;
				// Hide success message after 5 seconds
				setTimeout(() => {
					showMessage = false;
				}, 5000);
			} else {
				updateMessage =
					(result.message || 'Update failed') + (result.logFile ? ` (Log: ${result.logFile})` : '');
				showMessage = true;
				// Hide error message after 10 seconds
				setTimeout(() => {
					showMessage = false;
				}, 10000);
			}
		} catch (error) {
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
				<a href="files" class="text-gray-500 hover:text-gray-900">Files</a>
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
	<div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
		<div
			class="mt-4 rounded-md p-4 {updateMessage.includes('success') ||
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
