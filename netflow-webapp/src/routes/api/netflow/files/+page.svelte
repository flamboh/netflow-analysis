<script lang="ts">
	import { goto } from '$app/navigation';

	let timestamp = $state('');
	let error = $state('');

	function navigateToFile() {
		// Clear previous error
		error = '';

		// Validate timestamp format
		if (!timestamp) {
			error = 'Please enter a timestamp';
			return;
		}

		if (timestamp.length !== 12 || !/^\d{12}$/.test(timestamp)) {
			error = 'Invalid format. Expected 12 digits (YYYYMMDDHHmm)';
			return;
		}

		// Navigate to the file page
		goto(`/api/netflow/files/${timestamp}`);
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter') {
			navigateToFile();
		}
	}
</script>

<div class="app-content-shell">
	<section class="surface-card">
		<div class="surface-card-header">
			<div>
				<h1 class="text-2xl font-bold text-slate-900">NetFlow Files</h1>
				<p class="text-sm text-slate-600">Open a processed file directly from its timestamp key</p>
			</div>
		</div>
		<div class="surface-card-body">
			<div class="rounded-lg border border-cyan-200/70 bg-cyan-50/60 p-4">
				<h2 class="mb-3 text-lg font-semibold text-slate-900">Navigate by Timestamp</h2>
				<div class="flex flex-col gap-3 md:flex-row md:items-end">
					<div class="flex-1">
						<label for="timestamp" class="mb-1 block text-sm font-medium text-slate-700">
							File Timestamp (YYYYMMDDHHmm)
						</label>
						<input
							id="timestamp"
							type="text"
							bind:value={timestamp}
							onkeydown={handleKeydown}
							placeholder="202501011200"
							class="control-input w-full"
							maxlength="12"
						/>
						{#if error}
							<div class="mt-1 text-sm text-red-700">{error}</div>
						{/if}
					</div>
					<button onclick={navigateToFile} class="btn-primary"> Go to File </button>
				</div>
				<p class="mt-2 text-sm text-slate-600">
					Enter the exact 12-digit timestamp from NetFlow filenames, for example
					<code class="rounded bg-slate-100 px-1 py-0.5">nfcapd.202501011200</code>.
				</p>
			</div>
		</div>
	</section>
</div>
