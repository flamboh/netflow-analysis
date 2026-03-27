<script lang="ts">
	let {
		message,
		tone = 'neutral',
		actionLabel,
		action
	}: {
		message: string;
		tone?: 'neutral' | 'danger';
		actionLabel?: string;
		action?: () => void;
	} = $props();

	const classes = $derived(
		tone === 'danger'
			? 'rounded-lg border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400'
			: 'dark:border-dark-border dark:bg-dark-surface rounded-lg border bg-white p-4 text-gray-600 shadow-sm dark:text-gray-400'
	);
</script>

<div class={classes}>
	<p>{message}</p>
	{#if action && actionLabel}
		<button
			type="button"
			class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
			onclick={action}
		>
			{actionLabel}
		</button>
	{/if}
</div>
