<script lang="ts">
	import '../app.css';

	let { children } = $props();
</script>

<div class="font-body">
	{@render children()}
</div>
