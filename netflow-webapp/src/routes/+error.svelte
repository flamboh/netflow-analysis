<script lang="ts">
	import { page } from '$app/state';
</script>

<h1 class="text-white">Error: {page.status} {page.error?.message}</h1>
