<script lang="ts">
	import type { PageProps } from './$types';
	import ApiNetflowFilePage from '../../../api/netflow/files/[slug]/+page.svelte';

	let { data }: PageProps = $props();
</script>

<ApiNetflowFilePage {data} params={{ slug: data.slug }} />
