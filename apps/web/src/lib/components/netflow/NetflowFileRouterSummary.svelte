<script lang="ts">
	import type { FileDetailResourceView, NetflowFileRouterRow } from './file-detail-loader.svelte';
	import type { FileIpCounts } from '$lib/types/types';

	let {
		row,
		formatCount,
		formatTimestampAsPST
	}: {
		row: NetflowFileRouterRow;
		formatCount: (value: number | null | undefined) => string;
		formatTimestampAsPST: (timestamp: number) => string;
	} = $props();

	function formatIpCount(slot: FileDetailResourceView<FileIpCounts>, family: 'ipv4' | 'ipv6') {
		const value = family === 'ipv4' ? slot.data?.ipv4Count : slot.data?.ipv6Count;
		if (value !== null && value !== undefined) {
			return formatCount(value);
		}

		return slot.loading ? '...' : 'N/A';
	}

	function formatOptionalTimestamp(timestamp: number | null | undefined) {
		if (typeof timestamp !== 'number' || !Number.isFinite(timestamp)) {
			return 'N/A';
		}

		return formatTimestampAsPST(timestamp * 1000);
	}
</script>

<div class="bg-cisco-blue dark:bg-dark-subtle rounded-t-lg p-4 text-white dark:text-gray-100">
	<div class="mb-4 grid gap-3 lg:grid-cols-[1fr_auto]">
		<div>
			<h3 class="text-lg font-semibold">Source: {row.router}</h3>
			<p class="mt-1 text-sm break-all text-white/85 dark:text-gray-300">
				{row.summary.file_path ?? 'No input locator recorded'}
			</p>
		</div>
		<div class="grid grid-cols-2 gap-2 text-xs sm:grid-cols-4 lg:text-right">
			<div>
				<p class="font-semibold text-white/70 uppercase dark:text-gray-400">Kind</p>
				<p>{row.summary.input_kind ?? 'unknown'}</p>
			</div>
			<div>
				<p class="font-semibold text-white/70 uppercase dark:text-gray-400">Status</p>
				<p>{row.summary.input_status ?? 'unknown'}</p>
			</div>
			<div>
				<p class="font-semibold text-white/70 uppercase dark:text-gray-400">Bucket</p>
				<p>{formatOptionalTimestamp(row.summary.bucket_start)}</p>
			</div>
			<div>
				<p class="font-semibold text-white/70 uppercase dark:text-gray-400">On Disk</p>
				<p>{row.summary.file_exists_on_disk ? 'yes' : 'no'}</p>
			</div>
		</div>
	</div>
	{#if row.summary.input_error_message}
		<p class="mb-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
			{row.summary.input_error_message}
		</p>
	{/if}
	<div class="grid grid-cols-1 gap-2 text-sm md:grid-cols-2">
		<div>
			<h3 class="text-md font-semibold">Unique IP Count (Source)</h3>
			<div>IPv4: {formatIpCount(row.source.ipCounts, 'ipv4')}</div>
			<div>IPv6: {formatIpCount(row.source.ipCounts, 'ipv6')}</div>
		</div>
		<div>
			<h3 class="text-md font-semibold">Unique IP Count (Destination)</h3>
			<div>IPv4: {formatIpCount(row.destination.ipCounts, 'ipv4')}</div>
			<div>IPv6: {formatIpCount(row.destination.ipCounts, 'ipv6')}</div>
		</div>
	</div>
	<div class="grid grid-cols-4 gap-4 text-sm">
		<div>
			<h4 class="font-medium">Flows</h4>
			<p>Total: {row.summary.flows.toLocaleString()}</p>
			<p>TCP: {row.summary.flows_tcp.toLocaleString()}</p>
			<p>UDP: {row.summary.flows_udp.toLocaleString()}</p>
			<p>ICMP: {row.summary.flows_icmp.toLocaleString()}</p>
			<p>Other: {row.summary.flows_other.toLocaleString()}</p>
		</div>
		<div>
			<h4 class="font-medium">Packets</h4>
			<p>Total: {row.summary.packets.toLocaleString()}</p>
			<p>TCP: {row.summary.packets_tcp.toLocaleString()}</p>
			<p>UDP: {row.summary.packets_udp.toLocaleString()}</p>
			<p>ICMP: {row.summary.packets_icmp.toLocaleString()}</p>
			<p>Other: {row.summary.packets_other.toLocaleString()}</p>
		</div>
		<div>
			<h4 class="font-medium">Bytes</h4>
			<p>Total: {row.summary.bytes.toLocaleString()}</p>
			<p>TCP: {row.summary.bytes_tcp.toLocaleString()}</p>
			<p>UDP: {row.summary.bytes_udp.toLocaleString()}</p>
			<p>ICMP: {row.summary.bytes_icmp.toLocaleString()}</p>
			<p>Other: {row.summary.bytes_other.toLocaleString()}</p>
		</div>
		<div>
			<h4 class="font-medium">Timestamps & Metrics</h4>
			<p>First: {formatOptionalTimestamp(row.summary.first_timestamp)}</p>
			<p>Last: {formatOptionalTimestamp(row.summary.last_timestamp)}</p>
			<p>First ms: {formatCount(row.summary.msec_first)}</p>
			<p>Last ms: {formatCount(row.summary.msec_last)}</p>
			<p>Seq failures: {formatCount(row.summary.sequence_failures)}</p>
		</div>
	</div>
</div>
