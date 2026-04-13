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
</script>

<div class="bg-cisco-blue dark:bg-dark-subtle rounded-t-lg p-4">
	<h3 class="mb-2 text-lg font-semibold">Router: {row.router}</h3>
	<h3 class="text-md mb-2 font-semibold">
		Absolute Path: <br />
		{row.summary.file_path}
	</h3>
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
			<p>First: {formatTimestampAsPST(row.summary.first_timestamp * 1000)}</p>
			<p>Last: {formatTimestampAsPST(row.summary.last_timestamp * 1000)}</p>
			<p>First ms: {row.summary.msec_first}</p>
			<p>Last ms: {row.summary.msec_last}</p>
			<p>Seq failures: {row.summary.sequence_failures.toLocaleString()}</p>
		</div>
	</div>
</div>
