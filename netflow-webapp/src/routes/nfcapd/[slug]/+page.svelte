<script lang="ts">
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
</script>

<div class="container mx-auto p-6">
	<h1 class="text-2xl font-bold mb-4">
		NetFlow File: {data.fileInfo.filename}
	</h1>
	
	<div class="mb-6 p-4 bg-gray-100 rounded-lg">
		<h2 class="text-lg font-semibold mb-2">File Information</h2>
		<div class="grid grid-cols-3 gap-4">
			<div>Date: {data.fileInfo.year}-{data.fileInfo.month}-{data.fileInfo.day}</div>
			<div>Time: {data.fileInfo.hour}:{data.fileInfo.minute}</div>
			<div>Processed: {new Date(data.summary[0].processed_at).toLocaleString()}</div>
		</div>
	</div>

	<div class="space-y-4">
		{#each data.summary as record}
			<div class="border rounded-lg p-4">
				<h3 class="text-lg font-semibold mb-2">Router: {record.router}</h3>
				<div class="grid grid-cols-4 gap-4 text-sm">
					<div>
						<h4 class="font-medium">Flows</h4>
						<p>Total: {record.flows.toLocaleString()}</p>
						<p>TCP: {record.flows_tcp.toLocaleString()}</p>
						<p>UDP: {record.flows_udp.toLocaleString()}</p>
						<p>ICMP: {record.flows_icmp.toLocaleString()}</p>
						<p>Other: {record.flows_other.toLocaleString()}</p>
					</div>
					<div>
						<h4 class="font-medium">Packets</h4>
						<p>Total: {record.packets.toLocaleString()}</p>
						<p>TCP: {record.packets_tcp.toLocaleString()}</p>
						<p>UDP: {record.packets_udp.toLocaleString()}</p>
						<p>ICMP: {record.packets_icmp.toLocaleString()}</p>
						<p>Other: {record.packets_other.toLocaleString()}</p>
					</div>
					<div>
						<h4 class="font-medium">Bytes</h4>
						<p>Total: {record.bytes.toLocaleString()}</p>
						<p>TCP: {record.bytes_tcp.toLocaleString()}</p>
						<p>UDP: {record.bytes_udp.toLocaleString()}</p>
						<p>ICMP: {record.bytes_icmp.toLocaleString()}</p>
						<p>Other: {record.bytes_other.toLocaleString()}</p>
					</div>
					<div>
						<h4 class="font-medium">Timestamps & Metrics</h4>
						<p>First: {new Date(record.first_timestamp * 1000).toLocaleString()}</p>
						<p>Last: {new Date(record.last_timestamp * 1000).toLocaleString()}</p>
						<p>First ms: {record.msec_first}</p>
						<p>Last ms: {record.msec_last}</p>
						<p>Seq failures: {record.sequence_failures.toLocaleString()}</p>
					</div>
				</div>
			</div>
		{/each}
	</div>
</div>
