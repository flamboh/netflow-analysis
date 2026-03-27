<script lang="ts">
	import SingularitiesList from '$lib/components/charts/SingularitiesList.svelte';
	import SpectrumChart from '$lib/components/charts/SpectrumChart.svelte';
	import StructureFunctionChart from '$lib/components/charts/StructureFunctionChart.svelte';
	import type {
		FileIpCounts,
		NetflowFileSummaryRecord,
		SingularitiesData,
		SpectrumData,
		StructureFunctionData
	} from '$lib/types/types';

	type ReloadFn = (router: string, source: boolean) => void;
	type RequestedFn = (router: string, source: boolean) => boolean;

	let {
		record,
		structureFunctionDataSource,
		structureFunctionDataDestination,
		spectrumDataSource,
		spectrumDataDestination,
		singularitiesDataSource,
		singularitiesDataDestination,
		loadingStructureSource,
		loadingStructureDestination,
		loadingSpectrumSource,
		loadingSpectrumDestination,
		loadingSingularitiesSource,
		loadingSingularitiesDestination,
		errorsSource,
		errorsDestination,
		errorsSpectrumSource,
		errorsSpectrumDestination,
		errorsSingularitiesSource,
		errorsSingularitiesDestination,
		IPCountsSource,
		IPCountsDestination,
		reloadStructure,
		reloadSpectrum,
		reloadSingularities,
		hasRequestedSingularities,
		formatCount,
		formatTimestampAsPST
	}: {
		record: NetflowFileSummaryRecord;
		structureFunctionDataSource: Map<string, StructureFunctionData>;
		structureFunctionDataDestination: Map<string, StructureFunctionData>;
		spectrumDataSource: Map<string, SpectrumData>;
		spectrumDataDestination: Map<string, SpectrumData>;
		singularitiesDataSource: Map<string, SingularitiesData>;
		singularitiesDataDestination: Map<string, SingularitiesData>;
		loadingStructureSource: Map<string, boolean>;
		loadingStructureDestination: Map<string, boolean>;
		loadingSpectrumSource: Map<string, boolean>;
		loadingSpectrumDestination: Map<string, boolean>;
		loadingSingularitiesSource: Map<string, boolean>;
		loadingSingularitiesDestination: Map<string, boolean>;
		errorsSource: Map<string, string>;
		errorsDestination: Map<string, string>;
		errorsSpectrumSource: Map<string, string>;
		errorsSpectrumDestination: Map<string, string>;
		errorsSingularitiesSource: Map<string, string>;
		errorsSingularitiesDestination: Map<string, string>;
		IPCountsSource: Map<string, FileIpCounts>;
		IPCountsDestination: Map<string, FileIpCounts>;
		reloadStructure: ReloadFn;
		reloadSpectrum: ReloadFn;
		reloadSingularities: ReloadFn;
		hasRequestedSingularities: RequestedFn;
		formatCount: (value: number | null | undefined) => string;
		formatTimestampAsPST: (timestamp: number) => string;
	} = $props();
</script>

<div class="dark:border-dark-border dark:bg-dark-surface rounded-lg border bg-white shadow-sm">
	<div class="bg-cisco-blue dark:bg-dark-subtle rounded-t-lg p-4">
		<h3 class="mb-2 text-lg font-semibold">Router: {record.router}</h3>
		<h3 class="text-md mb-2 font-semibold">
			Absolute Path: <br />
			{record.file_path}
		</h3>
		<div class="grid grid-cols-1 gap-2 text-sm md:grid-cols-2">
			<div>
				<h3 class="text-md font-semibold">Unique IP Count (Source)</h3>
				<div>
					IPv4: {IPCountsSource.get(record.router)?.ipv4Count == null
						? '...'
						: formatCount(IPCountsSource.get(record.router)?.ipv4Count)}
				</div>
				<div>
					IPv6: {IPCountsSource.get(record.router)?.ipv6Count == null
						? '...'
						: formatCount(IPCountsSource.get(record.router)?.ipv6Count)}
				</div>
			</div>
			<div>
				<h3 class="text-md font-semibold">Unique IP Count (Destination)</h3>
				<div>
					IPv4: {IPCountsDestination.get(record.router)?.ipv4Count == null
						? '...'
						: formatCount(IPCountsDestination.get(record.router)?.ipv4Count)}
				</div>
				<div>
					IPv6: {IPCountsDestination.get(record.router)?.ipv6Count == null
						? '...'
						: formatCount(IPCountsDestination.get(record.router)?.ipv6Count)}
				</div>
			</div>
		</div>
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
				<p>First: {formatTimestampAsPST(record.first_timestamp * 1000)}</p>
				<p>Last: {formatTimestampAsPST(record.last_timestamp * 1000)}</p>
				<p>First ms: {record.msec_first}</p>
				<p>Last ms: {record.msec_last}</p>
				<p>Seq failures: {record.sequence_failures.toLocaleString()}</p>
			</div>
		</div>
	</div>

	<div class="rounded-b-lg p-4">
		{#if record.file_exists_on_disk === false}
			<div
				class="mb-4 rounded-lg border border-amber-300 bg-amber-50 p-4 text-amber-900 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-300"
			>
				The original NetFlow file is missing on disk for this router. DB-backed stats can still be
				shown, but on-demand MAAD analysis cannot run.
			</div>
		{/if}
		<h4 class="text-md mb-4 font-semibold text-gray-800 dark:text-gray-100">MAAD Analysis</h4>
		<div class="mb-6 grid grid-cols-1 gap-6 lg:grid-cols-2">
			<h5
				class="dark:border-dark-border hidden border-b pb-2 text-base font-semibold text-blue-700 lg:block dark:text-blue-400"
			>
				Source
			</h5>
			<h5
				class="dark:border-dark-border hidden border-b pb-2 text-base font-semibold text-blue-700 lg:block dark:text-blue-400"
			>
				Destination
			</h5>
		</div>
		<div class="space-y-6">
			<div class="space-y-3">
				<h6 class="text-md font-medium text-gray-700 dark:text-gray-300">Structure</h6>
				<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
					<div>
						{#if loadingStructureSource.get(record.router)}
							<div class="flex items-center justify-center py-6">
								<div class="text-gray-600 dark:text-gray-400">Loading source structure...</div>
							</div>
						{:else if errorsSource.get(record.router)}
							<div
								class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
							>
								<p>Error loading source structure: {errorsSource.get(record.router)}</p>
								<button
									class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
									onclick={() => reloadStructure(record.router, true)}
								>
									Retry
								</button>
							</div>
						{:else if structureFunctionDataSource.get(record.router)}
							<StructureFunctionChart data={structureFunctionDataSource.get(record.router)!} />
						{:else}
							<div class="text-sm text-gray-500 dark:text-gray-400">No source structure data.</div>
						{/if}
					</div>
					<div>
						{#if loadingStructureDestination.get(record.router)}
							<div class="flex items-center justify-center py-6">
								<div class="text-gray-600 dark:text-gray-400">Loading destination structure...</div>
							</div>
						{:else if errorsDestination.get(record.router)}
							<div
								class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
							>
								<p>Error loading destination structure: {errorsDestination.get(record.router)}</p>
								<button
									class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
									onclick={() => reloadStructure(record.router, false)}
								>
									Retry
								</button>
							</div>
						{:else if structureFunctionDataDestination.get(record.router)}
							<StructureFunctionChart data={structureFunctionDataDestination.get(record.router)!} />
						{:else}
							<div class="text-sm text-gray-500 dark:text-gray-400">
								No destination structure data.
							</div>
						{/if}
					</div>
				</div>
			</div>

			<div class="space-y-3">
				<h6 class="text-md font-medium text-gray-700 dark:text-gray-300">Spectrum</h6>
				<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
					<div>
						{#if loadingSpectrumSource.get(record.router)}
							<div class="flex items-center justify-center py-6">
								<div class="text-gray-600 dark:text-gray-400">Loading source spectrum...</div>
							</div>
						{:else if errorsSpectrumSource.get(record.router)}
							<div
								class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
							>
								<p>Error loading source spectrum: {errorsSpectrumSource.get(record.router)}</p>
								<button
									class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
									onclick={() => reloadSpectrum(record.router, true)}
								>
									Retry
								</button>
							</div>
						{:else if spectrumDataSource.get(record.router)}
							<SpectrumChart data={spectrumDataSource.get(record.router)!} />
						{:else}
							<div class="text-sm text-gray-500 dark:text-gray-400">No source spectrum data.</div>
						{/if}
					</div>
					<div>
						{#if loadingSpectrumDestination.get(record.router)}
							<div class="flex items-center justify-center py-6">
								<div class="text-gray-600 dark:text-gray-400">Loading destination spectrum...</div>
							</div>
						{:else if errorsSpectrumDestination.get(record.router)}
							<div
								class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
							>
								<p>
									Error loading destination spectrum: {errorsSpectrumDestination.get(record.router)}
								</p>
								<button
									class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
									onclick={() => reloadSpectrum(record.router, false)}
								>
									Retry
								</button>
							</div>
						{:else if spectrumDataDestination.get(record.router)}
							<SpectrumChart data={spectrumDataDestination.get(record.router)!} />
						{:else}
							<div class="text-sm text-gray-500 dark:text-gray-400">
								No destination spectrum data.
							</div>
						{/if}
					</div>
				</div>
			</div>

			<div class="space-y-3">
				<h6 class="text-md font-medium text-gray-700 dark:text-gray-300">Singularities</h6>
				<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
					<div>
						{#if !hasRequestedSingularities(record.router, true)}
							<button
								class="rounded bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700"
								onclick={() => reloadSingularities(record.router, true)}
							>
								Load source singularities
							</button>
						{:else if loadingSingularitiesSource.get(record.router)}
							<div class="flex items-center justify-center py-6">
								<div class="text-gray-600 dark:text-gray-400">Loading source singularities...</div>
							</div>
						{:else if errorsSingularitiesSource.get(record.router)}
							<div
								class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
							>
								<p>
									Error loading source singularities:
									{errorsSingularitiesSource.get(record.router)}
								</p>
								<button
									class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
									onclick={() => reloadSingularities(record.router, true)}
								>
									Retry
								</button>
							</div>
						{:else if singularitiesDataSource.get(record.router)}
							<SingularitiesList data={singularitiesDataSource.get(record.router)!} />
						{:else}
							<div class="text-sm text-gray-500 dark:text-gray-400">
								No source singularities data.
							</div>
						{/if}
					</div>
					<div>
						{#if !hasRequestedSingularities(record.router, false)}
							<button
								class="rounded bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700"
								onclick={() => reloadSingularities(record.router, false)}
							>
								Load destination singularities
							</button>
						{:else if loadingSingularitiesDestination.get(record.router)}
							<div class="flex items-center justify-center py-6">
								<div class="text-gray-600 dark:text-gray-400">
									Loading destination singularities...
								</div>
							</div>
						{:else if errorsSingularitiesDestination.get(record.router)}
							<div
								class="rounded border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-400"
							>
								<p>
									Error loading destination singularities:
									{errorsSingularitiesDestination.get(record.router)}
								</p>
								<button
									class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
									onclick={() => reloadSingularities(record.router, false)}
								>
									Retry
								</button>
							</div>
						{:else if singularitiesDataDestination.get(record.router)}
							<SingularitiesList data={singularitiesDataDestination.get(record.router)!} />
						{:else}
							<div class="text-sm text-gray-500 dark:text-gray-400">
								No destination singularities data.
							</div>
						{/if}
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
