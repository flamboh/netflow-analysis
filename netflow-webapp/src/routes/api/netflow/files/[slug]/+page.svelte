<script lang="ts">
	import type { PageProps } from './$types';
	import StructureFunctionChart from '$lib/components/charts/StructureFunctionChart.svelte';
	import SpectrumChart from '$lib/components/charts/SpectrumChart.svelte';
	import SingularitiesList from '$lib/components/charts/SingularitiesList.svelte';
	import { onMount } from 'svelte';

	let { data }: PageProps = $props();
	let structureFunctionDataSource = $state(new Map());
	let structureFunctionDataDestination = $state(new Map());
	let spectrumDataSource = $state(new Map());
	let spectrumDataDestination = $state(new Map());
	let singularitiesDataSource = $state(new Map());
	let singularitiesDataDestination = $state(new Map());
	let loadingSource = $state(new Map());
	let loadingDestination = $state(new Map());
	let loadingSpectrumSource = $state(new Map());
	let loadingSpectrumDestination = $state(new Map());
	let loadingSingularitiesSource = $state(new Map());
	let loadingSingularitiesDestination = $state(new Map());
	let errorsSource = $state(new Map());
	let errorsDestination = $state(new Map());
	let errorsSpectrumSource = $state(new Map());
	let errorsSpectrumDestination = $state(new Map());
	let errorsSingularitiesSource = $state(new Map());
	let errorsSingularitiesDestination = $state(new Map());
	type IPCounts = {
		uniqueIPCount: number;
		totalIPCount: number;
	};
	let loadingIPCountsSource = $state(new Map<string, boolean>());
	let loadingIPCountsDestination = $state(new Map<string, boolean>());
	let IPCountsSource = $state(new Map<string, IPCounts>());
	let IPCountsDestination = $state(new Map<string, IPCounts>());

	onMount(async () => {
		// Load structure function and spectrum data for each router (both source and destination)
		const tasks = data.summary.flatMap((record) => [
			// Load source and destination data in parallel for both analyses
			loadStructureFunctionData(record.router, record.file_path, true), // source
			loadStructureFunctionData(record.router, record.file_path, false), // destination
			loadSpectrumData(record.router, record.file_path, true), // source spectrum
			loadSpectrumData(record.router, record.file_path, false), // destination spectrum
			loadSingularitiesData(record.router, record.file_path, true), // source singularities
			loadSingularitiesData(record.router, record.file_path, false), // destination singularities
			loadIPCounts(record.router, true), // source IP count
			loadIPCounts(record.router, false) // destination IP count
		]);
		await Promise.all(tasks);
	});

	async function loadIPCounts(router: string, source: boolean) {
		try {
			const response = await fetch(
				`/api/netflow/files/${data.slug}/unique-ip?router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load unique IP count: ${response.statusText}`);
			}
			const result = await response.json();
			console.log(
				`Unique IP count loaded for ${router} (${source ? 'source' : 'destination'}):`,
				result
			);
			if (source) {
				IPCountsSource.set(router, result);
				IPCountsSource = new Map(IPCountsSource);
			} else {
				IPCountsDestination.set(router, result);
				IPCountsDestination = new Map(IPCountsDestination);
			}
		} catch (e) {
			console.error(
				`Failed to load unique IP count for ${router} (${source ? 'source' : 'destination'}):`,
				e
			);
		} finally {
			if (source) {
				loadingIPCountsSource.set(router, false);
				loadingIPCountsSource = new Map(loadingIPCountsSource);
			} else {
				loadingIPCountsDestination.set(router, false);
				loadingIPCountsDestination = new Map(loadingIPCountsDestination);
			}
		}
	}

	async function loadStructureFunctionData(router: string, _file_path: string, source: boolean) {
		// Set loading state
		if (source) {
			loadingSource.set(router, true);
			loadingSource = new Map(loadingSource);
			errorsSource.set(router, '');
			errorsSource = new Map(errorsSource);
		} else {
			loadingDestination.set(router, true);
			loadingDestination = new Map(loadingDestination);
			errorsDestination.set(router, '');
			errorsDestination = new Map(errorsDestination);
		}

		try {
			const response = await fetch(
				`/api/netflow/files/${data.slug}/structure-function?router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load structure function data: ${response.statusText}`);
			}
			const result = await response.json();
			console.log(
				`Structure function data loaded for ${router} (${source ? 'source' : 'destination'}):`,
				result
			);

			// Store the result in the appropriate data map
			if (source) {
				structureFunctionDataSource.set(router, result);
				structureFunctionDataSource = new Map(structureFunctionDataSource);
			} else {
				structureFunctionDataDestination.set(router, result);
				structureFunctionDataDestination = new Map(structureFunctionDataDestination);
			}
		} catch (e) {
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			console.error(
				`Failed to load structure function data for ${router} (${source ? 'source' : 'destination'}):`,
				e
			);

			// Set error state
			if (source) {
				errorsSource.set(router, errorMessage);
				errorsSource = new Map(errorsSource);
			} else {
				errorsDestination.set(router, errorMessage);
				errorsDestination = new Map(errorsDestination);
			}
		} finally {
			// Clear loading state
			if (source) {
				loadingSource.set(router, false);
				loadingSource = new Map(loadingSource);
			} else {
				loadingDestination.set(router, false);
				loadingDestination = new Map(loadingDestination);
			}
		}
	}

	async function loadSpectrumData(router: string, file_path: string, source: boolean) {
		// Set loading state
		if (source) {
			loadingSpectrumSource.set(router, true);
			loadingSpectrumSource = new Map(loadingSpectrumSource);
			errorsSpectrumSource.set(router, '');
			errorsSpectrumSource = new Map(errorsSpectrumSource);
		} else {
			loadingSpectrumDestination.set(router, true);
			loadingSpectrumDestination = new Map(loadingSpectrumDestination);
			errorsSpectrumDestination.set(router, '');
			errorsSpectrumDestination = new Map(errorsSpectrumDestination);
		}

		try {
			const response = await fetch(
				`/api/netflow/files/${data.slug}/spectrum?router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load spectrum data: ${response.statusText}`);
			}
			const result = await response.json();
			console.log(
				`Spectrum data loaded for ${router} (${source ? 'source' : 'destination'}):`,
				result
			);

			// Store the result in the appropriate data map
			if (source) {
				spectrumDataSource.set(router, result);
				spectrumDataSource = new Map(spectrumDataSource);
			} else {
				spectrumDataDestination.set(router, result);
				spectrumDataDestination = new Map(spectrumDataDestination);
			}
		} catch (e) {
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			console.error(
				`Failed to load spectrum data for ${router} (${source ? 'source' : 'destination'}):`,
				e
			);

			// Set error state
			if (source) {
				errorsSpectrumSource.set(router, errorMessage);
				errorsSpectrumSource = new Map(errorsSpectrumSource);
			} else {
				errorsSpectrumDestination.set(router, errorMessage);
				errorsSpectrumDestination = new Map(errorsSpectrumDestination);
			}
		} finally {
			// Clear loading state
			if (source) {
				loadingSpectrumSource.set(router, false);
				loadingSpectrumSource = new Map(loadingSpectrumSource);
			} else {
				loadingSpectrumDestination.set(router, false);
				loadingSpectrumDestination = new Map(loadingSpectrumDestination);
			}
		}
	}

	async function loadSingularitiesData(router: string, _file_path: string, source: boolean) {
		// Set loading state
		if (source) {
			loadingSingularitiesSource.set(router, true);
			loadingSingularitiesSource = new Map(loadingSingularitiesSource);
			errorsSingularitiesSource.set(router, '');
			errorsSingularitiesSource = new Map(errorsSingularitiesSource);
		} else {
			loadingSingularitiesDestination.set(router, true);
			loadingSingularitiesDestination = new Map(loadingSingularitiesDestination);
			errorsSingularitiesDestination.set(router, '');
			errorsSingularitiesDestination = new Map(errorsSingularitiesDestination);
		}

		try {
			const response = await fetch(
				`/api/netflow/files/${data.slug}/singularities?router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load singularities data: ${response.statusText}`);
			}
			const result = await response.json();
			console.log(
				`Singularities data loaded for ${router} (${source ? 'source' : 'destination'}):`,
				result
			);

			// Store the result in the appropriate data map
			if (source) {
				singularitiesDataSource.set(router, result);
				singularitiesDataSource = new Map(singularitiesDataSource);
			} else {
				singularitiesDataDestination.set(router, result);
				singularitiesDataDestination = new Map(singularitiesDataDestination);
			}
		} catch (e) {
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			console.error(
				`Failed to load singularities data for ${router} (${source ? 'source' : 'destination'}):`,
				e
			);

			// Set error state
			if (source) {
				errorsSingularitiesSource.set(router, errorMessage);
				errorsSingularitiesSource = new Map(errorsSingularitiesSource);
			} else {
				errorsSingularitiesDestination.set(router, errorMessage);
				errorsSingularitiesDestination = new Map(errorsSingularitiesDestination);
			}
		} finally {
			// Clear loading state
			if (source) {
				loadingSingularitiesSource.set(router, false);
				loadingSingularitiesSource = new Map(loadingSingularitiesSource);
			} else {
				loadingSingularitiesDestination.set(router, false);
				loadingSingularitiesDestination = new Map(loadingSingularitiesDestination);
			}
		}
	}
</script>

<div class="container mx-auto p-6">
	<h1 class="mb-4 text-2xl text-black">
		NetFlow File: {data.fileInfo.filename}
	</h1>

	<div class="mb-6 rounded-lg border bg-blue-100 p-4">
		<h2 class="mb-2 text-lg font-semibold">File Information</h2>
		<div class="grid grid-cols-3 gap-4">
			<div>Date: {data.fileInfo.year}-{data.fileInfo.month}-{data.fileInfo.day}</div>
			<div>Time: {data.fileInfo.hour}:{data.fileInfo.minute}</div>
			<div>
				{#if data.summary?.length}
					Processed in DB: {new Date(data.summary[0].processed_at).toLocaleString()}
				{:else}
					Processed in DB: N/A
				{/if}
			</div>
		</div>
	</div>

	<div class="space-y-6">
		{#each data.summary as record (record.router)}
			<div class="rounded-lg border bg-white shadow-sm">
				<!-- Router Data Summary -->
				<div class="bg-cisco-blue rounded-t-lg p-4">
					<h3 class="mb-2 text-lg font-semibold">Router: {record.router}</h3>
					<h3 class="text-md mb-2 font-semibold">
						Absolute Path: <br />
						{record.file_path}
					</h3>
					<h3 class="text-md mb-2 font-semibold">
						Unique/Total IP Count (Source):
						{#if IPCountsSource.get(record.router)}
							{IPCountsSource.get(record.router)?.uniqueIPCount ?? 'N/A'}/{IPCountsSource.get(
								record.router
							)?.totalIPCount ?? 'N/A'}
							or {(IPCountsSource.get(record.router)?.totalIPCount ?? 0) > 0
								? Math.round(
										((IPCountsSource.get(record.router)?.uniqueIPCount ?? 0) /
											(IPCountsSource.get(record.router)?.totalIPCount ?? 1)) *
											100
									)
								: 0}%
						{:else}
							Loading...
						{/if}
					</h3>
					<h3 class="text-md mb-2 font-semibold">
						Unique/Total IP Count (Destination):
						{#if IPCountsDestination.get(record.router)}
							{IPCountsDestination.get(record.router)?.uniqueIPCount ??
								'N/A'}/{IPCountsDestination.get(record.router)?.totalIPCount ?? 'N/A'}
							or {(IPCountsDestination.get(record.router)?.totalIPCount ?? 0) > 0
								? Math.round(
										((IPCountsDestination.get(record.router)?.uniqueIPCount ?? 0) /
											(IPCountsDestination.get(record.router)?.totalIPCount ?? 1)) *
											100
									)
								: 0}%
						{:else}
							Loading...
						{/if}
					</h3>
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

				<!-- MAAD Analysis for this Router -->
				<div class="rounded-b-lg p-4">
					<h4 class="text-md mb-4 font-semibold text-gray-800">MAAD Analysis</h4>

					<!-- Structure Function Analysis -->
					<div class="mb-6">
						<h5 class="text-md mb-3 font-medium text-gray-700">Structure Function tau(q)</h5>
						<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
							<!-- Source Address Analysis -->
							<div class="space-y-3">
								<h6 class="text-sm font-medium text-blue-700">Source Address Analysis</h6>
								{#if loadingSource.get(record.router)}
									<div class="flex items-center justify-center py-6">
										<div class="text-gray-600">Loading source address analysis...</div>
									</div>
								{:else if errorsSource.get(record.router)}
									<div class="rounded border border-red-200 bg-red-50 p-4 text-red-700">
										<p>Error loading source analysis: {errorsSource.get(record.router)}</p>
										<button
											class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
											onclick={() =>
												loadStructureFunctionData(record.router, record.file_path, true)}
										>
											Retry Source
										</button>
									</div>
								{:else if structureFunctionDataSource.get(record.router)}
									<StructureFunctionChart data={structureFunctionDataSource.get(record.router)} />
								{/if}
							</div>

							<!-- Destination Address Analysis -->
							<div class="space-y-3">
								<h6 class="text-sm font-medium text-blue-700">Destination Address Analysis</h6>
								{#if loadingDestination.get(record.router)}
									<div class="flex items-center justify-center py-6">
										<div class="text-gray-600">Loading destination address analysis...</div>
									</div>
								{:else if errorsDestination.get(record.router)}
									<div class="rounded border border-red-200 bg-red-50 p-4 text-red-700">
										<p>
											Error loading destination analysis: {errorsDestination.get(record.router)}
										</p>
										<button
											class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
											onclick={() =>
												loadStructureFunctionData(record.router, record.file_path, false)}
										>
											Retry Destination
										</button>
									</div>
								{:else if structureFunctionDataDestination.get(record.router)}
									<StructureFunctionChart
										data={structureFunctionDataDestination.get(record.router)}
									/>
								{/if}
							</div>
						</div>
					</div>

					<!-- Multifractal Spectrum Analysis -->
					<div class="mb-4">
						<h5 class="text-md mb-3 font-medium text-gray-700">Spectrum f(alpha)</h5>
						<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
							<!-- Source Address Spectrum -->
							<div class="space-y-3">
								<h6 class="text-sm font-medium text-blue-700">Source Address Spectrum</h6>
								{#if loadingSpectrumSource.get(record.router)}
									<div class="flex items-center justify-center py-6">
										<div class="text-gray-600">Loading source spectrum analysis...</div>
									</div>
								{:else if errorsSpectrumSource.get(record.router)}
									<div class="rounded border border-red-200 bg-red-50 p-4 text-red-700">
										<p>Error loading source spectrum: {errorsSpectrumSource.get(record.router)}</p>
										<button
											class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
											onclick={() => loadSpectrumData(record.router, record.file_path, true)}
										>
											Retry Source
										</button>
									</div>
								{:else if spectrumDataSource.get(record.router)}
									<SpectrumChart data={spectrumDataSource.get(record.router)} />
								{/if}
							</div>

							<!-- Destination Address Spectrum -->
							<div class="space-y-3">
								<h6 class="text-sm font-medium text-blue-700">Destination Address Spectrum</h6>
								{#if loadingSpectrumDestination.get(record.router)}
									<div class="flex items-center justify-center py-6">
										<div class="text-gray-600">Loading destination spectrum analysis...</div>
									</div>
								{:else if errorsSpectrumDestination.get(record.router)}
									<div class="rounded border border-red-200 bg-red-50 p-4 text-red-700">
										<p>
											Error loading destination spectrum: {errorsSpectrumDestination.get(
												record.router
											)}
										</p>
										<button
											class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
											onclick={() => loadSpectrumData(record.router, record.file_path, false)}
										>
											Retry Destination
										</button>
									</div>
								{:else if spectrumDataDestination.get(record.router)}
									<SpectrumChart data={spectrumDataDestination.get(record.router)} />
								{/if}
							</div>
						</div>
					</div>
					<!-- Singularities Analysis -->
					<div class="space-y-4">
						<h5 class="text-lg font-semibold">Singularities Analysis</h5>
						<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
							<!-- Source Address Singularities -->
							<div class="space-y-3">
								<h6 class="text-sm font-medium text-blue-700">Source Address Singularities</h6>
								{#if loadingSingularitiesSource.get(record.router)}
									<div class="flex items-center justify-center py-6">
										<div class="text-gray-600">Loading source singularities analysis...</div>
									</div>
								{:else if errorsSingularitiesSource.get(record.router)}
									<div class="rounded-lg border border-red-300 bg-red-50 p-4">
										<p class="text-red-700">
											Error loading source singularities: {errorsSingularitiesSource.get(
												record.router
											)}
										</p>
										<button
											class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
											onclick={() => loadSingularitiesData(record.router, record.file_path, true)}
										>
											Retry Source
										</button>
									</div>
								{:else if singularitiesDataSource.get(record.router)}
									<SingularitiesList data={singularitiesDataSource.get(record.router)} />
								{/if}
							</div>
							<!-- Destination Address Singularities -->
							<div class="space-y-3">
								<h6 class="text-sm font-medium text-blue-700">Destination Address Singularities</h6>
								{#if loadingSingularitiesDestination.get(record.router)}
									<div class="flex items-center justify-center py-6">
										<div class="text-gray-600">Loading destination singularities analysis...</div>
									</div>
								{:else if errorsSingularitiesDestination.get(record.router)}
									<div class="rounded-lg border border-red-300 bg-red-50 p-4">
										<p class="text-red-700">
											Error loading destination singularities: {errorsSingularitiesDestination.get(
												record.router
											)}
										</p>
										<button
											class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
											onclick={() => loadSingularitiesData(record.router, record.file_path, false)}
										>
											Retry Destination
										</button>
									</div>
								{:else if singularitiesDataDestination.get(record.router)}
									<SingularitiesList data={singularitiesDataDestination.get(record.router)} />
								{/if}
							</div>
						</div>
					</div>
				</div>
			</div>
		{/each}
	</div>
</div>
