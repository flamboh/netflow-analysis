<script lang="ts">
	import type { PageProps } from './$types';
	import StructureFunctionChart from '$lib/components/charts/StructureFunctionChart.svelte';
	import SpectrumChart from '$lib/components/charts/SpectrumChart.svelte';
	import SingularitiesList from '$lib/components/charts/SingularitiesList.svelte';
	import { onMount } from 'svelte';
	import { afterNavigate } from '$app/navigation';

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
		ipv4Count: number | null;
		ipv6Count: number | null;
	};
	let IPCountsSource = $state(new Map<string, IPCounts>());
	let IPCountsDestination = $state(new Map<string, IPCounts>());
	const formatCount = (value: number | null | undefined) => {
		return typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString() : 'N/A';
	};

	function getNextSlug(slug: string) {
		if (!slug || slug.length !== 12 || !/^\d{12}$/.test(slug)) {
			return slug;
		}
		const year = parseInt(slug.slice(0, 4), 10);
		const month = parseInt(slug.slice(4, 6), 10) - 1;
		const day = parseInt(slug.slice(6, 8), 10);
		const hour = parseInt(slug.slice(8, 10), 10);
		const minute = parseInt(slug.slice(10, 12), 10);
		const nextDate = new Date(year, month, day, hour, minute + 5);

		return `${nextDate.getFullYear()}${String(nextDate.getMonth() + 1).padStart(2, '0')}${String(nextDate.getDate()).padStart(2, '0')}${String(nextDate.getHours()).padStart(2, '0')}${String(nextDate.getMinutes()).padStart(2, '0')}`;
	}

	const nextSlug = $derived(getNextSlug(data.slug));
	let currentSlug = $state(data.slug);
	let loadToken = 0;

	function resetDataStores() {
		structureFunctionDataSource = new Map();
		structureFunctionDataDestination = new Map();
		spectrumDataSource = new Map();
		spectrumDataDestination = new Map();
		singularitiesDataSource = new Map();
		singularitiesDataDestination = new Map();
		loadingSource = new Map();
		loadingDestination = new Map();
		loadingSpectrumSource = new Map();
		loadingSpectrumDestination = new Map();
		loadingSingularitiesSource = new Map();
		loadingSingularitiesDestination = new Map();
		errorsSource = new Map();
		errorsDestination = new Map();
		errorsSpectrumSource = new Map();
		errorsSpectrumDestination = new Map();
		errorsSingularitiesSource = new Map();
		errorsSingularitiesDestination = new Map();
		IPCountsSource = new Map();
		IPCountsDestination = new Map();
	}

	async function loadAllData(slug: string) {
		if (!data.summary || data.summary.length === 0) {
			resetDataStores();
			return;
		}
		const token = ++loadToken;
		resetDataStores();
		const tasks = data.summary.flatMap((record) => [
			loadStructureFunctionData(token, slug, record.router, true),
			loadStructureFunctionData(token, slug, record.router, false),
			loadSpectrumData(token, slug, record.router, true),
			loadSpectrumData(token, slug, record.router, false),
			loadSingularitiesData(token, slug, record.router, true),
			loadSingularitiesData(token, slug, record.router, false),
			loadIPCounts(token, slug, record.router, true),
			loadIPCounts(token, slug, record.router, false)
		]);
		await Promise.all(tasks);
	}

	onMount(() => {
		currentSlug = data.slug;
		void loadAllData(data.slug);
	});

	afterNavigate(() => {
		if (data.slug !== currentSlug) {
			currentSlug = data.slug;
			void loadAllData(data.slug);
		}
	});

	async function loadIPCounts(token: number, slug: string, router: string, source: boolean) {
		try {
			const response = await fetch(
				`/api/netflow/files/${slug}/ip-counts?router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load IP counts: ${response.statusText}`);
			}
			const result = await response.json();
			if (token !== loadToken) {
				return;
			}
			console.log(`IP counts loaded for ${router} (${source ? 'source' : 'destination'}):`, result);
			if (source) {
				IPCountsSource.set(router, result);
				IPCountsSource = new Map(IPCountsSource);
			} else {
				IPCountsDestination.set(router, result);
				IPCountsDestination = new Map(IPCountsDestination);
			}
		} catch (e) {
			console.error(
				`Failed to load IP counts for ${router} (${source ? 'source' : 'destination'}):`,
				e
			);
		}
	}

	async function loadStructureFunctionData(
		token: number,
		slug: string,
		router: string,
		source: boolean
	) {
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
				`/api/netflow/files/${slug}/structure-function?router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load structure function data: ${response.statusText}`);
			}
			const result = await response.json();
			if (token !== loadToken) {
				return;
			}
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

	async function loadSpectrumData(token: number, slug: string, router: string, source: boolean) {
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
				`/api/netflow/files/${slug}/spectrum?router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load spectrum data: ${response.statusText}`);
			}
			const result = await response.json();
			if (token !== loadToken) {
				return;
			}
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

	async function loadSingularitiesData(
		token: number,
		slug: string,
		router: string,
		source: boolean
	) {
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
				`/api/netflow/files/${slug}/singularities?router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load singularities data: ${response.statusText}`);
			}
			const result = await response.json();
			if (token !== loadToken) {
				return;
			}
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

	function reloadStructure(router: string, source: boolean) {
		void loadStructureFunctionData(loadToken, data.slug, router, source);
	}

	function reloadSpectrum(router: string, source: boolean) {
		void loadSpectrumData(loadToken, data.slug, router, source);
	}

	function reloadSingularities(router: string, source: boolean) {
		void loadSingularitiesData(loadToken, data.slug, router, source);
	}
</script>

<div class="mx-auto max-w-[90vw] px-2 py-2 sm:px-2 lg:px-4">
	<h1 class="mb-2 flex items-center justify-between text-2xl text-black">
		NetFlow File: {data.fileInfo.filename}
		<a
			class="w-24 rounded bg-blue-600 px-4 py-1 text-sm text-white hover:bg-blue-700"
			href={`/api/netflow/files/${nextSlug}`}
		>
			Next File
		</a>
	</h1>

	<div class="mb-2 rounded-lg border bg-blue-100 p-4">
		<h2 class="mb-2 text-lg font-semibold">File Information</h2>
		<div class="grid grid-cols-3 gap-2">
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

	<div class="space-y-2">
		{#each data.summary as record (record.router)}
			<div class="rounded-lg border bg-white shadow-sm">
				<!-- Router Data Summary -->
				<div class="bg-cisco-blue rounded-t-lg p-4">
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
											onclick={() => reloadStructure(record.router, true)}
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
											onclick={() => reloadStructure(record.router, false)}
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
											onclick={() => reloadSpectrum(record.router, true)}
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
											onclick={() => reloadSpectrum(record.router, false)}
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
											onclick={() => reloadSingularities(record.router, true)}
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
											onclick={() => reloadSingularities(record.router, false)}
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
