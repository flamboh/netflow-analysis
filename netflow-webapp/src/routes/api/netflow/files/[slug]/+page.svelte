<script lang="ts">
	import type { PageProps } from './$types';
	import type {
		FileIpCounts,
		NetflowFileDetailsResponse,
		NetflowFileSummaryRecord,
		SingularitiesData,
		SpectrumData,
		StructureFunctionData
	} from '$lib/types/types';
	import StructureFunctionChart from '$lib/components/charts/StructureFunctionChart.svelte';
	import SpectrumChart from '$lib/components/charts/SpectrumChart.svelte';
	import SingularitiesList from '$lib/components/charts/SingularitiesList.svelte';
	import { onMount } from 'svelte';
	import { afterNavigate } from '$app/navigation';
	import {
		createDateFromPSTComponents,
		epochToPSTComponents,
		formatTimestampAsPST
	} from '$lib/utils/timezone';

	let { data }: PageProps = $props();
	let summaryRecords = $state<NetflowFileSummaryRecord[]>([]);
	let summaryLoading = $state(false);
	let summaryError = $state('');
	let summarySkeletonCount = $state(2);
	let structureFunctionDataSource = $state(new Map<string, StructureFunctionData>());
	let structureFunctionDataDestination = $state(new Map<string, StructureFunctionData>());
	let spectrumDataSource = $state(new Map<string, SpectrumData>());
	let spectrumDataDestination = $state(new Map<string, SpectrumData>());
	let singularitiesDataSource = $state(new Map<string, SingularitiesData>());
	let singularitiesDataDestination = $state(new Map<string, SingularitiesData>());
	let loadingSingularitiesSource = $state(new Map<string, boolean>());
	let loadingSingularitiesDestination = $state(new Map<string, boolean>());
	let errorsSource = $state(new Map<string, string>());
	let errorsDestination = $state(new Map<string, string>());
	let errorsSpectrumSource = $state(new Map<string, string>());
	let errorsSpectrumDestination = $state(new Map<string, string>());
	let errorsSingularitiesSource = $state(new Map<string, string>());
	let errorsSingularitiesDestination = $state(new Map<string, string>());
	let IPCountsSource = $state(new Map<string, FileIpCounts>());
	let IPCountsDestination = $state(new Map<string, FileIpCounts>());
	const formatCount = (value: number | null | undefined) => {
		return typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString() : 'N/A';
	};

	function getNextSlug(slug: string) {
		if (!slug || slug.length !== 12 || !/^\d{12}$/.test(slug)) {
			return slug;
		}
		const year = parseInt(slug.slice(0, 4), 10);
		const month = parseInt(slug.slice(4, 6), 10); // 1-12 for PST utilities
		const day = parseInt(slug.slice(6, 8), 10);
		const hour = parseInt(slug.slice(8, 10), 10);
		const minute = parseInt(slug.slice(10, 12), 10);

		// Create a PST date and add 5 minutes
		const currentDate = createDateFromPSTComponents(year, month, day, hour, minute);
		const nextDate = new Date(currentDate.getTime() + 5 * 60 * 1000);

		// Convert back to PST components
		const nextPST = epochToPSTComponents(Math.floor(nextDate.getTime() / 1000));

		return `${nextPST.year}${String(nextPST.month).padStart(2, '0')}${String(nextPST.day).padStart(2, '0')}${String(nextPST.hours).padStart(2, '0')}${String(nextPST.minutes).padStart(2, '0')}`;
	}

	const nextSlug = $derived(getNextSlug(data.slug));
	const summarySkeletons = $derived(
		Array.from({ length: summarySkeletonCount }, (_, index) => `summary-skeleton-${index}`)
	);
	let currentSlug = $state(data.slug);
	let loadToken = 0;

	function resetDataStores() {
		structureFunctionDataSource = new Map<string, StructureFunctionData>();
		structureFunctionDataDestination = new Map<string, StructureFunctionData>();
		spectrumDataSource = new Map<string, SpectrumData>();
		spectrumDataDestination = new Map<string, SpectrumData>();
		singularitiesDataSource = new Map<string, SingularitiesData>();
		singularitiesDataDestination = new Map<string, SingularitiesData>();
		loadingSingularitiesSource = new Map<string, boolean>();
		loadingSingularitiesDestination = new Map<string, boolean>();
		errorsSource = new Map<string, string>();
		errorsDestination = new Map<string, string>();
		errorsSpectrumSource = new Map<string, string>();
		errorsSpectrumDestination = new Map<string, string>();
		errorsSingularitiesSource = new Map<string, string>();
		errorsSingularitiesDestination = new Map<string, string>();
		IPCountsSource = new Map<string, FileIpCounts>();
		IPCountsDestination = new Map<string, FileIpCounts>();
	}

	function applyDbDetails(result: NetflowFileDetailsResponse) {
		const nextStructureSource = new Map<string, StructureFunctionData>();
		const nextStructureDestination = new Map<string, StructureFunctionData>();
		const nextSpectrumSource = new Map<string, SpectrumData>();
		const nextSpectrumDestination = new Map<string, SpectrumData>();
		const nextIpCountsSource = new Map<string, FileIpCounts>();
		const nextIpCountsDestination = new Map<string, FileIpCounts>();
		const nextErrorsSource = new Map<string, string>();
		const nextErrorsDestination = new Map<string, string>();
		const nextErrorsSpectrumSource = new Map<string, string>();
		const nextErrorsSpectrumDestination = new Map<string, string>();

		for (const routerDetails of result.routers) {
			const router = routerDetails.summary.router;

			if (routerDetails.structureSource) {
				nextStructureSource.set(router, routerDetails.structureSource);
			} else {
				nextErrorsSource.set(router, 'Structure statistics not found');
			}

			if (routerDetails.structureDestination) {
				nextStructureDestination.set(router, routerDetails.structureDestination);
			} else {
				nextErrorsDestination.set(router, 'Structure statistics not found');
			}

			if (routerDetails.spectrumSource) {
				nextSpectrumSource.set(router, routerDetails.spectrumSource);
			} else {
				nextErrorsSpectrumSource.set(router, 'Spectrum statistics not found');
			}

			if (routerDetails.spectrumDestination) {
				nextSpectrumDestination.set(router, routerDetails.spectrumDestination);
			} else {
				nextErrorsSpectrumDestination.set(router, 'Spectrum statistics not found');
			}

			if (routerDetails.ipCountsSource) {
				nextIpCountsSource.set(router, routerDetails.ipCountsSource);
			}

			if (routerDetails.ipCountsDestination) {
				nextIpCountsDestination.set(router, routerDetails.ipCountsDestination);
			}
		}

		structureFunctionDataSource = nextStructureSource;
		structureFunctionDataDestination = nextStructureDestination;
		spectrumDataSource = nextSpectrumSource;
		spectrumDataDestination = nextSpectrumDestination;
		IPCountsSource = nextIpCountsSource;
		IPCountsDestination = nextIpCountsDestination;
		errorsSource = nextErrorsSource;
		errorsDestination = nextErrorsDestination;
		errorsSpectrumSource = nextErrorsSpectrumSource;
		errorsSpectrumDestination = nextErrorsSpectrumDestination;
	}

	async function loadSummary(slug: string) {
		const token = ++loadToken;
		summarySkeletonCount = Math.max(summaryRecords.length, summarySkeletonCount, 2);
		summaryRecords = [];
		summaryLoading = true;
		summaryError = '';
		resetDataStores();

		try {
			const response = await fetch(`/api/netflow/files/${slug}/details`);
			if (!response.ok) {
				let message = `Failed to load file summary: ${response.statusText}`;
				try {
					const payload = await response.json();
					if (payload?.error) {
						message = payload.error;
					}
				} catch {
					// Ignore JSON parse failures and use the status text fallback.
				}
				throw new Error(message);
			}

			const result = (await response.json()) as NetflowFileDetailsResponse;
			if (token !== loadToken) {
				return;
			}

			summaryRecords = result.routers.map((routerDetails) => routerDetails.summary);
			summarySkeletonCount = Math.max(result.routers.length, 2);
			applyDbDetails(result);
			summaryLoading = false;

			const tasks = result.routers.flatMap((routerDetails) => [
				loadSingularitiesData(token, slug, routerDetails.summary.router, true),
				loadSingularitiesData(token, slug, routerDetails.summary.router, false)
			]);
			await Promise.all(tasks);
		} catch (e) {
			if (token !== loadToken) {
				return;
			}

			summaryLoading = false;
			summaryError = e instanceof Error ? e.message : 'Unknown error occurred';
		}
	}

	onMount(() => {
		currentSlug = data.slug;
		void loadSummary(data.slug);
	});

	afterNavigate(() => {
		if (data.slug !== currentSlug) {
			currentSlug = data.slug;
			void loadSummary(data.slug);
		}
	});

	function reloadSummary() {
		void loadSummary(data.slug);
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

	function reloadStructure(_router: string, _source: boolean) {
		void loadSummary(data.slug);
	}

	function reloadSpectrum(_router: string, _source: boolean) {
		void loadSummary(data.slug);
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
				{#if summaryRecords.length}
					Processed in DB: {formatTimestampAsPST(Date.parse(summaryRecords[0].processed_at))}
				{:else if summaryLoading}
					Processed in DB: Loading...
				{:else}
					Processed in DB: N/A
				{/if}
			</div>
		</div>
	</div>

	{#if summaryError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-red-700">
			<p>Failed to load file summary: {summaryError}</p>
			<button
				class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
				onclick={reloadSummary}
			>
				Retry Summary
			</button>
		</div>
	{:else if summaryLoading}
		<div class="space-y-2">
			{#each summarySkeletons as skeletonId (skeletonId)}
				<div class="animate-pulse rounded-lg border bg-white shadow-sm">
					<div class="bg-cisco-blue rounded-t-lg p-4">
						<div class="mb-2 h-7 w-56 rounded bg-white/70"></div>
						<div class="mb-4 h-5 w-[34rem] max-w-full rounded bg-white/60"></div>
						<div class="grid grid-cols-1 gap-2 text-sm md:grid-cols-2">
							<div>
								<div class="mb-2 h-5 w-40 rounded bg-white/60"></div>
								<div class="mb-1 h-4 w-20 rounded bg-white/60"></div>
								<div class="h-4 w-20 rounded bg-white/60"></div>
							</div>
							<div>
								<div class="mb-2 h-5 w-44 rounded bg-white/60"></div>
								<div class="mb-1 h-4 w-20 rounded bg-white/60"></div>
								<div class="h-4 w-20 rounded bg-white/60"></div>
							</div>
						</div>
						<div class="mt-4 grid grid-cols-4 gap-4 text-sm">
							{#each [0, 1, 2, 3] as columnIndex (`summary-column-${skeletonId}-${columnIndex}`)}
								<div>
									<div class="mb-2 h-5 w-28 rounded bg-white/60"></div>
									<div class="mb-1 h-4 w-24 rounded bg-white/60"></div>
									<div class="mb-1 h-4 w-20 rounded bg-white/60"></div>
									<div class="mb-1 h-4 w-24 rounded bg-white/60"></div>
									<div class="h-4 w-20 rounded bg-white/60"></div>
								</div>
							{/each}
						</div>
					</div>
					<div class="space-y-4 p-4">
						<div class="h-6 w-36 rounded bg-gray-200"></div>
						<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
							<div class="space-y-3">
								<div class="h-5 w-44 rounded bg-gray-200"></div>
								<div class="h-4 w-96 max-w-full rounded bg-gray-100"></div>
								<div class="h-96 rounded bg-gray-100"></div>
							</div>
							<div class="space-y-3">
								<div class="h-5 w-52 rounded bg-gray-200"></div>
								<div class="h-4 w-96 max-w-full rounded bg-gray-100"></div>
								<div class="h-96 rounded bg-gray-100"></div>
							</div>
						</div>
						<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
							<div class="space-y-3">
								<div class="h-5 w-44 rounded bg-gray-200"></div>
								<div class="h-4 w-96 max-w-full rounded bg-gray-100"></div>
								<div class="h-96 rounded bg-gray-100"></div>
							</div>
							<div class="space-y-3">
								<div class="h-5 w-52 rounded bg-gray-200"></div>
								<div class="h-4 w-96 max-w-full rounded bg-gray-100"></div>
								<div class="h-96 rounded bg-gray-100"></div>
							</div>
						</div>
						<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
							<div class="space-y-3">
								<div class="h-5 w-48 rounded bg-gray-200"></div>
								<div class="h-56 rounded bg-gray-100"></div>
							</div>
							<div class="space-y-3">
								<div class="h-5 w-56 rounded bg-gray-200"></div>
								<div class="h-56 rounded bg-gray-100"></div>
							</div>
						</div>
					</div>
				</div>
			{/each}
		</div>
	{:else if summaryRecords.length === 0}
		<div class="rounded-lg border bg-white p-4 text-gray-600 shadow-sm">
			No database summary is available for this file.
		</div>
	{:else}
		<div class="space-y-2">
			{#each summaryRecords as record (record.router)}
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
								<p>First: {formatTimestampAsPST(record.first_timestamp * 1000)}</p>
								<p>Last: {formatTimestampAsPST(record.last_timestamp * 1000)}</p>
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
									{#if errorsSource.get(record.router)}
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
										<StructureFunctionChart
											data={structureFunctionDataSource.get(record.router)!}
										/>
									{:else}
										<div class="rounded border bg-gray-50 p-4 text-gray-600">
											No source structure statistics available.
										</div>
									{/if}
								</div>

								<!-- Destination Address Analysis -->
								<div class="space-y-3">
									<h6 class="text-sm font-medium text-blue-700">Destination Address Analysis</h6>
									{#if errorsDestination.get(record.router)}
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
											data={structureFunctionDataDestination.get(record.router)!}
										/>
									{:else}
										<div class="rounded border bg-gray-50 p-4 text-gray-600">
											No destination structure statistics available.
										</div>
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
									{#if errorsSpectrumSource.get(record.router)}
										<div class="rounded border border-red-200 bg-red-50 p-4 text-red-700">
											<p>
												Error loading source spectrum: {errorsSpectrumSource.get(record.router)}
											</p>
											<button
												class="mt-2 rounded bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
												onclick={() => reloadSpectrum(record.router, true)}
											>
												Retry Source
											</button>
										</div>
									{:else if spectrumDataSource.get(record.router)}
										<SpectrumChart data={spectrumDataSource.get(record.router)!} />
									{:else}
										<div class="rounded border bg-gray-50 p-4 text-gray-600">
											No source spectrum statistics available.
										</div>
									{/if}
								</div>

								<!-- Destination Address Spectrum -->
								<div class="space-y-3">
									<h6 class="text-sm font-medium text-blue-700">Destination Address Spectrum</h6>
									{#if errorsSpectrumDestination.get(record.router)}
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
										<SpectrumChart data={spectrumDataDestination.get(record.router)!} />
									{:else}
										<div class="rounded border bg-gray-50 p-4 text-gray-600">
											No destination spectrum statistics available.
										</div>
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
										<SingularitiesList data={singularitiesDataSource.get(record.router)!} />
									{/if}
								</div>
								<!-- Destination Address Singularities -->
								<div class="space-y-3">
									<h6 class="text-sm font-medium text-blue-700">
										Destination Address Singularities
									</h6>
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
										<SingularitiesList data={singularitiesDataDestination.get(record.router)!} />
									{/if}
								</div>
							</div>
						</div>
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
