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
	let loadingStructureSource = $state(new Map<string, boolean>());
	let loadingStructureDestination = $state(new Map<string, boolean>());
	let loadingSpectrumSource = $state(new Map<string, boolean>());
	let loadingSpectrumDestination = $state(new Map<string, boolean>());
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
		loadingStructureSource = new Map<string, boolean>();
		loadingStructureDestination = new Map<string, boolean>();
		loadingSpectrumSource = new Map<string, boolean>();
		loadingSpectrumDestination = new Map<string, boolean>();
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
			}

			if (routerDetails.structureDestination) {
				nextStructureDestination.set(router, routerDetails.structureDestination);
			}

			if (routerDetails.spectrumSource) {
				nextSpectrumSource.set(router, routerDetails.spectrumSource);
			}

			if (routerDetails.spectrumDestination) {
				nextSpectrumDestination.set(router, routerDetails.spectrumDestination);
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
			const response = await fetch(
				`/api/netflow/files/${slug}/details?dataset=${encodeURIComponent(data.dataset)}`
			);
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
				`/api/netflow/files/${slug}/singularities?dataset=${encodeURIComponent(data.dataset)}&router=${encodeURIComponent(router)}&source=${source}`
			);
			if (!response.ok) {
				throw new Error(`Failed to load singularities data: ${response.statusText}`);
			}
			const result = await response.json();
			if (token !== loadToken) {
				return;
			}

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

	async function loadStructureData(token: number, slug: string, router: string, source: boolean) {
		const errorMap = source ? errorsSource : errorsDestination;
		const dataMap = source ? structureFunctionDataSource : structureFunctionDataDestination;
		const loadingMap = source ? loadingStructureSource : loadingStructureDestination;

		errorMap.set(router, '');
		loadingMap.set(router, true);
		if (source) {
			errorsSource = new Map(errorMap);
			loadingStructureSource = new Map(loadingMap);
		} else {
			errorsDestination = new Map(errorMap);
			loadingStructureDestination = new Map(loadingMap);
		}

		try {
			const response = await fetch(
				`/api/netflow/files/${slug}/structure?dataset=${encodeURIComponent(data.dataset)}&router=${encodeURIComponent(router)}&source=${source}`
			);

			if (token !== loadToken) {
				return;
			}

			if (response.status === 404) {
				dataMap.delete(router);
				if (source) {
					structureFunctionDataSource = new Map(dataMap);
				} else {
					structureFunctionDataDestination = new Map(dataMap);
				}
				return;
			}

			if (!response.ok) {
				let message = `Failed to load structure data: ${response.statusText}`;
				try {
					const payload = await response.json();
					if (payload?.error) {
						message = payload.error;
					}
				} catch {
					// Ignore JSON parse failures and keep the status text fallback.
				}
				throw new Error(message);
			}

			const result = (await response.json()) as StructureFunctionData;
			dataMap.set(router, result);

			if (source) {
				structureFunctionDataSource = new Map(dataMap);
			} else {
				structureFunctionDataDestination = new Map(dataMap);
			}
		} catch (e) {
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			dataMap.delete(router);
			errorMap.set(router, errorMessage);

			if (source) {
				structureFunctionDataSource = new Map(dataMap);
				errorsSource = new Map(errorMap);
			} else {
				structureFunctionDataDestination = new Map(dataMap);
				errorsDestination = new Map(errorMap);
			}
		} finally {
			loadingMap.set(router, false);
			if (source) {
				loadingStructureSource = new Map(loadingMap);
			} else {
				loadingStructureDestination = new Map(loadingMap);
			}
		}
	}

	async function loadSpectrumData(token: number, slug: string, router: string, source: boolean) {
		const errorMap = source ? errorsSpectrumSource : errorsSpectrumDestination;
		const dataMap = source ? spectrumDataSource : spectrumDataDestination;
		const loadingMap = source ? loadingSpectrumSource : loadingSpectrumDestination;

		errorMap.set(router, '');
		loadingMap.set(router, true);
		if (source) {
			errorsSpectrumSource = new Map(errorMap);
			loadingSpectrumSource = new Map(loadingMap);
		} else {
			errorsSpectrumDestination = new Map(errorMap);
			loadingSpectrumDestination = new Map(loadingMap);
		}

		try {
			const response = await fetch(
				`/api/netflow/files/${slug}/spectrum?dataset=${encodeURIComponent(data.dataset)}&router=${encodeURIComponent(router)}&source=${source}`
			);

			if (token !== loadToken) {
				return;
			}

			if (response.status === 404) {
				dataMap.delete(router);
				if (source) {
					spectrumDataSource = new Map(dataMap);
				} else {
					spectrumDataDestination = new Map(dataMap);
				}
				return;
			}

			if (!response.ok) {
				let message = `Failed to load spectrum data: ${response.statusText}`;
				try {
					const payload = await response.json();
					if (payload?.error) {
						message = payload.error;
					}
				} catch {
					// Ignore JSON parse failures and keep the status text fallback.
				}
				throw new Error(message);
			}

			const result = (await response.json()) as SpectrumData;
			dataMap.set(router, result);

			if (source) {
				spectrumDataSource = new Map(dataMap);
			} else {
				spectrumDataDestination = new Map(dataMap);
			}
		} catch (e) {
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			dataMap.delete(router);
			errorMap.set(router, errorMessage);

			if (source) {
				spectrumDataSource = new Map(dataMap);
				errorsSpectrumSource = new Map(errorMap);
			} else {
				spectrumDataDestination = new Map(dataMap);
				errorsSpectrumDestination = new Map(errorMap);
			}
		} finally {
			loadingMap.set(router, false);
			if (source) {
				loadingSpectrumSource = new Map(loadingMap);
			} else {
				loadingSpectrumDestination = new Map(loadingMap);
			}
		}
	}

	function reloadStructure(router: string, source: boolean) {
		void loadStructureData(loadToken, data.slug, router, source);
	}

	function reloadSpectrum(router: string, source: boolean) {
		void loadSpectrumData(loadToken, data.slug, router, source);
	}

	function reloadSingularities(router: string, source: boolean) {
		void loadSingularitiesData(loadToken, data.slug, router, source);
	}
</script>

<div class="mx-auto max-w-[90vw] px-2 py-4 sm:px-2 lg:px-4">
	<div class="mb-4 flex items-center justify-between">
		<div>
			<h1 class="text-text-primary text-lg font-semibold">
				{data.fileInfo.filename}
			</h1>
			<div class="text-text-muted mt-0.5 flex items-center gap-3 text-xs">
				<span
					>{data.fileInfo.year}-{data.fileInfo.month}-{data.fileInfo.day}
					{data.fileInfo.hour}:{data.fileInfo.minute}</span
				>
				{#if summaryRecords.length}
					<span>·</span>
					<span>Processed {formatTimestampAsPST(Date.parse(summaryRecords[0].processed_at))}</span>
				{/if}
			</div>
		</div>
		<a
			class="bg-cisco-blue hover:bg-cisco-blue-dark rounded-lg px-3 py-1.5 text-xs font-medium text-white transition-colors"
			href={`/netflow/files/${nextSlug}?dataset=${encodeURIComponent(data.dataset)}`}
		>
			Next File →
		</a>
	</div>

	{#if summaryError}
		<div
			class="rounded-xl border border-red-300 bg-red-50 p-4 text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-400"
		>
			<p>Failed to load file summary: {summaryError}</p>
			<button
				class="mt-2 rounded-lg bg-red-600 px-3 py-1 text-sm text-white transition-colors hover:bg-red-700"
				onclick={reloadSummary}
			>
				Retry Summary
			</button>
		</div>
	{:else if summaryLoading}
		<div class="space-y-4">
			{#each summarySkeletons as skeletonId (skeletonId)}
				<div class="border-border bg-surface animate-pulse rounded-xl border p-5 shadow-sm">
					<div class="mb-4 flex items-center gap-3">
						<div class="bg-cisco-blue/30 h-5 w-5 rounded-full"></div>
						<div class="bg-surface-hover h-5 w-40 rounded"></div>
					</div>
					<div class="mb-4 grid grid-cols-4 gap-4">
						{#each [0, 1, 2, 3] as columnIndex (`summary-column-${skeletonId}-${columnIndex}`)}
							<div class="space-y-2">
								<div class="bg-surface-hover h-4 w-20 rounded"></div>
								<div class="bg-surface-alt h-3 w-28 rounded"></div>
								<div class="bg-surface-alt h-3 w-24 rounded"></div>
								<div class="bg-surface-alt h-3 w-26 rounded"></div>
							</div>
						{/each}
					</div>
					<div class="grid grid-cols-2 gap-6">
						<div class="bg-surface-alt h-72 rounded-lg"></div>
						<div class="bg-surface-alt h-72 rounded-lg"></div>
					</div>
				</div>
			{/each}
		</div>
	{:else if summaryRecords.length === 0}
		<div class="border-border bg-surface text-text-secondary rounded-xl border p-4 shadow-sm">
			No database summary is available for this file.
		</div>
	{:else}
		<div class="space-y-4">
			{#each summaryRecords as record (record.router)}
				<div class="border-border bg-surface rounded-xl border shadow-sm">
					<div class="border-border border-b p-4">
						<div class="flex items-center gap-2.5">
							<span class="bg-cisco-blue h-2.5 w-2.5 rounded-full"></span>
							<h3 class="text-text-primary text-base font-semibold">{record.router}</h3>
							<span class="text-text-muted truncate font-mono text-xs" title={record.file_path}
								>{record.file_path}</span
							>
						</div>

						<div
							class="border-border mt-3 grid grid-cols-2 gap-x-6 gap-y-1 border-b pb-3 text-xs md:grid-cols-4"
						>
							<div>
								<span class="text-text-muted">Src IPv4</span>
								<span class="text-text-primary ml-1 font-medium"
									>{IPCountsSource.get(record.router)?.ipv4Count == null
										? '...'
										: formatCount(IPCountsSource.get(record.router)?.ipv4Count)}</span
								>
							</div>
							<div>
								<span class="text-text-muted">Src IPv6</span>
								<span class="text-text-primary ml-1 font-medium"
									>{IPCountsSource.get(record.router)?.ipv6Count == null
										? '...'
										: formatCount(IPCountsSource.get(record.router)?.ipv6Count)}</span
								>
							</div>
							<div>
								<span class="text-text-muted">Dst IPv4</span>
								<span class="text-text-primary ml-1 font-medium"
									>{IPCountsDestination.get(record.router)?.ipv4Count == null
										? '...'
										: formatCount(IPCountsDestination.get(record.router)?.ipv4Count)}</span
								>
							</div>
							<div>
								<span class="text-text-muted">Dst IPv6</span>
								<span class="text-text-primary ml-1 font-medium"
									>{IPCountsDestination.get(record.router)?.ipv6Count == null
										? '...'
										: formatCount(IPCountsDestination.get(record.router)?.ipv6Count)}</span
								>
							</div>
						</div>

						<div class="mt-3 overflow-x-auto">
							<table class="w-full text-xs">
								<thead>
									<tr class="text-text-muted text-left">
										<th class="pr-4 pb-1 font-medium"></th>
										<th class="pr-4 pb-1 font-medium">Total</th>
										<th class="pr-4 pb-1 font-medium">TCP</th>
										<th class="pr-4 pb-1 font-medium">UDP</th>
										<th class="pr-4 pb-1 font-medium">ICMP</th>
										<th class="pr-4 pb-1 font-medium">Other</th>
									</tr>
								</thead>
								<tbody class="text-text-primary">
									<tr>
										<td class="text-text-secondary py-0.5 pr-4 font-medium">Flows</td>
										<td class="py-0.5 pr-4 font-mono">{record.flows.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.flows_tcp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.flows_udp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.flows_icmp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.flows_other.toLocaleString()}</td>
									</tr>
									<tr>
										<td class="text-text-secondary py-0.5 pr-4 font-medium">Packets</td>
										<td class="py-0.5 pr-4 font-mono">{record.packets.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.packets_tcp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.packets_udp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.packets_icmp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.packets_other.toLocaleString()}</td>
									</tr>
									<tr>
										<td class="text-text-secondary py-0.5 pr-4 font-medium">Bytes</td>
										<td class="py-0.5 pr-4 font-mono">{record.bytes.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.bytes_tcp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.bytes_udp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.bytes_icmp.toLocaleString()}</td>
										<td class="py-0.5 pr-4 font-mono">{record.bytes_other.toLocaleString()}</td>
									</tr>
								</tbody>
							</table>
						</div>

						<div class="text-text-muted mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs">
							<span>First: {formatTimestampAsPST(record.first_timestamp * 1000)}</span>
							<span>Last: {formatTimestampAsPST(record.last_timestamp * 1000)}</span>
							{#if record.sequence_failures > 0}
								<span class="text-red-500 dark:text-red-400"
									>Seq failures: {record.sequence_failures.toLocaleString()}</span
								>
							{/if}
						</div>
					</div>

					<div class="p-4">
						{#if record.file_exists_on_disk === false}
							<div
								class="mb-4 rounded-lg border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-400"
							>
								File missing on disk — DB stats shown, but MAAD analysis unavailable.
							</div>
						{/if}

						<div class="mb-4 grid grid-cols-1 gap-6 lg:grid-cols-2">
							<h5
								class="text-cisco-blue hidden text-xs font-semibold tracking-wider uppercase lg:block"
							>
								Source
							</h5>
							<h5
								class="text-cisco-blue hidden text-xs font-semibold tracking-wider uppercase lg:block"
							>
								Destination
							</h5>
						</div>
						<div class="space-y-6">
							<div class="space-y-3">
								<h6 class="text-text-secondary text-sm font-medium">Structure Function</h6>
								<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
									<div>
										{#if loadingStructureSource.get(record.router)}
											<div class="flex items-center justify-center py-6">
												<div class="text-text-muted">Loading source structure...</div>
											</div>
										{:else if errorsSource.get(record.router)}
											<div
												class="rounded-xl border border-red-300 bg-red-50 p-4 text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-400"
											>
												<p>Error loading source structure: {errorsSource.get(record.router)}</p>
												<button
													class="mt-2 rounded-lg bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
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
											<div
												class="border-border bg-surface-alt text-text-muted rounded-xl border p-4"
											>
												No source structure available.
											</div>
										{/if}
									</div>
									<div>
										{#if loadingStructureDestination.get(record.router)}
											<div class="flex items-center justify-center py-6">
												<div class="text-text-muted">Loading destination structure...</div>
											</div>
										{:else if errorsDestination.get(record.router)}
											<div
												class="rounded-xl border border-red-300 bg-red-50 p-4 text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-400"
											>
												<p>
													Error loading destination structure: {errorsDestination.get(
														record.router
													)}
												</p>
												<button
													class="mt-2 rounded-lg bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
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
											<div
												class="border-border bg-surface-alt text-text-muted rounded-xl border p-4"
											>
												No destination structure available.
											</div>
										{/if}
									</div>
								</div>
							</div>
							<div class="space-y-3">
								<h6 class="text-text-secondary text-sm font-medium">Spectrum</h6>
								<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
									<div>
										{#if loadingSpectrumSource.get(record.router)}
											<div class="flex items-center justify-center py-6">
												<div class="text-text-muted">Loading source spectrum...</div>
											</div>
										{:else if errorsSpectrumSource.get(record.router)}
											<div
												class="rounded-xl border border-red-300 bg-red-50 p-4 text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-400"
											>
												<p>
													Error loading source spectrum: {errorsSpectrumSource.get(record.router)}
												</p>
												<button
													class="mt-2 rounded-lg bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
													onclick={() => reloadSpectrum(record.router, true)}
												>
													Retry Source
												</button>
											</div>
										{:else if spectrumDataSource.get(record.router)}
											<SpectrumChart data={spectrumDataSource.get(record.router)!} />
										{:else}
											<div
												class="border-border bg-surface-alt text-text-muted rounded-xl border p-4"
											>
												No source spectrum available.
											</div>
										{/if}
									</div>
									<div>
										{#if loadingSpectrumDestination.get(record.router)}
											<div class="flex items-center justify-center py-6">
												<div class="text-text-muted">Loading destination spectrum...</div>
											</div>
										{:else if errorsSpectrumDestination.get(record.router)}
											<div
												class="rounded-xl border border-red-300 bg-red-50 p-4 text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-400"
											>
												<p>
													Error loading destination spectrum: {errorsSpectrumDestination.get(
														record.router
													)}
												</p>
												<button
													class="mt-2 rounded-lg bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
													onclick={() => reloadSpectrum(record.router, false)}
												>
													Retry Destination
												</button>
											</div>
										{:else if spectrumDataDestination.get(record.router)}
											<SpectrumChart data={spectrumDataDestination.get(record.router)!} />
										{:else}
											<div
												class="border-border bg-surface-alt text-text-muted rounded-xl border p-4"
											>
												No destination spectrum available.
											</div>
										{/if}
									</div>
								</div>
							</div>
							<div class="space-y-3">
								<h6 class="text-text-secondary text-sm font-medium">Singularities</h6>
								<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
									<div>
										{#if loadingSingularitiesSource.get(record.router)}
											<div class="flex items-center justify-center py-6">
												<div class="text-text-muted">Loading source singularities...</div>
											</div>
										{:else if errorsSingularitiesSource.get(record.router)}
											<div
												class="rounded-xl border border-red-300 bg-red-50 p-4 dark:border-red-800 dark:bg-red-950"
											>
												<p class="text-red-700 dark:text-red-400">
													Error loading source singularities: {errorsSingularitiesSource.get(
														record.router
													)}
												</p>
												<button
													class="mt-2 rounded-lg bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
													onclick={() => reloadSingularities(record.router, true)}
												>
													Retry Source
												</button>
											</div>
										{:else if singularitiesDataSource.get(record.router)}
											<SingularitiesList data={singularitiesDataSource.get(record.router)!} />
										{:else}
											<div
												class="border-border bg-surface-alt text-text-muted rounded-xl border p-4"
											>
												No source singularities available.
											</div>
										{/if}
									</div>
									<div>
										{#if loadingSingularitiesDestination.get(record.router)}
											<div class="flex items-center justify-center py-6">
												<div class="text-text-muted">Loading destination singularities...</div>
											</div>
										{:else if errorsSingularitiesDestination.get(record.router)}
											<div
												class="rounded-xl border border-red-300 bg-red-50 p-4 dark:border-red-800 dark:bg-red-950"
											>
												<p class="text-red-700 dark:text-red-400">
													Error loading destination singularities: {errorsSingularitiesDestination.get(
														record.router
													)}
												</p>
												<button
													class="mt-2 rounded-lg bg-red-600 px-3 py-1 text-sm text-white hover:bg-red-700"
													onclick={() => reloadSingularities(record.router, false)}
												>
													Retry Destination
												</button>
											</div>
										{:else if singularitiesDataDestination.get(record.router)}
											<SingularitiesList data={singularitiesDataDestination.get(record.router)!} />
										{:else}
											<div
												class="border-border bg-surface-alt text-text-muted rounded-xl border p-4"
											>
												No destination singularities available.
											</div>
										{/if}
									</div>
								</div>
							</div>
						</div>
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
