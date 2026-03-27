<script lang="ts">
	import { afterNavigate } from '$app/navigation';
	import NetflowFileHeader from '$lib/components/netflow/NetflowFileHeader.svelte';
	import NetflowFileLoadingSkeleton from '$lib/components/netflow/NetflowFileLoadingSkeleton.svelte';
	import NetflowFileMessageCard from '$lib/components/netflow/NetflowFileMessageCard.svelte';
	import NetflowFileRouterCard from '$lib/components/netflow/NetflowFileRouterCard.svelte';
	import type {
		FileIpCounts,
		NetflowFileDetailsResponse,
		NetflowFileSummaryRecord,
		SingularitiesData,
		SpectrumData,
		StructureFunctionData
	} from '$lib/types/types';
	import {
		createDateFromPSTComponents,
		epochToPSTComponents,
		formatTimestampAsPST
	} from '$lib/utils/timezone';
	import { SvelteMap } from 'svelte/reactivity';
	import { onDestroy, onMount } from 'svelte';

	type NetflowFileDetailData = {
		dataset: string;
		slug: string;
		fileInfo: {
			year: string;
			month: string;
			day: string;
			hour: string;
			minute: string;
			filename: string;
		};
	};

	let { data }: { data: NetflowFileDetailData } = $props();
	let summaryRecords = $state<NetflowFileSummaryRecord[]>([]);
	let summaryLoading = $state(false);
	let summaryError = $state('');
	let summarySkeletonVisible = $state(false);
	let summarySkeletonCount = $state(2);
	const structureFunctionDataSource = new SvelteMap<string, StructureFunctionData>();
	const structureFunctionDataDestination = new SvelteMap<string, StructureFunctionData>();
	const spectrumDataSource = new SvelteMap<string, SpectrumData>();
	const spectrumDataDestination = new SvelteMap<string, SpectrumData>();
	const singularitiesDataSource = new SvelteMap<string, SingularitiesData>();
	const singularitiesDataDestination = new SvelteMap<string, SingularitiesData>();
	const loadingStructureSource = new SvelteMap<string, boolean>();
	const loadingStructureDestination = new SvelteMap<string, boolean>();
	const loadingSpectrumSource = new SvelteMap<string, boolean>();
	const loadingSpectrumDestination = new SvelteMap<string, boolean>();
	const loadingSingularitiesSource = new SvelteMap<string, boolean>();
	const loadingSingularitiesDestination = new SvelteMap<string, boolean>();
	const requestedSingularitiesSource = new SvelteMap<string, boolean>();
	const requestedSingularitiesDestination = new SvelteMap<string, boolean>();
	const errorsSource = new SvelteMap<string, string>();
	const errorsDestination = new SvelteMap<string, string>();
	const errorsSpectrumSource = new SvelteMap<string, string>();
	const errorsSpectrumDestination = new SvelteMap<string, string>();
	const errorsSingularitiesSource = new SvelteMap<string, string>();
	const errorsSingularitiesDestination = new SvelteMap<string, string>();
	const IPCountsSource = new SvelteMap<string, FileIpCounts>();
	const IPCountsDestination = new SvelteMap<string, FileIpCounts>();

	const formatCount = (value: number | null | undefined) =>
		typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString() : 'N/A';

	function getNextSlug(slug: string) {
		if (!slug || slug.length !== 12 || !/^\d{12}$/.test(slug)) {
			return slug;
		}
		const year = parseInt(slug.slice(0, 4), 10);
		const month = parseInt(slug.slice(4, 6), 10);
		const day = parseInt(slug.slice(6, 8), 10);
		const hour = parseInt(slug.slice(8, 10), 10);
		const minute = parseInt(slug.slice(10, 12), 10);
		const currentDate = createDateFromPSTComponents(year, month, day, hour, minute);
		const nextDate = new Date(currentDate.getTime() + 5 * 60 * 1000);
		const nextPST = epochToPSTComponents(Math.floor(nextDate.getTime() / 1000));

		return `${nextPST.year}${String(nextPST.month).padStart(2, '0')}${String(nextPST.day).padStart(2, '0')}${String(nextPST.hours).padStart(2, '0')}${String(nextPST.minutes).padStart(2, '0')}`;
	}

	const nextSlug = $derived(getNextSlug(data.slug));
	const SUMMARY_SKELETON_DELAY_MS = 150;
	let summaryLoadDelayId: ReturnType<typeof setTimeout> | null = null;
	let currentKey = $state('');
	let loadToken = 0;

	function getDataKey(dataset: string, slug: string) {
		return `${dataset}:${slug}`;
	}

	function clearSummaryLoadDelay() {
		if (summaryLoadDelayId !== null) {
			clearTimeout(summaryLoadDelayId);
			summaryLoadDelayId = null;
		}
	}

	function startSummaryLoading() {
		clearSummaryLoadDelay();
		summaryLoading = true;
		summarySkeletonVisible = false;
		summaryLoadDelayId = setTimeout(() => {
			if (summaryLoading) {
				summarySkeletonVisible = true;
			}
		}, SUMMARY_SKELETON_DELAY_MS);
	}

	function finishSummaryLoading() {
		clearSummaryLoadDelay();
		summaryLoading = false;
		summarySkeletonVisible = false;
	}

	function resetDataStores() {
		structureFunctionDataSource.clear();
		structureFunctionDataDestination.clear();
		spectrumDataSource.clear();
		spectrumDataDestination.clear();
		singularitiesDataSource.clear();
		singularitiesDataDestination.clear();
		loadingStructureSource.clear();
		loadingStructureDestination.clear();
		loadingSpectrumSource.clear();
		loadingSpectrumDestination.clear();
		loadingSingularitiesSource.clear();
		loadingSingularitiesDestination.clear();
		requestedSingularitiesSource.clear();
		requestedSingularitiesDestination.clear();
		errorsSource.clear();
		errorsDestination.clear();
		errorsSpectrumSource.clear();
		errorsSpectrumDestination.clear();
		errorsSingularitiesSource.clear();
		errorsSingularitiesDestination.clear();
		IPCountsSource.clear();
		IPCountsDestination.clear();
	}

	function applyDbDetails(result: NetflowFileDetailsResponse) {
		structureFunctionDataSource.clear();
		structureFunctionDataDestination.clear();
		spectrumDataSource.clear();
		spectrumDataDestination.clear();
		IPCountsSource.clear();
		IPCountsDestination.clear();

		for (const routerDetails of result.routers) {
			const router = routerDetails.summary.router;
			if (routerDetails.structureSource) {
				structureFunctionDataSource.set(router, routerDetails.structureSource);
			}
			if (routerDetails.structureDestination) {
				structureFunctionDataDestination.set(router, routerDetails.structureDestination);
			}
			if (routerDetails.spectrumSource) {
				spectrumDataSource.set(router, routerDetails.spectrumSource);
			}
			if (routerDetails.spectrumDestination) {
				spectrumDataDestination.set(router, routerDetails.spectrumDestination);
			}
			if (routerDetails.ipCountsSource) {
				IPCountsSource.set(router, routerDetails.ipCountsSource);
			}
			if (routerDetails.ipCountsDestination) {
				IPCountsDestination.set(router, routerDetails.ipCountsDestination);
			}
		}
	}

	async function loadSummary(dataset: string, slug: string) {
		const token = ++loadToken;
		summarySkeletonCount = Math.max(summaryRecords.length, summarySkeletonCount, 2);
		summaryRecords = [];
		startSummaryLoading();
		summaryError = '';
		resetDataStores();

		try {
			const response = await fetch(
				`/api/netflow/files/${slug}/details?dataset=${encodeURIComponent(dataset)}`
			);
			if (!response.ok) {
				let message = `Failed to load file summary: ${response.statusText}`;
				try {
					const payload = await response.json();
					if (payload?.error) {
						message = payload.error;
					}
				} catch {
					// Keep status text fallback.
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
			finishSummaryLoading();
		} catch (e) {
			if (token !== loadToken) {
				return;
			}
			finishSummaryLoading();
			summaryError = e instanceof Error ? e.message : 'Unknown error occurred';
		}
	}

	onMount(() => {
		currentKey = getDataKey(data.dataset, data.slug);
		void loadSummary(data.dataset, data.slug);
	});

	afterNavigate(() => {
		const nextKey = getDataKey(data.dataset, data.slug);
		if (nextKey !== currentKey) {
			currentKey = nextKey;
			void loadSummary(data.dataset, data.slug);
		}
	});

	onDestroy(() => {
		clearSummaryLoadDelay();
	});

	function reloadSummary() {
		void loadSummary(data.dataset, data.slug);
	}

	async function loadSingularitiesData(
		token: number,
		dataset: string,
		slug: string,
		router: string,
		source: boolean
	) {
		const requestedMap = source ? requestedSingularitiesSource : requestedSingularitiesDestination;
		requestedMap.set(router, true);

		if (source) {
			loadingSingularitiesSource.set(router, true);
			errorsSingularitiesSource.set(router, '');
		} else {
			loadingSingularitiesDestination.set(router, true);
			errorsSingularitiesDestination.set(router, '');
		}

		try {
			const response = await fetch(
				`/api/netflow/files/${slug}/singularities?dataset=${encodeURIComponent(dataset)}&router=${encodeURIComponent(router)}&source=${source}`
			);
			if (token !== loadToken) {
				return;
			}
			if (response.status === 404 || response.status === 422) {
				if (source) {
					singularitiesDataSource.delete(router);
					errorsSingularitiesSource.delete(router);
				} else {
					singularitiesDataDestination.delete(router);
					errorsSingularitiesDestination.delete(router);
				}
				return;
			}
			if (!response.ok) {
				throw new Error(`Failed to load singularities data: ${response.statusText}`);
			}
			const result = (await response.json()) as SingularitiesData;
			if (token !== loadToken) {
				return;
			}

			if (source) {
				singularitiesDataSource.set(router, result);
				errorsSingularitiesSource.delete(router);
			} else {
				singularitiesDataDestination.set(router, result);
				errorsSingularitiesDestination.delete(router);
			}
		} catch (e) {
			if (token !== loadToken) {
				return;
			}
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			if (source) {
				errorsSingularitiesSource.set(router, errorMessage);
			} else {
				errorsSingularitiesDestination.set(router, errorMessage);
			}
		} finally {
			if (token === loadToken) {
				if (source) {
					loadingSingularitiesSource.set(router, false);
				} else {
					loadingSingularitiesDestination.set(router, false);
				}
			}
		}
	}

	async function loadStructureData(
		token: number,
		dataset: string,
		slug: string,
		router: string,
		source: boolean
	) {
		const errorMap = source ? errorsSource : errorsDestination;
		const dataMap = source ? structureFunctionDataSource : structureFunctionDataDestination;
		const loadingMap = source ? loadingStructureSource : loadingStructureDestination;

		errorMap.set(router, '');
		loadingMap.set(router, true);

		try {
			const response = await fetch(
				`/api/netflow/files/${slug}/structure?dataset=${encodeURIComponent(dataset)}&router=${encodeURIComponent(router)}&source=${source}`
			);
			if (token !== loadToken) {
				return;
			}
			if (response.status === 404) {
				dataMap.delete(router);
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
					// Keep status text fallback.
				}
				throw new Error(message);
			}

			const result = (await response.json()) as StructureFunctionData;
			if (token !== loadToken) {
				return;
			}
			dataMap.set(router, result);
		} catch (e) {
			if (token !== loadToken) {
				return;
			}
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			dataMap.delete(router);
			errorMap.set(router, errorMessage);
		} finally {
			if (token === loadToken) {
				loadingMap.set(router, false);
			}
		}
	}

	async function loadSpectrumData(
		token: number,
		dataset: string,
		slug: string,
		router: string,
		source: boolean
	) {
		const errorMap = source ? errorsSpectrumSource : errorsSpectrumDestination;
		const dataMap = source ? spectrumDataSource : spectrumDataDestination;
		const loadingMap = source ? loadingSpectrumSource : loadingSpectrumDestination;

		errorMap.set(router, '');
		loadingMap.set(router, true);

		try {
			const response = await fetch(
				`/api/netflow/files/${slug}/spectrum?dataset=${encodeURIComponent(dataset)}&router=${encodeURIComponent(router)}&source=${source}`
			);
			if (token !== loadToken) {
				return;
			}
			if (response.status === 404) {
				dataMap.delete(router);
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
					// Keep status text fallback.
				}
				throw new Error(message);
			}

			const result = (await response.json()) as SpectrumData;
			if (token !== loadToken) {
				return;
			}
			dataMap.set(router, result);
		} catch (e) {
			if (token !== loadToken) {
				return;
			}
			const errorMessage = e instanceof Error ? e.message : 'Unknown error occurred';
			dataMap.delete(router);
			errorMap.set(router, errorMessage);
		} finally {
			if (token === loadToken) {
				loadingMap.set(router, false);
			}
		}
	}

	function reloadStructure(router: string, source: boolean) {
		void loadStructureData(loadToken, data.dataset, data.slug, router, source);
	}

	function reloadSpectrum(router: string, source: boolean) {
		void loadSpectrumData(loadToken, data.dataset, data.slug, router, source);
	}

	function reloadSingularities(router: string, source: boolean) {
		void loadSingularitiesData(loadToken, data.dataset, data.slug, router, source);
	}

	function hasRequestedSingularities(router: string, source: boolean) {
		return source
			? requestedSingularitiesSource.get(router) === true
			: requestedSingularitiesDestination.get(router) === true;
	}
</script>

<div class="mx-auto max-w-[90vw] px-2 py-2 sm:px-2 lg:px-4">
	<NetflowFileHeader
		dataset={data.dataset}
		{nextSlug}
		filename={data.fileInfo.filename}
		year={data.fileInfo.year}
		month={data.fileInfo.month}
		day={data.fileInfo.day}
		hour={data.fileInfo.hour}
		minute={data.fileInfo.minute}
		processedAt={summaryRecords.length
			? formatTimestampAsPST(Date.parse(summaryRecords[0].processed_at))
			: summaryLoading
				? 'Loading...'
				: 'N/A'}
	/>

	{#if summaryError}
		<NetflowFileMessageCard
			tone="danger"
			message={`Failed to load file summary: ${summaryError}`}
			actionLabel="Retry Summary"
			action={reloadSummary}
		/>
	{:else if summaryLoading && summarySkeletonVisible}
		<NetflowFileLoadingSkeleton count={summarySkeletonCount} />
	{:else if summaryLoading}
		<NetflowFileMessageCard message="Loading file summary..." />
	{:else if summaryRecords.length === 0}
		<NetflowFileMessageCard message="No database summary is available for this file." />
	{:else}
		<div class="space-y-2">
			{#each summaryRecords as record (record.router)}
				<NetflowFileRouterCard
					{record}
					{structureFunctionDataSource}
					{structureFunctionDataDestination}
					{spectrumDataSource}
					{spectrumDataDestination}
					{singularitiesDataSource}
					{singularitiesDataDestination}
					{loadingStructureSource}
					{loadingStructureDestination}
					{loadingSpectrumSource}
					{loadingSpectrumDestination}
					{loadingSingularitiesSource}
					{loadingSingularitiesDestination}
					{errorsSource}
					{errorsDestination}
					{errorsSpectrumSource}
					{errorsSpectrumDestination}
					{errorsSingularitiesSource}
					{errorsSingularitiesDestination}
					{IPCountsSource}
					{IPCountsDestination}
					{reloadStructure}
					{reloadSpectrum}
					{reloadSingularities}
					{hasRequestedSingularities}
					{formatCount}
					{formatTimestampAsPST}
				/>
			{/each}
		</div>
	{/if}
</div>
