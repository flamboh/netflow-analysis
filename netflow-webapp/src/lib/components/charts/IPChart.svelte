<script lang="ts">
	import { onDestroy, tick } from 'svelte';
	import { Chart } from 'chart.js/auto';
	import type { RouterConfig } from '$lib/components/netflow/types.ts';
	import {
		IP_METRIC_OPTIONS,
		type IpGranularity,
		type IpMetricKey,
		type IpMetricOption,
		type IpStatsBucket,
		type IpStatsResponse
	} from '$lib/types/types';

	const DEFAULT_METRICS: IpMetricKey[] = ['saIpv4Count', 'daIpv4Count'];

	const props = $props<{
		startDate?: string;
		endDate?: string;
		granularity?: IpGranularity;
		routers?: RouterConfig;
		activeMetrics?: IpMetricKey[];
	}>();
	const today = new Date();
	const formatDate = (date: Date): string => new Date(date).toISOString().slice(0, 10);
	const getStartDate = () => props.startDate ?? '2025-01-01';
	const getEndDate = () => props.endDate ?? formatDate(today);
	const getGranularity = () => props.granularity ?? '1d';

	function deriveSelectedRouters(config: RouterConfig | undefined): string[] {
		if (!config) {
			return [];
		}
		return Object.entries(config)
			.filter(([, enabled]) => enabled)
			.map(([name]) => name.trim())
			.filter((name) => name.length > 0)
			.sort();
	}

	function arraysEqual<T>(a: T[], b: T[]): boolean {
		if (a.length !== b.length) {
			return false;
		}
		return a.every((value, index) => value === b[index]);
	}

	let activeMetrics = $state<IpMetricKey[]>(props.activeMetrics ?? DEFAULT_METRICS);
	let currentGranularity = $state<IpGranularity>(props.granularity ?? '1d');

	let buckets = $state<IpStatsBucket[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	let chartCanvas = $state<HTMLCanvasElement | null>(null);
	let chart: Chart<'line'> | null = null;

	function toEpochSeconds(dateString: string, isEnd = false): number {
		const base = new Date(dateString);
		if (Number.isNaN(base.getTime())) {
			return 0;
		}
		const offset = isEnd ? 24 * 60 * 60 * 1000 : 0;
		return Math.floor((base.getTime() + offset) / 1000);
	}

	function formatBucketLabel(bucket: IpStatsBucket, granularity: IpGranularity): string {
		const start = new Date(bucket.bucketStart * 1000);
		const year = start.getFullYear();
		const month = `${start.getMonth() + 1}`.padStart(2, '0');
		const day = `${start.getDate()}`.padStart(2, '0');
		const hours = `${start.getHours()}`.padStart(2, '0');
		const minutes = `${start.getMinutes()}`.padStart(2, '0');

		if (granularity === '1d') {
			return `${year}-${month}-${day}`;
		}

		return `${year}-${month}-${day} ${hours}:${minutes}`;
	}

	const HUE_STEP = 70;
	const FAMILY_STYLES: Record<
		IpMetricOption['family'],
		{
			hue: number;
			saturation: number;
			lightness: { source: number; destination: number };
		}
	> = {
		ipv4: { hue: 130, saturation: 65, lightness: { source: 58, destination: 38 } },
		ipv6: { hue: 25, saturation: 72, lightness: { source: 60, destination: 40 } }
	};

	function buildColors(option: IpMetricOption, routerIndex: number) {
		const style = FAMILY_STYLES[option.family];
		const hue = (style.hue + routerIndex * HUE_STEP) % 360;
		const lightness =
			option.variant === 'source' ? style.lightness.source : style.lightness.destination;
		const stroke = `hsl(${hue}, ${style.saturation}%, ${lightness}%)`;
		const fill = `hsla(${hue}, ${style.saturation}%, ${lightness}%, 0.18)`;
		return { stroke, fill };
	}

	function destroyChart() {
		if (chart) {
			chart.destroy();
			chart = null;
		}
	}

	function renderChart() {
		const canvas = chartCanvas;
		if (!canvas) {
			return;
		}

		const selectedBuckets = buckets;

		if (activeMetrics.length === 0 || selectedBuckets.length === 0) {
			if (chart) {
				chart.data.labels = [];
				chart.data.datasets = [];
				chart.update();
			}
			return;
		}

		const bucketStarts = Array.from(
			new Set(selectedBuckets.map((bucket) => bucket.bucketStart))
		).sort((a, b) => a - b);
		const routers = Array.from(new Set(selectedBuckets.map((bucket) => bucket.router))).sort();

		const labelSamples = new Map<number, IpStatsBucket>();
		selectedBuckets.forEach((bucket) => {
			if (!labelSamples.has(bucket.bucketStart)) {
				labelSamples.set(bucket.bucketStart, bucket);
			}
		});

		const labels = bucketStarts.map((bucketStart) => {
			const bucket = labelSamples.get(bucketStart);
			return bucket ? formatBucketLabel(bucket, currentGranularity) : '';
		});

		const bucketMap = new Map<string, IpStatsBucket>();
		selectedBuckets.forEach((bucket) => {
			bucketMap.set(`${bucket.router}-${bucket.bucketStart}`, bucket);
		});

		const datasets = routers.flatMap((router, routerIndex) =>
			IP_METRIC_OPTIONS.filter((option) => activeMetrics.includes(option.key)).map((option) => {
				const { stroke, fill } = buildColors(option, routerIndex);
				const data = bucketStarts.map((bucketStart) => {
					const bucket = bucketMap.get(`${router}-${bucketStart}`);
					return bucket ? bucket[option.key] : null;
				});

				return {
					label: `${option.label} – ${router}`,
					data,
					borderColor: stroke,
					backgroundColor: fill,
					tension: 0.3,
					fill: false,
					pointRadius: 0,
					pointHoverRadius: 4,
					spanGaps: true
				};
			})
		);

		if (datasets.length === 0 || labels.length === 0) {
			if (chart) {
				chart.data.labels = [];
				chart.data.datasets = [];
				chart.update();
			}
			return;
		}

		if (!chart) {
			chart = new Chart(canvas, {
				type: 'line',
				data: { labels, datasets },
				options: {
					responsive: true,
					maintainAspectRatio: false,
					interaction: { mode: 'index', intersect: false },
					plugins: {
						legend: { position: 'top' }
					},
					scales: {
						x: {
							title: {
								display: true,
								text: `Time (${currentGranularity})`
							}
						},
						y: {
							beginAtZero: true,
							title: {
								display: true,
								text: 'Unique IPs'
							}
						}
					}
				}
			});
		} else {
			chart.data.labels = labels;
			chart.data.datasets = datasets as never[];
			chart.options.scales = {
				x: {
					...chart.options.scales?.x,
					title: { display: true, text: `Time (${currentGranularity})` }
				},
				y: {
					...chart.options.scales?.y,
					title: { display: true, text: 'Unique IPs' }
				}
			};
			chart.update();
		}
	}

	type FilterInputs = {
		startDate: string;
		endDate: string;
		granularity: IpGranularity;
		routers: string[];
	};

	let lastFiltersKey = '';
	let requestToken = 0;

	async function loadData(filters: FilterInputs, token: number) {
		loading = true;
		error = null;
		destroyChart();

		const params = new URLSearchParams({
			startDate: toEpochSeconds(filters.startDate).toString(),
			endDate: toEpochSeconds(filters.endDate, true).toString(),
			granularity: filters.granularity,
			routers: filters.routers.join(',')
		});

		try {
			const response = await fetch(`/api/ip/stats?${params.toString()}`);
			if (!response.ok) {
				const message = await response.text();
				throw new Error(message || 'Failed to load IP statistics');
			}
			const data = (await response.json()) as IpStatsResponse;
			if (token !== requestToken) {
				return;
			}
			buckets = data.buckets;
			loading = false;
			await tick();
			renderChart();
		} catch (err) {
			if (token !== requestToken) {
				return;
			}
			error = err instanceof Error ? err.message : 'Unexpected error loading IP statistics';
			buckets = [];
			loading = false;
			destroyChart();
		}
	}

	onDestroy(() => {
		destroyChart();
	});

	$effect(() => {
		const routerConfig = props.routers;
		if (!routerConfig || Object.keys(routerConfig).length === 0) {
			return;
		}

		const filters: FilterInputs = {
			startDate: getStartDate(),
			endDate: getEndDate(),
			granularity: getGranularity(),
			routers: deriveSelectedRouters(routerConfig)
		};

		const incomingMetrics = props.activeMetrics ?? DEFAULT_METRICS;
		if (!arraysEqual(activeMetrics, incomingMetrics)) {
			activeMetrics = [...incomingMetrics];
			// renderChart keeps chart in sync with new metrics without re-fetching data
			renderChart();
		}

		currentGranularity = filters.granularity;

		if (filters.routers.length === 0) {
			error = 'Select at least one router to view IP statistics';
			buckets = [];
			destroyChart();
			lastFiltersKey = JSON.stringify(filters);
			loading = false;
			return;
		}

		const nextKey = JSON.stringify(filters);
		if (nextKey === lastFiltersKey) {
			return;
		}

		lastFiltersKey = nextKey;
		const token = ++requestToken;
		loadData(filters, token);
	});
</script>

<div class="rounded-lg border bg-white shadow-sm">
	<div class="border-b p-4">
		<h3 class="text-lg font-semibold text-gray-900">Unique IP Counts</h3>
	</div>
	<div class="p-4">
		<div
			class="h-[320px] min-h-[240px] resize-y overflow-auto rounded-md border border-gray-200 bg-white/60"
		>
			{#if loading}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">Loading IP data...</div>
				</div>
			{:else if error}
				<div class="flex h-full items-center justify-center">
					<div class="text-red-500">{error}</div>
				</div>
			{:else if activeMetrics.length === 0}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">Select at least one metric to display.</div>
				</div>
			{:else if buckets.length === 0}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">No IP data for the selected window.</div>
				</div>
			{:else}
				<div class="h-full">
					<canvas bind:this={chartCanvas} aria-label="IP chart"></canvas>
				</div>
			{/if}
		</div>
	</div>
</div>
