<script lang="ts">
	import { createEventDispatcher, onDestroy, tick } from 'svelte';
	import { goto } from '$app/navigation';
	import { Chart } from 'chart.js/auto';
	import { getRelativePosition } from 'chart.js/helpers';
	import type { ActiveElement, ChartEvent } from 'chart.js';
	import type { GroupByOption, RouterConfig } from '$lib/components/netflow/types.ts';
	import {
		type IpGranularity,
		type ProtocolMetricKey,
		type ProtocolStatsBucket,
		type ProtocolStatsResponse
	} from '$lib/types/types';
	import { parseClickedLabel, generateSlugFromLabel } from './chart-utils';
	import {
		dateStringToEpochPST,
		epochToPSTComponents,
		formatDateAsPSTDateString,
		getWeekdayName
	} from '$lib/utils/timezone';

	type SeriesKey = 'uniqueProtocolsIpv4' | 'uniqueProtocolsIpv6';

	const METRIC_LABELS: Record<SeriesKey, string> = {
		uniqueProtocolsIpv4: 'Unique Protocols (IPv4)',
		uniqueProtocolsIpv6: 'Unique Protocols (IPv6)'
	};

	const IP_TO_GROUP_BY: Record<IpGranularity, GroupByOption> = {
		'1d': 'date',
		'1h': 'hour',
		'30m': '30min',
		'5m': '5min'
	};

	const GROUP_BY_TRANSITIONS: Record<GroupByOption, GroupByOption | null> = {
		date: 'hour',
		hour: '30min',
		'30min': '5min',
		'5min': null
	};

	const dispatch = createEventDispatcher<{
		dateChange: { startDate: string; endDate: string };
		groupByChange: { groupBy: GroupByOption };
	}>();

	const props = $props<{
		startDate?: string;
		endDate?: string;
		granularity?: IpGranularity;
		routers?: RouterConfig;
		activeMetrics?: ProtocolMetricKey[];
	}>();

	const today = new Date();
	const formatDate = (date: Date): string => formatDateAsPSTDateString(date);
	const getStartDate = () => props.startDate ?? '2025-01-01';
	const getEndDate = () => props.endDate ?? formatDate(today);
	const getGranularity = () => props.granularity ?? '1h';

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
		if (a.length !== b.length) return false;
		return a.every((value, index) => value === b[index]);
	}

	const DEFAULT_METRICS: ProtocolMetricKey[] = ['uniqueProtocolsIpv4', 'uniqueProtocolsIpv6'];

	let activeMetrics = $state<ProtocolMetricKey[]>(props.activeMetrics ?? DEFAULT_METRICS);
	let currentGranularity = $state<IpGranularity>(getGranularity());

	let buckets = $state<ProtocolStatsBucket[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	let chartCanvas = $state<HTMLCanvasElement | null>(null);
	let chart: Chart<'line'> | null = null;

	function toEpochSeconds(dateString: string, isEnd = false): number {
		return dateStringToEpochPST(dateString, isEnd);
	}

	function formatBucketLabel(bucket: ProtocolStatsBucket, granularity: IpGranularity): string {
		const pst = epochToPSTComponents(bucket.bucketStart);
		const year = pst.year;
		const month = `${pst.month}`.padStart(2, '0');
		const day = `${pst.day}`.padStart(2, '0');
		const hours = `${pst.hours}`.padStart(2, '0');
		const minutes = `${pst.minutes}`.padStart(2, '0');
		return granularity === '1d'
			? `${year}-${month}-${day}`
			: `${year}-${month}-${day} ${hours}:${minutes}`;
	}

	function formatTickLabel(bucketStart: number, granularity: IpGranularity, _index: number): string {
		const pst = epochToPSTComponents(bucketStart);
		const day = pst.day.toString().padStart(2, '0');
		const month = pst.month.toString().padStart(2, '0');
		const hours = pst.hours;
		const minutes = pst.minutes;
		const weekday = getWeekdayName(pst.dayOfWeek);

		if (granularity === '1d') {
			return pst.dayOfWeek === 1 ? `Mon ${month}/${day}` : '';
		}

		if (granularity === '1h') {
			if (hours === 0) {
				return `${weekday} ${pst.month}/${pst.day}`;
			}
			return '';
		}

		if (granularity === '30m') {
			if (minutes === 0 && (hours === 0 || hours === 12)) {
				return `${weekday} ${pst.month}/${pst.day} ${hours.toString().padStart(2, '0')}:00`;
			}
			return '';
		}

		if (granularity === '5m') {
			if (minutes === 0) {
				return `${weekday} ${pst.month}/${pst.day} ${hours.toString().padStart(2, '0')}:00`;
			}
			return '';
		}

		return '';
	}

	function shouldHighlightTick(bucketStart: number, granularity: IpGranularity, index: number): boolean {
		const pst = epochToPSTComponents(bucketStart);
		const hours = pst.hours;
		const minutes = pst.minutes;

		if (granularity === '1d') {
			return pst.dayOfWeek === 1;
		}
		if (granularity === '1h') {
			return hours === 0;
		}
		if (granularity === '30m') {
			return minutes === 0 && (hours === 0 || hours === 12);
		}
		if (granularity === '5m') {
			return minutes === 0;
		}
		return index === 0;
	}

	const HUE_STEP = 110;
	const SERIES_COLOR: Record<SeriesKey, { hue: number; saturation: number; lightness: number }> = {
		uniqueProtocolsIpv4: { hue: 210, saturation: 70, lightness: 50 },
		uniqueProtocolsIpv6: { hue: 35, saturation: 75, lightness: 48 }
	};

	function buildColors(key: SeriesKey, routerIndex: number) {
		const base = SERIES_COLOR[key];
		const hue = (base.hue + routerIndex * HUE_STEP) % 360;
		const stroke = `hsl(${hue}, ${base.saturation}%, ${base.lightness}%)`;
		const fill = `hsla(${hue}, ${base.saturation}%, ${base.lightness}%, 0.2)`;
		return { stroke, fill };
	}

	function destroyChart() {
		if (chart) {
			chart.destroy();
			chart = null;
		}
	}

	function emitDrilldown(nextGroupBy: GroupByOption, start: Date, end: Date) {
		dispatch('groupByChange', { groupBy: nextGroupBy });
		dispatch('dateChange', {
			startDate: formatDate(start),
			endDate: formatDate(end)
		});
	}

	function getLabelFromIndex(index: number): string | null {
		if (!chart || !chart.data.labels) return null;
		const labels = chart.data.labels as string[];
		if (index < 0 || index >= labels.length) return null;
		return labels[index] ?? null;
	}

	function handleChartClick(event: ChartEvent, activeElements: ActiveElement[]) {
		if (!chart || !chart.data.labels) return;

		const groupBy = IP_TO_GROUP_BY[currentGranularity];
		if (!groupBy) return;

		const labels = chart.data.labels as string[];
		if (labels.length === 0) return;

		const canvasPosition = getRelativePosition(event, chart);
		const dataX = chart.scales.x.getValueForPixel(canvasPosition.x);

		let labelIndex: number | null = null;
		if (typeof dataX === 'number' && Number.isFinite(dataX)) {
			const roundedIndex = Math.round(dataX);
			if (roundedIndex >= 0 && roundedIndex < labels.length) {
				labelIndex = roundedIndex;
			} else if (roundedIndex < 0) {
				labelIndex = 0;
			} else {
				labelIndex = labels.length - 1;
			}
		}

		const fallbackIndex = activeElements.length > 0 ? activeElements[0].index : null;
		const targetIndex = labelIndex ?? fallbackIndex;
		const label = targetIndex !== null ? getLabelFromIndex(targetIndex) : labels[0];

		if (!label) return;

		const clickedDate = parseClickedLabel(label, groupBy);
		if (!(clickedDate instanceof Date) || Number.isNaN(clickedDate.getTime())) return;
		const activeLabel = fallbackIndex !== null ? getLabelFromIndex(fallbackIndex) : null;

		if (groupBy === '5min') {
			const labelForSlug = activeLabel ?? label;
			const slug = generateSlugFromLabel(labelForSlug, '5min');
			if (slug) goto(`/api/netflow/files/${slug}`);
			return;
		}

		const nextGroupBy = GROUP_BY_TRANSITIONS[groupBy];
		if (!nextGroupBy) return;

		if (groupBy === 'date') {
			const rangeStart = new Date(clickedDate.getTime() - 15 * 24 * 60 * 60 * 1000);
			const rangeEnd = new Date(clickedDate.getTime() + 16 * 24 * 60 * 60 * 1000);
			emitDrilldown(nextGroupBy, rangeStart, rangeEnd);
		} else if (groupBy === 'hour') {
			const rangeStart = new Date(clickedDate.getTime() - 3 * 24 * 60 * 60 * 1000);
			const rangeEnd = new Date(clickedDate.getTime() + 4 * 24 * 60 * 60 * 1000);
			emitDrilldown(nextGroupBy, rangeStart, rangeEnd);
		} else if (groupBy === '30min') {
			const rangeEnd = new Date(clickedDate.getTime() + 24 * 60 * 60 * 1000);
			emitDrilldown(nextGroupBy, clickedDate, rangeEnd);
		}
	}

	function renderChart() {
		const selectedBuckets = buckets;

		if (activeMetrics.length === 0 || selectedBuckets.length === 0) {
			destroyChart();
			return;
		}

		const canvas = chartCanvas;
		if (!canvas) return;

		const bucketStarts = Array.from(
			new Set(selectedBuckets.map((bucket) => bucket.bucketStart))
		).sort((a, b) => a - b);
		const routers = Array.from(new Set(selectedBuckets.map((bucket) => bucket.router))).sort();

		const labelSamples = new Map<number, ProtocolStatsBucket>();
		selectedBuckets.forEach((bucket) => {
			if (!labelSamples.has(bucket.bucketStart)) {
				labelSamples.set(bucket.bucketStart, bucket);
			}
		});

		const labels = bucketStarts.map((bucketStart) => {
			const bucket = labelSamples.get(bucketStart);
			return bucket ? formatBucketLabel(bucket, currentGranularity) : '';
		});

		const bucketMap = new Map<string, ProtocolStatsBucket>();
		selectedBuckets.forEach((bucket) => {
			bucketMap.set(`${bucket.router}-${bucket.bucketStart}`, bucket);
		});

		const datasets = routers.flatMap((router, routerIndex) =>
			(['uniqueProtocolsIpv4', 'uniqueProtocolsIpv6'] as const)
				.filter((key) => activeMetrics.includes(key))
				.map((key) => {
					const { stroke, fill } = buildColors(key, routerIndex);
					const data = bucketStarts.map((bucketStart) => {
						const bucket = bucketMap.get(`${router}-${bucketStart}`);
						return bucket ? bucket[key] : null;
					});

					return {
						label: `${METRIC_LABELS[key]} – ${router}`,
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
					onClick: handleChartClick,
					responsive: true,
					maintainAspectRatio: false,
					interaction: { mode: 'index', intersect: false },
					plugins: { legend: { position: 'top' } },
					scales: {
						x: {
							title: { display: true, text: `Time (${currentGranularity})` },
							ticks: {
								autoSkip: false,
								maxRotation: 45,
								minRotation: 45,
								callback: (_value, idx) =>
									formatTickLabel(bucketStarts[idx as number] ?? 0, currentGranularity, idx as number)
							},
							grid: {
								color: (ctx) =>
									shouldHighlightTick(
										bucketStarts[ctx.index] ?? 0,
										currentGranularity,
										ctx.index
									)
										? 'rgba(0,0,0,0.08)'
										: 'rgba(0,0,0,0.02)'
							}
						},
						y: { beginAtZero: true, title: { display: true, text: 'Unique Protocols' } }
					}
				}
			});
		} else {
			chart.data.labels = labels;
			chart.data.datasets = datasets as never[];
			chart.options.scales = {
				x: {
					...chart.options.scales?.x,
					title: { display: true, text: `Time (${currentGranularity})` },
					ticks: {
						autoSkip: false,
						maxRotation: 45,
						minRotation: 45,
						callback: (_value, idx) =>
							formatTickLabel(bucketStarts[idx as number] ?? 0, currentGranularity, idx as number)
					},
					grid: {
						color: (ctx) =>
							shouldHighlightTick(bucketStarts[ctx.index] ?? 0, currentGranularity, ctx.index)
								? 'rgba(0,0,0,0.08)'
								: 'rgba(0,0,0,0.02)'
					}
				},
				y: { ...chart.options.scales?.y, title: { display: true, text: 'Unique Protocols' } }
			};
			chart.options.onClick = handleChartClick;
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
			const response = await fetch(`/api/protocol/stats?${params.toString()}`);
			if (!response.ok) {
				const message = await response.text();
				throw new Error(message || 'Failed to load protocol statistics');
			}
			const data = (await response.json()) as ProtocolStatsResponse;
			if (token !== requestToken) return;
			buckets = data.buckets;
			loading = false;
			await tick();
			renderChart();
		} catch (err) {
			if (token !== requestToken) return;
			error = err instanceof Error ? err.message : 'Unexpected error loading protocol statistics';
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
			void (async () => {
				await tick();
				renderChart();
			})();
		}

		currentGranularity = filters.granularity;

		if (filters.routers.length === 0) {
			error = 'Select at least one router to view protocol statistics';
			buckets = [];
			destroyChart();
			lastFiltersKey = JSON.stringify(filters);
			loading = false;
			return;
		}

		const nextKey = JSON.stringify(filters);
		if (nextKey === lastFiltersKey) return;

		lastFiltersKey = nextKey;
		const token = ++requestToken;
		loadData(filters, token);
	});
</script>

<div class="rounded-lg border bg-white shadow-sm">
	<div class="border-b p-4">
		<h3 class="text-lg font-semibold text-gray-900">Unique Protocol Counts</h3>
	</div>
	<div class="p-4">
		<div
			class="h-[320px] min-h-[240px] resize-y overflow-auto rounded-md border border-gray-200 bg-white/60"
		>
			{#if loading}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">Loading protocol data...</div>
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
					<div class="text-gray-500">No protocol data for the selected window.</div>
				</div>
			{:else}
				<div class="h-full">
					<canvas bind:this={chartCanvas} aria-label="Protocol chart"></canvas>
				</div>
			{/if}
		</div>
	</div>
</div>
