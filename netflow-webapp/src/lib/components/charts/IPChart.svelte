<script lang="ts">
	import { createEventDispatcher, onDestroy, tick } from 'svelte';
	import { goto } from '$app/navigation';
	import { Chart } from 'chart.js/auto';
	import { getRelativePosition } from 'chart.js/helpers';
	import type { ActiveElement, ChartEvent } from 'chart.js';
	import type { GroupByOption, RouterConfig } from '$lib/components/netflow/types.ts';
	import {
		IP_METRIC_OPTIONS,
		type IpGranularity,
		type IpMetricKey,
		type IpMetricOption,
		type IpStatsBucket,
		type IpStatsResponse
	} from '$lib/types/types';
	import {
		generateSlugFromLabel,
		parseClickedLabel,
		formatNumber,
		Y_AXIS_WIDTH
	} from './chart-utils';
	import { verticalCrosshairPlugin } from './crosshair-plugin';
	import { crosshairStore } from '$lib/stores/crosshair';

	const CHART_ID = 'ip';

	// Register the crosshair plugin once
	Chart.register(verticalCrosshairPlugin);
	import {
		dateStringToEpochPST,
		epochToPSTComponents,
		formatDateAsPSTDateString,
		getWeekdayName
	} from '$lib/utils/timezone';

	const DEFAULT_METRICS: IpMetricKey[] = ['saIpv4Count', 'daIpv4Count'];
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
		activeMetrics?: IpMetricKey[];
	}>();
	const today = new Date();
	const formatDate = (date: Date): string => formatDateAsPSTDateString(date);
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
		return dateStringToEpochPST(dateString, isEnd);
	}

	function formatBucketLabel(bucket: IpStatsBucket, granularity: IpGranularity): string {
		const pst = epochToPSTComponents(bucket.bucketStart);
		const year = pst.year;
		const month = `${pst.month}`.padStart(2, '0');
		const day = `${pst.day}`.padStart(2, '0');
		const hours = `${pst.hours}`.padStart(2, '0');
		const minutes = `${pst.minutes}`.padStart(2, '0');

		if (granularity === '1d') {
			return `${year}-${month}-${day}`;
		}

		return `${year}-${month}-${day} ${hours}:${minutes}`;
	}

	function formatTickLabel(
		bucketStart: number,
		granularity: IpGranularity,
		_index: number
	): string {
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

	function shouldHighlightTick(
		bucketStart: number,
		granularity: IpGranularity,
		index: number
	): boolean {
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
			crosshairStore.unregister(CHART_ID);
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
		if (!chart || !chart.data.labels) {
			return null;
		}
		const labels = chart.data.labels as string[];
		if (index < 0 || index >= labels.length) {
			return null;
		}
		return labels[index] ?? null;
	}

	function handleChartClick(event: ChartEvent, activeElements: ActiveElement[]) {
		if (!chart || !chart.data.labels) {
			return;
		}

		const groupBy = IP_TO_GROUP_BY[currentGranularity];
		if (!groupBy) {
			return;
		}

		const labels = chart.data.labels as string[];
		if (labels.length === 0) {
			return;
		}

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

		if (!label) {
			return;
		}

		const clickedDate = parseClickedLabel(label, groupBy);
		if (!(clickedDate instanceof Date) || Number.isNaN(clickedDate.getTime())) {
			console.warn('Unable to parse clicked label for drilldown', { label, groupBy });
			return;
		}
		const activeLabel = fallbackIndex !== null ? getLabelFromIndex(fallbackIndex) : null;

		if (groupBy === '5min') {
			const labelForSlug = activeLabel ?? label;
			const slug = generateSlugFromLabel(labelForSlug, '5min');
			if (slug) {
				goto(`/api/netflow/files/${slug}`);
			}
			return;
		}

		const nextGroupBy = GROUP_BY_TRANSITIONS[groupBy];
		if (!nextGroupBy) {
			return;
		}

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
		if (!canvas) {
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
					onClick: handleChartClick,
					responsive: true,
					maintainAspectRatio: false,
					interaction: { mode: 'index', intersect: false },
					plugins: {
						legend: { position: 'top' },
						verticalCrosshair: {
							enabled: true,
							line: {
								color: 'rgba(100, 100, 100, 0.8)',
								width: 1,
								dash: [3, 3]
							},
							tooltip: {
								enabled: true,
								delay: 500,
								backgroundColor: 'rgba(0, 0, 0, 0.85)',
								textColor: 'white',
								borderColor: 'rgba(100, 100, 100, 0.8)',
								borderWidth: 1,
								borderRadius: 4,
								padding: 8,
								fontSize: 12,
								fontFamily: 'system-ui, sans-serif'
							},
							sync: {
								onHover: (label: string | null) => crosshairStore.setHover(label, CHART_ID),
								getExternalLabel: () => crosshairStore.getExternalLabel(CHART_ID)
							}
						}
					} as Record<string, unknown>,
					scales: {
						x: {
							title: {
								display: true,
								text: `Time (${currentGranularity})`
							},
							ticks: {
								autoSkip: false,
								maxRotation: 45,
								minRotation: 45,
								callback: (_value, idx) =>
									formatTickLabel(
										bucketStarts[idx as number] ?? 0,
										currentGranularity,
										idx as number
									)
							},
							grid: {
								color: (ctx) =>
									shouldHighlightTick(bucketStarts[ctx.index] ?? 0, currentGranularity, ctx.index)
										? 'rgba(0,0,0,0.08)'
										: 'rgba(0,0,0,0.02)'
							}
						},
						y: {
							beginAtZero: true,
							afterFit(axis: { width: number }) {
								axis.width = Y_AXIS_WIDTH;
							},
							title: {
								display: true,
								text: 'Unique IPs'
							},
							ticks: {
								callback: (value: string | number) => formatNumber(Number(value))
							}
						}
					}
				}
			});
			// Register chart with crosshair store for synchronized crosshairs
			crosshairStore.register(CHART_ID, chart);
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
				y: {
					...chart.options.scales?.y,
					afterFit(axis: { width: number }) {
						axis.width = Y_AXIS_WIDTH;
					},
					title: { display: true, text: 'Unique IPs' },
					ticks: {
						callback: (value: string | number) => formatNumber(Number(value))
					}
				}
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
			// wait for the DOM to reconcile (canvas may re-mount) before painting
			void (async () => {
				await tick();
				renderChart();
			})();
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
	<div
		class="relative border-b p-4 select-none cursor-grab active:cursor-grabbing"
		draggable="true"
		data-drag-handle
	>
		<h3 class="text-lg font-semibold text-gray-900">Unique IP Counts</h3>
		<span class="pointer-events-none absolute inset-0 flex items-start justify-center pt-1 text-gray-400" aria-hidden="true">
			<span class="grid grid-cols-3 grid-rows-2 gap-[2px]">
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
				<span class="h-[2px] w-[2px] rounded-full bg-current"></span>
			</span>
		</span>
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
