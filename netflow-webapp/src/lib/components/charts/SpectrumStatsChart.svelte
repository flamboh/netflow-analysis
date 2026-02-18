<script lang="ts">
	import DragGrip from '$lib/components/common/DragGrip.svelte';
	import { createEventDispatcher, onDestroy, tick } from 'svelte';
	import { goto } from '$app/navigation';
	import { Chart, registerables } from 'chart.js/auto';
	import { getRelativePosition } from 'chart.js/helpers';
	import type { ActiveElement, ChartEvent, TooltipItem } from 'chart.js';
	import type { GroupByOption } from '$lib/components/netflow/types.ts';
	import type {
		IpGranularity,
		SpectrumPoint,
		SpectrumStatsBucket,
		SpectrumStatsResponse
	} from '$lib/types/types';
	import {
		generateSlugFromLabel,
		parseClickedLabel,
		Y_AXIS_WIDTH,
		MIN_DRAG_PIXELS,
		groupByBucketDurationMs,
		chooseAdaptiveGranularity,
		createRangeDragState,
		getSelectionLabels,
		beginRangeDrag,
		updateRangeDrag,
		endRangeDrag,
		buildMirroredSelectionStyle
	} from './chart-utils';
	import {
		dateStringToEpochPST,
		epochToPSTComponents,
		formatDateAsPSTDateString,
		getWeekdayName
	} from '$lib/utils/timezone';
	import { verticalCrosshairPlugin } from './crosshair-plugin';
	import { crosshairStore } from '$lib/stores/crosshair';
	import { rangeSelectionStore, type RangeSelectionState } from '$lib/stores/rangeSelection';

	const CHART_ID = 'spectrum';

	Chart.register(...registerables, verticalCrosshairPlugin);

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

	const props = $props<{
		startDate?: string;
		endDate?: string;
		granularity?: IpGranularity;
		router?: string;
		addressType?: 'sa' | 'da';
		availableRouters?: string[];
	}>();

	const dispatch = createEventDispatcher<{
		dateChange: { startDate: string; endDate: string };
		groupByChange: { groupBy: GroupByOption };
		routerChange: { router: string };
		addressTypeChange: { addressType: 'sa' | 'da' };
	}>();

	const today = new Date();
	const formatDate = (date: Date): string => formatDateAsPSTDateString(date);

	let buckets = $state<SpectrumStatsBucket[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);
	let addressType = $state<'sa' | 'da'>(props.addressType ?? 'sa');
	let currentRouter = $state((props.router ?? '').trim());
	let bucketStarts: number[] = [];

	let chartCanvas = $state<HTMLCanvasElement | null>(null);
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let chart: Chart<'scatter', any[], any> | null = null;
	let rangeDrag = $state(createRangeDragState());
	let selectionLeft = $derived(Math.min(rangeDrag.dragStartX, rangeDrag.dragCurrentX));
	let selectionWidth = $derived(Math.abs(rangeDrag.dragStartX - rangeDrag.dragCurrentX));
	let mirroredRange = $state<RangeSelectionState | null>(null);

	$effect(() => {
		const unsubscribe = rangeSelectionStore.subscribe((value) => {
			mirroredRange = value;
		});
		return unsubscribe;
	});

	function toEpochSeconds(dateString: string, isEnd = false): number {
		return dateStringToEpochPST(dateString, isEnd);
	}

	function formatBucketLabel(bucketStart: number, granularity: IpGranularity): string {
		const pst = epochToPSTComponents(bucketStart);
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

	function getBucketStartForTickValue(value: unknown): number | null {
		if (typeof value !== 'number' || !Number.isFinite(value)) {
			return null;
		}
		const roundedIndex = Math.round(value);
		if (Math.abs(value - roundedIndex) > 1e-6) {
			return null;
		}
		if (roundedIndex < 0 || roundedIndex >= bucketStarts.length) {
			return null;
		}
		return bucketStarts[roundedIndex] ?? null;
	}

	// Color gradient function based on f value
	// Purple (low f) -> Blue -> Cyan -> Green -> Yellow (high f)
	function getColorForF(f: number, minF: number, maxF: number): string {
		if (maxF === minF) return 'hsl(180, 70%, 50%)';
		const normalized = (f - minF) / (maxF - minF);
		// HSL: hue 270=purple, 180=cyan, 120=green, 60=yellow
		const hue = 270 - normalized * 210; // 270 (purple) to 60 (yellow)
		return `hsl(${hue}, 70%, 50%)`;
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

	function handleRouterChange(router: string) {
		if (router === (props.router ?? '')) {
			return;
		}
		dispatch('routerChange', { router });
	}

	function handleAddressTypeChange(nextAddressType: 'sa' | 'da') {
		if (nextAddressType === addressType) {
			return;
		}
		addressType = nextAddressType;
		if (chart) {
			renderChart();
		}
		dispatch('addressTypeChange', { addressType: nextAddressType });
	}

	function publishRangeSelection(startIndex: number, endIndex: number) {
		const labels = getSelectionLabels(chart, startIndex, endIndex);
		if (!labels) return;
		rangeSelectionStore.set({ sourceChartId: CHART_ID, ...labels });
	}

	function applyRangeDrilldown(startIndex: number, endIndex: number) {
		if (!chart?.data.labels) return;
		const labels = chart.data.labels as string[];
		const from = Math.max(0, Math.min(labels.length - 1, Math.min(startIndex, endIndex)));
		const to = Math.max(0, Math.min(labels.length - 1, Math.max(startIndex, endIndex)));
		const startLabel = labels[from];
		const endLabel = labels[to];
		if (!startLabel || !endLabel) return;

		const groupBy = IP_TO_GROUP_BY[currentGranularity];
		if (!groupBy) return;

		const startDate = parseClickedLabel(startLabel, groupBy);
		const endBucketStart = parseClickedLabel(endLabel, groupBy);
		if (Number.isNaN(startDate.getTime()) || Number.isNaN(endBucketStart.getTime())) return;
		const endExclusive = new Date(endBucketStart.getTime() + groupByBucketDurationMs(groupBy));
		const selectedRangeMs = endExclusive.getTime() - startDate.getTime();
		const nextGroupBy = chooseAdaptiveGranularity(selectedRangeMs);
		emitDrilldown(nextGroupBy, startDate, endExclusive);
	}

	function handleRangeMouseDown(event: MouseEvent) {
		beginRangeDrag(rangeDrag, event, chartCanvas, chart, publishRangeSelection);
	}

	function handleRangeMouseMove(event: MouseEvent) {
		updateRangeDrag(rangeDrag, event, chartCanvas, chart, publishRangeSelection);
	}

	function finishRangeSelection() {
		endRangeDrag(rangeDrag, chart, applyRangeDrilldown);
		rangeSelectionStore.set(null);
	}
	let mirroredSelectionStyle = $derived(
		buildMirroredSelectionStyle(chart, mirroredRange, CHART_ID)
	);

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
		if (rangeDrag.suppressNextClick) {
			rangeDrag.suppressNextClick = false;
			return;
		}
		if (!chart || !chart.data.labels) {
			return;
		}

		const groupBy = IP_TO_GROUP_BY[currentGranularity];
		if (!groupBy) {
			return;
		}

		const labels = chart.data.labels as string[];
		if (labels.length === 0 || bucketStarts.length === 0) {
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

	interface DataPoint {
		x: number;
		y: number;
		f: number;
		timeLabel: string;
	}

	function buildDatasets(
		selectedBuckets: SpectrumStatsBucket[],
		bucketStarts: number[]
	): {
		data: DataPoint[];
		minF: number;
		maxF: number;
		minAlpha: number;
		maxAlpha: number;
	} {
		// Create a map for quick lookup: bucketStart -> spectrum points
		const bucketMap = new Map<number, SpectrumPoint[]>();
		selectedBuckets.forEach((bucket) => {
			const points = addressType === 'sa' ? bucket.spectrumSa : bucket.spectrumDa;
			if (points.length > 0) {
				bucketMap.set(bucket.bucketStart, points);
			}
		});

		// Find global min/max for f and alpha
		let minF = Infinity;
		let maxF = -Infinity;
		let minAlpha = Infinity;
		let maxAlpha = -Infinity;

		bucketMap.forEach((points) => {
			points.forEach((point) => {
				minF = Math.min(minF, point.f);
				maxF = Math.max(maxF, point.f);
				minAlpha = Math.min(minAlpha, point.alpha);
				maxAlpha = Math.max(maxAlpha, point.alpha);
			});
		});

		if (minF === Infinity) {
			return { data: [], minF: 0, maxF: 0, minAlpha: 0, maxAlpha: 0 };
		}

		// Build data points: each (time, alpha) has an f value for coloring
		const data: DataPoint[] = [];

		bucketStarts.forEach((bucketStart, timeIndex) => {
			const points = bucketMap.get(bucketStart);
			if (!points || points.length === 0) return;

			const timeLabel = formatBucketLabel(bucketStart, currentGranularity);

			points.forEach((point) => {
				data.push({
					x: timeIndex,
					y: point.alpha,
					f: point.f,
					timeLabel
				});
			});
		});

		return { data, minF, maxF, minAlpha, maxAlpha };
	}

	function renderChart() {
		const canvas = chartCanvas;
		if (!canvas) {
			return;
		}

		const selectedBuckets = currentRouter
			? buckets.filter((bucket) => bucket.router === currentRouter)
			: [];

		// Get unique time buckets, sorted
		bucketStarts = Array.from(new Set(selectedBuckets.map((b) => b.bucketStart))).sort(
			(a, b) => a - b
		);

		if (bucketStarts.length === 0) {
			if (chart) {
				chart.data.datasets = [];
				chart.update('none');
			}
			return;
		}

		const { data, minF, maxF, minAlpha, maxAlpha } = buildDatasets(selectedBuckets, bucketStarts);

		if (data.length === 0) {
			if (chart) {
				chart.data.datasets = [];
				chart.update('none');
			}
			return;
		}

		const labels = bucketStarts.map((bucketStart) =>
			formatBucketLabel(bucketStart, currentGranularity)
		);

		// Create scatter dataset with individual point colors based on f
		const pointColors = data.map((d) => getColorForF(d.f, minF, maxF));

		const granularity = currentGranularity;
		const alphaPadding = (maxAlpha - minAlpha) * 0.05;

		if (!chart) {
			chart = new Chart(canvas, {
				type: 'scatter',
				data: {
					labels,
					datasets: [
						{
							data: data.map((d) => ({ x: d.x, y: d.y })),
							backgroundColor: pointColors,
							borderColor: pointColors,
							pointRadius: 1,
							pointHoverRadius: 2
						}
					]
				},
				options: {
					onClick: handleChartClick,
					animation: false,
					responsive: true,
					maintainAspectRatio: false,
					interaction: {
						mode: 'nearest',
						intersect: true
					},
					plugins: {
						legend: {
							display: false
						},
						tooltip: {
							enabled: true,
							callbacks: {
								title: (items: TooltipItem<'scatter'>[]) => {
									const item = items[0];
									if (!item) return '';
									const dataIndex = item.dataIndex;
									return data[dataIndex]?.timeLabel ?? '';
								},
								label: (context: TooltipItem<'scatter'>) => {
									const dataIndex = context.dataIndex;
									const point = data[dataIndex];
									if (!point) return '';
									return [`alpha = ${point.y.toFixed(6)}`, `f = ${point.f.toFixed(6)}`];
								}
							}
						},
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
							type: 'linear',
							min: 0,
							max: bucketStarts.length - 1,
							title: {
								display: true,
								text: `Time (${granularity})`
							},
							ticks: {
								stepSize: 1,
								autoSkip: false,
								maxRotation: 45,
								minRotation: 45,
								callback: (value: unknown) => {
									const bucketStart = getBucketStartForTickValue(value);
									if (bucketStart === null) return '';
									const index = Math.round(value as number);
									return formatTickLabel(bucketStart, granularity, index);
								}
							},
							grid: {
								color: (ctx: { tick?: { value?: number } }) => {
									const tickValue = ctx.tick?.value;
									const bucketStart = getBucketStartForTickValue(tickValue);
									if (bucketStart === null || typeof tickValue !== 'number') {
										return 'rgba(0,0,0,0.02)';
									}
									const index = Math.round(tickValue);
									return shouldHighlightTick(bucketStart, granularity, index)
										? 'rgba(0,0,0,0.08)'
										: 'rgba(0,0,0,0.02)';
								}
							}
						},
						y: {
							type: 'linear',
							min: minAlpha - alphaPadding,
							max: maxAlpha + alphaPadding,
							afterFit(axis: { width: number }) {
								axis.width = Y_AXIS_WIDTH;
							},
							title: {
								display: true,
								text: 'alpha'
							}
						}
					}
				}
			});
			// Register chart with crosshair store for synchronized crosshairs
			crosshairStore.register(CHART_ID, chart);
		} else {
			chart.data.labels = labels;
			chart.data.datasets = [
				{
					data: data.map((d) => ({ x: d.x, y: d.y })),
					backgroundColor: pointColors,
					borderColor: pointColors,
					pointRadius: 1,
					pointHoverRadius: 2
				}
			];
			chart.options.scales = {
				x: {
					min: 0,
					max: bucketStarts.length - 1,
					title: { display: true, text: `Time (${granularity})` },
					ticks: {
						stepSize: 1,
						autoSkip: false,
						maxRotation: 45,
						minRotation: 45,
						callback: (value: unknown) => {
							const bucketStart = getBucketStartForTickValue(value);
							if (bucketStart === null) return '';
							const index = Math.round(value as number);
							return formatTickLabel(bucketStart, granularity, index);
						}
					},
					grid: {
						color: (ctx: { tick?: { value?: number } }) => {
							const tickValue = ctx.tick?.value;
							const bucketStart = getBucketStartForTickValue(tickValue);
							if (bucketStart === null || typeof tickValue !== 'number') {
								return 'rgba(0,0,0,0.02)';
							}
							const index = Math.round(tickValue);
							return shouldHighlightTick(bucketStart, granularity, index)
								? 'rgba(0,0,0,0.08)'
								: 'rgba(0,0,0,0.02)';
						}
					}
				} as never,
				y: {
					min: minAlpha - alphaPadding,
					max: maxAlpha + alphaPadding,
					afterFit(axis: { width: number }) {
						axis.width = Y_AXIS_WIDTH;
					},
					title: { display: true, text: 'alpha' }
				} as never
			};
			chart.options.onClick = handleChartClick;
			chart.update('none');
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
			const response = await fetch(`/api/netflow/spectrum-stats?${params.toString()}`);
			if (!response.ok) {
				const message = await response.text();
				throw new Error(message || 'Failed to load spectrum statistics');
			}
			const data = (await response.json()) as SpectrumStatsResponse;
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
			error = err instanceof Error ? err.message : 'Unexpected error loading spectrum statistics';
			buckets = [];
			loading = false;
			destroyChart();
		}
	}

	onDestroy(() => {
		if (mirroredRange?.sourceChartId === CHART_ID) {
			rangeSelectionStore.set(null);
		}
		destroyChart();
	});

	let currentGranularity = $state<IpGranularity>(props.granularity ?? '1h');

	$effect(() => {
		const availableRouters = (props.availableRouters ?? [])
			.map((router: string) => router.trim())
			.filter((router: string) => router.length > 0);
		const requestedRouter = props.router?.trim() ?? '';
		const nextRouter = availableRouters.includes(requestedRouter)
			? requestedRouter
			: (availableRouters[0] ?? '');
		const startDateProp = props.startDate;
		const endDateProp = props.endDate;
		const granularityProp = props.granularity;
		const nextAddressType = props.addressType ?? 'sa';

		if (nextAddressType !== addressType) {
			addressType = nextAddressType;
			if (chart) {
				renderChart();
			}
		}
		if (nextRouter !== currentRouter) {
			currentRouter = nextRouter;
			if (chart) {
				renderChart();
			}
		}

		const filters: FilterInputs = {
			startDate: startDateProp ?? '2025-01-01',
			endDate: endDateProp ?? formatDate(today),
			granularity: granularityProp ?? '1h',
			routers: availableRouters
		};

		currentGranularity = filters.granularity;

		if (filters.routers.length === 0) {
			error = 'Select at least one router to view spectrum statistics';
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
		class="relative cursor-grab border-b p-4 select-none active:cursor-grabbing"
		draggable="true"
		data-drag-handle
	>
		<h3 class="text-lg font-semibold text-gray-900">Spectrum</h3>
		<DragGrip />
	</div>
	<div class="p-4">
		<div class="mb-4 space-y-2">
			<div class="flex min-h-6 flex-wrap items-center gap-4">
				{#if (props.availableRouters ?? []).length === 0}
					{#each Array(4) as _, index (index)}
						<span class="inline-block h-4 w-24 animate-pulse rounded bg-gray-200" aria-hidden="true"
						></span>
					{/each}
				{:else}
					{#each props.availableRouters ?? [] as routerName (routerName)}
						<label class="flex cursor-pointer items-center gap-2">
							<input
								type="radio"
								name="spectrum-router-local"
								checked={props.router === routerName}
								onchange={() => handleRouterChange(routerName)}
								class="h-4 w-4 border-gray-300 text-blue-600 focus:ring-blue-500"
							/>
							<span class="text-sm text-gray-700">{routerName}</span>
						</label>
					{/each}
				{/if}
			</div>
			<div class="flex flex-wrap items-center gap-4">
				<label class="flex cursor-pointer items-center gap-2">
					<input
						type="radio"
						name="spectrum-address-type-local"
						checked={addressType === 'sa'}
						onchange={() => handleAddressTypeChange('sa')}
						class="h-4 w-4 border-gray-300 text-blue-600 focus:ring-blue-500"
					/>
					<span class="text-sm text-gray-700">Source IPv4</span>
				</label>
				<label class="flex cursor-pointer items-center gap-2">
					<input
						type="radio"
						name="spectrum-address-type-local"
						checked={addressType === 'da'}
						onchange={() => handleAddressTypeChange('da')}
						class="h-4 w-4 border-gray-300 text-blue-600 focus:ring-blue-500"
					/>
					<span class="text-sm text-gray-700">Destination IPv4</span>
				</label>
			</div>
		</div>

		<div
			class="relative h-[400px] min-h-[300px] resize-y overflow-auto rounded-md border border-gray-200 bg-white/60"
			role="presentation"
			onmousedown={handleRangeMouseDown}
			onmousemove={handleRangeMouseMove}
			onmouseup={finishRangeSelection}
			onmouseleave={finishRangeSelection}
		>
			{#if loading}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">Loading spectrum data...</div>
				</div>
			{:else if error}
				<div class="flex h-full items-center justify-center">
					<div class="text-red-500">{error}</div>
				</div>
			{:else if buckets.length === 0}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">No spectrum data for the selected window.</div>
				</div>
			{:else}
				<div class="h-full">
					<canvas bind:this={chartCanvas} aria-label="Spectrum chart"></canvas>
					{#if rangeDrag.isDraggingRange && selectionWidth >= MIN_DRAG_PIXELS}
						<div
							class="pointer-events-none absolute border border-gray-500/70 bg-gray-500/20"
							style={`left:${selectionLeft}px; width:${selectionWidth}px; top:${rangeDrag.selectionTop}px; height:${rangeDrag.selectionHeight}px;`}
						></div>
					{/if}
					{#if !rangeDrag.isDraggingRange && mirroredSelectionStyle !== null}
						<div
							class="pointer-events-none absolute border border-gray-500/70 bg-gray-500/20"
							style={mirroredSelectionStyle}
						></div>
					{/if}
				</div>
			{/if}
		</div>
	</div>
</div>
