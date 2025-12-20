<script lang="ts">
	import { createEventDispatcher, onDestroy, tick } from 'svelte';
	import { Chart, registerables } from 'chart.js/auto';
	import type { GroupByOption, RouterConfig } from '$lib/components/netflow/types.ts';
	import type {
		IpGranularity,
		StructureFunctionPoint,
		StructureStatsBucket,
		StructureStatsResponse
	} from '$lib/types/types';

	Chart.register(...registerables);

	const props = $props<{
		startDate?: string;
		endDate?: string;
		granularity?: IpGranularity;
		routers?: RouterConfig;
	}>();

	const dispatch = createEventDispatcher<{
		dateChange: { startDate: string; endDate: string };
		groupByChange: { groupBy: GroupByOption };
	}>();

	const today = new Date();
	const formatDate = (date: Date): string => new Date(date).toISOString().slice(0, 10);

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

	let buckets = $state<StructureStatsBucket[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);
	let addressType = $state<'sa' | 'da'>('sa');

	let chartCanvas = $state<HTMLCanvasElement | null>(null);
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let chart: Chart<'scatter', any[], any> | null = null;

	function toEpochSeconds(dateString: string, isEnd = false): number {
		const base = new Date(dateString);
		if (Number.isNaN(base.getTime())) {
			return 0;
		}
		const offset = isEnd ? 24 * 60 * 60 * 1000 : 0;
		return Math.floor((base.getTime() + offset) / 1000);
	}

	function formatBucketLabel(bucketStart: number, granularity: IpGranularity): string {
		const start = new Date(bucketStart * 1000);
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

	function formatTickLabel(
		bucketStart: number,
		granularity: IpGranularity,
		_index: number
	): string {
		const date = new Date(bucketStart * 1000);
		const day = date.getDate().toString().padStart(2, '0');
		const month = (date.getMonth() + 1).toString().padStart(2, '0');
		const hours = date.getHours();
		const minutes = date.getMinutes();

		if (granularity === '1d') {
			return date.getDay() === 1 ? `Mon ${month}/${day}` : '';
		}

		if (granularity === '1h') {
			if (hours === 0) {
				const weekday = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][date.getDay()];
				const m = date.getMonth() + 1;
				const d = date.getDate();
				return `${weekday} ${m}/${d}`;
			}
			return '';
		}

		if (granularity === '30m') {
			if (minutes === 0 && (hours === 0 || hours === 12)) {
				const weekday = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][date.getDay()];
				const m = date.getMonth() + 1;
				const d = date.getDate();
				return `${weekday} ${m}/${d} ${hours.toString().padStart(2, '0')}:00`;
			}
			return '';
		}

		if (granularity === '5m') {
			if (minutes === 0) {
				const weekday = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][date.getDay()];
				const m = date.getMonth() + 1;
				const d = date.getDate();
				return `${weekday} ${m}/${d} ${hours.toString().padStart(2, '0')}:00`;
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
		const date = new Date(bucketStart * 1000);
		const hours = date.getHours();
		const minutes = date.getMinutes();

		if (granularity === '1d') {
			return date.getDay() === 1;
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

	// Color gradient function based on tau value
	// Purple (low tau) -> Blue -> Cyan -> Green -> Yellow (high tau)
	function getColorForTau(tau: number, minTau: number, maxTau: number): string {
		if (maxTau === minTau) return 'hsl(180, 70%, 50%)';
		const normalized = (tau - minTau) / (maxTau - minTau);
		// HSL: hue 270=purple, 180=cyan, 120=green, 60=yellow
		const hue = 270 - normalized * 210; // 270 (purple) to 60 (yellow)
		return `hsl(${hue}, 70%, 50%)`;
	}

	function destroyChart() {
		if (chart) {
			chart.destroy();
			chart = null;
		}
	}

	interface DataPoint {
		x: number;
		y: number;
		tau: number;
		timeLabel: string;
	}

	function buildDatasets(bucketStarts: number[]): {
		data: DataPoint[];
		minTau: number;
		maxTau: number;
		minQ: number;
		maxQ: number;
	} {
		const selectedBuckets = buckets;

		// Create a map for quick lookup: bucketStart -> structure function points
		const bucketMap = new Map<number, StructureFunctionPoint[]>();
		selectedBuckets.forEach((bucket) => {
			const points = addressType === 'sa' ? bucket.structureSa : bucket.structureDa;
			if (points.length > 0) {
				bucketMap.set(bucket.bucketStart, points);
			}
		});

		// Find global min/max for tau and q
		let minTau = Infinity;
		let maxTau = -Infinity;
		let minQ = Infinity;
		let maxQ = -Infinity;

		bucketMap.forEach((points) => {
			points.forEach((point) => {
				minTau = Math.min(minTau, point.tau);
				maxTau = Math.max(maxTau, point.tau);
				minQ = Math.min(minQ, point.q);
				maxQ = Math.max(maxQ, point.q);
			});
		});

		if (minTau === Infinity) {
			return { data: [], minTau: 0, maxTau: 0, minQ: 0, maxQ: 0 };
		}

		// Build data points: each (time, q) has a tau value for coloring
		const data: DataPoint[] = [];

		bucketStarts.forEach((bucketStart, timeIndex) => {
			const points = bucketMap.get(bucketStart);
			if (!points || points.length === 0) return;

			const date = new Date(bucketStart * 1000);
			const timeLabel = `${date.getMonth() + 1}/${date.getDate()} ${date
				.getHours()
				.toString()
				.padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;

			points.forEach((point) => {
				data.push({
					x: timeIndex,
					y: point.q,
					tau: point.tau,
					timeLabel
				});
			});
		});

		return { data, minTau, maxTau, minQ, maxQ };
	}

	function renderChart() {
		const canvas = chartCanvas;
		if (!canvas) {
			return;
		}

		// Get unique time buckets, sorted
		const bucketStarts = Array.from(new Set(buckets.map((b) => b.bucketStart))).sort(
			(a, b) => a - b
		);

		if (bucketStarts.length === 0) {
			if (chart) {
				chart.data.datasets = [];
				chart.update('none');
			}
			return;
		}

		const { data, minTau, maxTau, minQ, maxQ } = buildDatasets(bucketStarts);

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

		// Create scatter dataset with individual point colors based on tau
		const pointColors = data.map((d) => getColorForTau(d.tau, minTau, maxTau));

		const granularity = currentGranularity;
		const qPadding = (maxQ - minQ) * 0.05;

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
								title: (items) => {
									const item = items[0];
									if (!item) return '';
									const dataIndex = item.dataIndex;
									return data[dataIndex]?.timeLabel ?? '';
								},
								label: (context) => {
									const dataIndex = context.dataIndex;
									const point = data[dataIndex];
									if (!point) return '';
									return [`q = ${point.y.toFixed(2)}`, `τ = ${point.tau.toFixed(4)}`];
								}
							}
						}
					},
					scales: {
						x: {
							type: 'linear',
							min: -0.5,
							max: bucketStarts.length - 0.5,
							title: {
								display: true,
								text: `Time (${granularity})`
							},
							ticks: {
								stepSize: 1,
								callback: (_value: unknown, idx: number) => {
									if (idx >= bucketStarts.length) return '';
									return formatTickLabel(bucketStarts[idx] ?? 0, granularity, idx);
								}
							},
							grid: {
								color: (ctx: { index: number }) =>
									shouldHighlightTick(bucketStarts[ctx.index] ?? 0, granularity, ctx.index)
										? 'rgba(0,0,0,0.08)'
										: 'rgba(0,0,0,0.02)'
							}
						},
						y: {
							type: 'linear',
							min: minQ - qPadding,
							max: maxQ + qPadding,
							title: {
								display: true,
								text: 'q'
							}
						}
					}
				}
			});
		} else {
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
					min: -0.5,
					max: bucketStarts.length - 0.5,
					title: { display: true, text: `Time (${granularity})` },
					ticks: {
						stepSize: 1,
						callback: (_value: unknown, idx: number) => {
							if (idx >= bucketStarts.length) return '';
							return formatTickLabel(bucketStarts[idx] ?? 0, granularity, idx);
						}
					},
					grid: {
						color: (ctx: { index: number }) =>
							shouldHighlightTick(bucketStarts[ctx.index] ?? 0, granularity, ctx.index)
								? 'rgba(0,0,0,0.08)'
								: 'rgba(0,0,0,0.02)'
					}
				} as never,
				y: {
					min: minQ - qPadding,
					max: maxQ + qPadding,
					title: { display: true, text: 'q' }
				} as never
			};
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
			const response = await fetch(`/api/netflow/structure-stats?${params.toString()}`);
			if (!response.ok) {
				const message = await response.text();
				throw new Error(message || 'Failed to load structure statistics');
			}
			const data = (await response.json()) as StructureStatsResponse;
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
			error = err instanceof Error ? err.message : 'Unexpected error loading structure statistics';
			buckets = [];
			loading = false;
			destroyChart();
		}
	}

	onDestroy(() => {
		destroyChart();
	});

	// Compute tau range for color scale legend
	const tauRange = $derived.by(() => {
		if (buckets.length === 0) return { min: 0, max: 0 };
		let minTau = Infinity;
		let maxTau = -Infinity;
		buckets.forEach((bucket) => {
			const points = addressType === 'sa' ? bucket.structureSa : bucket.structureDa;
			points.forEach((point) => {
				minTau = Math.min(minTau, point.tau);
				maxTau = Math.max(maxTau, point.tau);
			});
		});
		if (minTau === Infinity) return { min: 0, max: 0 };
		return { min: minTau, max: maxTau };
	});

	let currentGranularity = $state<IpGranularity>(props.granularity ?? '1h');

	$effect(() => {
		// Access props directly to ensure Svelte tracks them as dependencies
		const routerConfig = props.routers;
		const startDateProp = props.startDate;
		const endDateProp = props.endDate;
		const granularityProp = props.granularity;

		if (!routerConfig || Object.keys(routerConfig).length === 0) {
			return;
		}

		const filters: FilterInputs = {
			startDate: startDateProp ?? '2025-01-01',
			endDate: endDateProp ?? formatDate(today),
			granularity: granularityProp ?? '1h',
			routers: deriveSelectedRouters(routerConfig)
		};

		currentGranularity = filters.granularity;

		if (filters.routers.length === 0) {
			error = 'Select at least one router to view structure statistics';
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

	// Re-render when addressType changes
	$effect(() => {
		if (buckets.length > 0 && chartCanvas) {
			renderChart();
		}
	});
</script>

<div class="rounded-lg border bg-white shadow-sm">
	<div class="border-b p-4">
		<div class="flex items-center justify-between">
			<h3 class="text-lg font-semibold text-gray-900">Structure Function</h3>
			<div class="flex items-center gap-2">
				<button
					type="button"
					class="rounded px-3 py-1 text-sm font-medium transition-colors {addressType === 'sa'
						? 'bg-blue-600 text-white'
						: 'bg-gray-200 text-gray-700 hover:bg-gray-300'}"
					onclick={() => {
						addressType = 'sa';
					}}
				>
					Source Addresses
				</button>
				<button
					type="button"
					class="rounded px-3 py-1 text-sm font-medium transition-colors {addressType === 'da'
						? 'bg-blue-600 text-white'
						: 'bg-gray-200 text-gray-700 hover:bg-gray-300'}"
					onclick={() => {
						addressType = 'da';
					}}
				>
					Destination Addresses
				</button>
			</div>
		</div>
		{#if buckets.length > 0 && tauRange.max > tauRange.min}
			<div class="mt-2 flex items-center gap-2 text-xs text-gray-600">
				<span>τ = {tauRange.min.toFixed(2)}</span>
				<div class="flex h-4 w-32 items-center rounded border border-gray-300">
					<div
						class="h-full w-full rounded"
						style="background: linear-gradient(to right, hsl(270, 70%, 50%), hsl(60, 70%, 50%))"
					></div>
				</div>
				<span>τ = {tauRange.max.toFixed(2)}</span>
			</div>
		{/if}
	</div>
	<div class="p-4">
		<div
			class="h-[400px] min-h-[300px] resize-y overflow-auto rounded-md border border-gray-200 bg-white/60"
		>
			{#if loading}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">Loading structure data...</div>
				</div>
			{:else if error}
				<div class="flex h-full items-center justify-center">
					<div class="text-red-500">{error}</div>
				</div>
			{:else if buckets.length === 0}
				<div class="flex h-full items-center justify-center">
					<div class="text-gray-500">No structure data for the selected window.</div>
				</div>
			{:else}
				<div class="h-full">
					<canvas bind:this={chartCanvas} aria-label="Structure function chart"></canvas>
				</div>
			{/if}
		</div>
	</div>
</div>
