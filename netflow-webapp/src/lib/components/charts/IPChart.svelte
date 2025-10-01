<script lang="ts">
	import { onDestroy, onMount, tick } from 'svelte';
	import { Chart } from 'chart.js/auto';
	import DateRangeFilter from '$lib/components/filters/DateRangeFilter.svelte';
	import RouterFilter from '$lib/components/filters/RouterFilter.svelte';
	import type { RouterConfig } from '$lib/components/netflow/types.ts';
	import {
		IP_GRANULARITIES,
		type IpChartState,
		type IpGranularity,
		type IpMetricKey,
		type IpStatsBucket,
		type IpStatsResponse
	} from '$lib/types/types';

	interface MetricOption {
		key: IpMetricKey;
		label: string;
		color: string;
	}

	const METRIC_OPTIONS: MetricOption[] = [
		{ key: 'saIpv4Count', label: 'Source IPv4', color: '#2563eb' },
		{ key: 'daIpv4Count', label: 'Destination IPv4', color: '#10b981' },
		{ key: 'saIpv6Count', label: 'Source IPv6', color: '#f59e0b' },
		{ key: 'daIpv6Count', label: 'Destination IPv6', color: '#ef4444' }
	];

	const today = new Date();
	const formatDate = (date: Date): string => new Date(date).toISOString().slice(0, 10);
	const defaultEndDate = formatDate(today);
	const defaultStartDate = '2025-01-01';

	let chartState = $state<IpChartState>({
		startDate: defaultStartDate,
		endDate: defaultEndDate,
		granularity: '1d',
		selectedRouters: [],
		activeMetrics: ['saIpv4Count', 'daIpv4Count']
	});

	let routerConfig = $state<RouterConfig>({});
	let availableGranularities = $state<IpGranularity[]>([...IP_GRANULARITIES]);
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

	function withAlpha(hex: string, alpha: number): string {
		const parsed = hex.replace('#', '');
		const bigint = parseInt(parsed, 16);
		// Handle shorthand hex colours
		if (parsed.length === 3) {
			const r = parseInt(parsed[0] + parsed[0], 16);
			const g = parseInt(parsed[1] + parsed[1], 16);
			const b = parseInt(parsed[2] + parsed[2], 16);
			return `rgba(${r}, ${g}, ${b}, ${alpha})`;
		}

		const r = (bigint >> 16) & 255;
		const g = (bigint >> 8) & 255;
		const b = bigint & 255;
		return `rgba(${r}, ${g}, ${b}, ${alpha})`;
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

		const activeMetrics = chartState.activeMetrics;
		const selectedBuckets = buckets;

		if (activeMetrics.length === 0 || selectedBuckets.length === 0) {
			if (chart) {
				chart.data.labels = [];
				chart.data.datasets = [];
				chart.update();
			}
			return;
		}

		const labels = selectedBuckets.map((bucket) =>
			formatBucketLabel(bucket, chartState.granularity)
		);
		const datasets = METRIC_OPTIONS.filter((option) => activeMetrics.includes(option.key)).map(
			(option) => ({
				label: option.label,
				data: selectedBuckets.map((bucket) => bucket[option.key]),
				borderColor: option.color,
				backgroundColor: withAlpha(option.color, 0.2),
				tension: 0.3,
				fill: false,
				pointRadius: 0,
				pointHoverRadius: 4
			})
		);

		if (!chart) {
			chart = new Chart(canvas, {
				type: 'line',
				data: { labels, datasets },
				options: {
					responsive: true,
					maintainAspectRatio: false,
					interaction: { mode: 'index', intersect: false },
					plugins: {
						legend: { position: 'top' },
						title: {
							display: true,
							text: 'Unique IP Counts'
						}
					},
					scales: {
						x: {
							title: {
								display: true,
								text: `Time (${chartState.granularity})`
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
					title: { display: true, text: `Time (${chartState.granularity})` }
				},
				y: {
					...chart.options.scales?.y,
					title: { display: true, text: 'Unique IPs' }
				}
			};
			chart.update();
		}
	}

	async function loadRouters() {
		try {
			const response = await fetch('/api/routers');
			if (!response.ok) {
				throw new Error(`Router request failed: ${response.status}`);
			}
			const routerList = (await response.json()) as string[];
			const initialConfig: RouterConfig = {};
			routerList.forEach((router) => {
				const trimmed = router.trim();
				if (trimmed.length > 0) {
					initialConfig[trimmed] = true;
				}
			});
			routerConfig = initialConfig;
			chartState = {
				...chartState,
				selectedRouters: Object.keys(initialConfig)
			};
			if (chartState.selectedRouters.length > 0) {
				await loadData();
			}
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load routers';
		}
	}

	async function loadData() {
		const selectedRouters = chartState.selectedRouters;
		if (selectedRouters.length === 0) {
			error = 'Select at least one router to view IP statistics';
			buckets = [];
			destroyChart();
			return;
		}

		loading = true;
		destroyChart();
		error = null;

		const params = new URLSearchParams({
			startDate: toEpochSeconds(chartState.startDate).toString(),
			endDate: toEpochSeconds(chartState.endDate, true).toString(),
			granularity: chartState.granularity,
			routers: selectedRouters.join(',')
		});

		try {
			const response = await fetch(`/api/ip/stats?${params.toString()}`);
			if (!response.ok) {
				const message = await response.text();
				throw new Error(message || 'Failed to load IP statistics');
			}
			const data = (await response.json()) as IpStatsResponse;
			availableGranularities = data.availableGranularities;
			buckets = data.buckets;
			loading = false;
			await tick();
			renderChart();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Unexpected error loading IP statistics';
			buckets = [];
			loading = false;
			destroyChart();
		}
	}

	function handleStartDateChange(date: string) {
		chartState = { ...chartState, startDate: date };
		loadData();
	}

	function handleEndDateChange(date: string) {
		chartState = { ...chartState, endDate: date };
		loadData();
	}

	function handleRouterChange(newRouterConfig: RouterConfig) {
		routerConfig = newRouterConfig;
		const nextSelected = Object.entries(newRouterConfig)
			.filter(([, isEnabled]) => isEnabled)
			.map(([name]) => name.trim())
			.filter((name) => name.length > 0);
		chartState = { ...chartState, selectedRouters: nextSelected };
		loadData();
	}

	function handleGranularityChange(event: Event) {
		const target = event.currentTarget as HTMLSelectElement;
		const value = target.value as IpGranularity;
		chartState = { ...chartState, granularity: value };
		loadData();
	}

	function handleMetricToggle(metric: IpMetricKey) {
		const isActive = chartState.activeMetrics.includes(metric);
		const nextMetrics = isActive
			? chartState.activeMetrics.filter((item) => item !== metric)
			: [...chartState.activeMetrics, metric];
		chartState = { ...chartState, activeMetrics: nextMetrics };
		renderChart();
	}

	onMount(async () => {
		await loadRouters();
	});

	onDestroy(() => {
		destroyChart();
	});
</script>

<div class="space-y-6">
	<div class="space-y-4 rounded-lg border bg-white p-4 shadow-sm">
		<div class="flex items-center justify-between">
			<h2 class="text-lg font-semibold text-gray-900">IP Statistics Filters</h2>
			<label class="text-sm text-gray-600">
				Granularity
				<select
					class="ml-2 rounded border border-gray-300 bg-white px-2 py-1 text-sm text-gray-700 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
					onchange={handleGranularityChange}
					value={chartState.granularity}
				>
					{#each availableGranularities as option (option)}
						<option value={option}>{option}</option>
					{/each}
				</select>
			</label>
		</div>

		<DateRangeFilter
			startDate={chartState.startDate}
			endDate={chartState.endDate}
			onStartDateChange={handleStartDateChange}
			onEndDateChange={handleEndDateChange}
		/>

		<RouterFilter routers={routerConfig} onRouterChange={handleRouterChange} />

		<div class="flex flex-wrap items-center gap-4">
			<span class="text-sm font-medium text-gray-700">Metrics:</span>
			{#each METRIC_OPTIONS as option (option.key)}
				<label class="flex cursor-pointer items-center gap-2">
					<input
						type="checkbox"
						checked={chartState.activeMetrics.includes(option.key)}
						onchange={() => handleMetricToggle(option.key)}
						class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
					/>
					<span class="text-sm text-gray-700">{option.label}</span>
				</label>
			{/each}
		</div>
	</div>

	<div class="rounded-lg border bg-white shadow-sm">
		<div class="border-b p-4">
			<h3 class="text-lg font-semibold text-gray-900">IP Activity Visualization</h3>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="flex h-[480px] items-center justify-center">
					<div class="text-gray-500">Loading IP data...</div>
				</div>
			{:else if error}
				<div class="flex h-[480px] items-center justify-center">
					<div class="text-red-500">{error}</div>
				</div>
			{:else if chartState.activeMetrics.length === 0}
				<div class="flex h-[480px] items-center justify-center">
					<div class="text-gray-500">Select at least one metric to display.</div>
				</div>
			{:else if buckets.length === 0}
				<div class="flex h-[480px] items-center justify-center">
					<div class="text-gray-500">No IP data for the selected window.</div>
				</div>
			{:else}
				<div class="h-[480px]">
					<canvas bind:this={chartCanvas} aria-label="IP statistics chart"></canvas>
				</div>
			{/if}
		</div>
	</div>
</div>
