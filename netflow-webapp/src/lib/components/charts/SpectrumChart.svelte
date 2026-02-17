<script lang="ts">
	import { onMount } from 'svelte';
	import Chart from 'chart.js/auto';
	import annotationPlugin from 'chartjs-plugin-annotation';
	import type { SpectrumData } from '$lib/types/types';

	let { data }: { data: SpectrumData } = $props();
	let chartCanvas: HTMLCanvasElement;
	let chart: Chart;

	onMount(() => {
		// Register the annotation plugin
		Chart.register(annotationPlugin);

		return () => {
			if (chart) {
				chart.destroy();
			}
		};
	});

	// Watch for data changes and create/update chart
	$effect(() => {
		if (chartCanvas && data?.spectrum?.length > 0) {
			if (chart) {
				chart.destroy();
			}
			createChart();
		}
	});

	function createChart() {
		const points = data.spectrum;

		// Calculate range for y=x reference line
		const alphaValues = points.map((p) => p.alpha);
		const minAlpha = Math.min(...alphaValues);
		const maxAlpha = Math.max(...alphaValues);

		const chartData = {
			datasets: [
				{
					label: 'f(alpha)',
					data: points.map((p) => ({ x: p.alpha, y: p.f })),
					borderColor: 'rgb(14, 116, 144)',
					backgroundColor: 'rgba(14, 116, 144, 0.1)',
					borderWidth: 2,
					pointRadius: 3,
					pointHoverRadius: 5,
					pointBackgroundColor: 'rgb(14, 116, 144)',
					pointBorderColor: 'white',
					pointBorderWidth: 1,
					fill: false,
					tension: 0.3
				},
				{
					label: 'y = x (reference)',
					data: [
						{
							x: minAlpha - 0.02 * (maxAlpha - minAlpha),
							y: minAlpha - 0.02 * (maxAlpha - minAlpha)
						},
						{
							x: maxAlpha + 0.02 * (maxAlpha - minAlpha),
							y: maxAlpha + 0.02 * (maxAlpha - minAlpha)
						}
					],
					borderColor: 'rgba(128, 128, 128, 0.5)',
					borderWidth: 1,
					borderDash: [5, 5],
					pointRadius: 0,
					pointHoverRadius: 0,
					fill: false,
					tension: 0
				}
			]
		};

		const config = {
			type: 'line' as const,
			data: chartData,
			options: {
				responsive: true,
				maintainAspectRatio: false,
				scales: {
					x: {
						type: 'linear' as const,
						min: minAlpha - 0.02 * (maxAlpha - minAlpha),
						max: maxAlpha + 0.02 * (maxAlpha - minAlpha),
						title: {
							display: true,
							text: 'alpha'
						}
					},
					y: {
						type: 'linear' as const,
						title: {
							display: true,
							text: 'f(alpha)'
						},
						position: 'left' as const
					}
				},
				plugins: {
					title: {
						display: true,
						text: `Spectrum - ${data.router} - ${data.filename}`,
						font: {
							size: 16
						}
					},
					legend: {
						display: true,
						position: 'top' as const
					},
					tooltip: {
						mode: 'index' as const,
						intersect: false,
						callbacks: {
							title: (items: { parsed: { x: number } }[]) =>
								`alpha = ${items[0]?.parsed?.x?.toFixed(6)}`,
							label: (item: {
								dataset: { label: string };
								parsed: { y: number };
								dataIndex: number;
							}) => {
								const value = item.parsed.y.toFixed(6);
								return `${item.dataset.label}: ${value}`;
							}
						}
					}
				},
				interaction: {
					mode: 'index' as const,
					intersect: false
				}
			}
		};

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		chart = new Chart(chartCanvas, config as any);
	}
</script>

<div class="w-full">
	<div class="mb-2 rounded-lg border border-slate-200/70 bg-slate-50/65 p-3 text-sm text-slate-600">
		<p>
			Data Source: {data.metadata.dataSource} | alpha Range: [{data.metadata.alphaRange.min.toFixed(
				3
			)}, {data.metadata.alphaRange.max.toFixed(3)}]
		</p>
		{#if data.metadata.uniqueIPCount && data.metadata.uniqueIPCount > 0}
			<p class="text-xs font-medium text-emerald-700">
				Real NetFlow data analysis ({data.metadata.uniqueIPCount.toLocaleString()} unique IP addresses)
			</p>
		{:else if data.metadata.uniqueIPCount !== -1}
			<p class="text-xs text-amber-700">Using test data from MAAD sample set</p>
		{/if}
	</div>
	<div class="relative h-96 w-full">
		<canvas bind:this={chartCanvas}></canvas>
	</div>
</div>
