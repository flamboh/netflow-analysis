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
		console.log('SpectrumChart effect triggered', {
			hasCanvas: !!chartCanvas,
			hasData: !!data,
			hasSpectrum: !!data?.spectrum,
			dataLength: data?.spectrum?.length || 0,
			router: data?.router
		});

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
					borderColor: 'rgb(147, 51, 234)',
					backgroundColor: 'rgba(147, 51, 234, 0.1)',
					borderWidth: 2,
					pointRadius: 3,
					pointHoverRadius: 5,
					pointBackgroundColor: 'rgb(147, 51, 234)',
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
							title: (items: any[]) => `alpha = ${items[0]?.parsed?.x?.toFixed(6)}`,
							label: (item: any) => {
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

		chart = new Chart(chartCanvas, config);
	}
</script>

<div class="w-full">
	<div class="mb-2 text-sm text-gray-600">
		<p>
			Data Source: {data.metadata.dataSource} | alpha Range: [{data.metadata.alphaRange.min.toFixed(
				3
			)}, {data.metadata.alphaRange.max.toFixed(3)}]
		</p>
		{#if data.metadata.uniqueIPCount && data.metadata.uniqueIPCount > 0}
			<p class="text-xs font-medium text-green-600">
				✓ Real NetFlow Data Analysis - {data.metadata.uniqueIPCount.toLocaleString()} unique IP addresses
				analyzed
			</p>
		{:else if data.metadata.uniqueIPCount !== -1}
			<p class="text-xs text-amber-600">⚠ Using test data from MAAD sample set</p>
		{/if}
	</div>
	<div class="relative h-96 w-full">
		<canvas bind:this={chartCanvas}></canvas>
	</div>
</div>
