<script lang="ts">
	import { onMount } from 'svelte';
	import Chart from 'chart.js/auto';

	interface StructureFunctionPoint {
		q: number;
		tauTilde: number;
		sd: number;
	}

	interface StructureFunctionData {
		slug: string;
		router: string;
		filename: string;
		structureFunction: StructureFunctionPoint[];
		metadata: {
			dataSource: string;
			uniqueIPCount?: number;
			pointCount: number;
			qRange: { min: number; max: number };
		};
	}

	let { data }: { data: StructureFunctionData } = $props();
	let chartCanvas: HTMLCanvasElement;
	let chart: Chart;

	onMount(() => {
		return () => {
			if (chart) {
				chart.destroy();
			}
		};
	});

	// Watch for data changes and create/update chart
	$effect(() => {
		console.log('StructureFunctionChart effect triggered', {
			hasCanvas: !!chartCanvas,
			hasData: !!data,
			hasStructureFunction: !!data?.structureFunction,
			dataLength: data?.structureFunction?.length || 0,
			router: data?.router
		});

		if (chartCanvas && data?.structureFunction?.length > 0) {
			if (chart) {
				chart.destroy();
			}
			createChart();
		}
	});

	function createChart() {
		const points = data.structureFunction;

		const chartData = {
			datasets: [
				{
					label: 'τ̃(q) - Structure Function',
					data: points.map((p) => ({ x: p.q, y: p.tauTilde })),
					borderColor: 'rgb(59, 130, 246)',
					backgroundColor: 'rgba(59, 130, 246, 0.1)',
					borderWidth: 2,
					pointRadius: 3,
					pointHoverRadius: 5,
					fill: false,
					tension: 0.1
				},
				{
					label: 'Standard Deviation',
					data: points.map((p) => ({ x: p.q, y: p.sd })),
					borderColor: 'rgb(239, 68, 68)',
					backgroundColor: 'rgba(239, 68, 68, 0.1)',
					borderWidth: 1,
					pointRadius: 2,
					pointHoverRadius: 4,
					fill: false,
					tension: 0.1,
					yAxisID: 'y1'
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
						title: {
							display: true,
							text: 'q (moment parameter)'
						}
					},
					y: {
						type: 'linear' as const,
						title: {
							display: true,
							text: 'τ̃(q) - Structure Function'
						},
						position: 'left' as const
					},
					y1: {
						type: 'linear' as const,
						title: {
							display: true,
							text: 'Standard Deviation'
						},
						position: 'right' as const,
						grid: {
							drawOnChartArea: false
						}
					}
				},
				plugins: {
					title: {
						display: true,
						text: `Multifractal Structure Function - ${data.router} - ${data.filename}`,
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
							title: (items: any[]) => `q = ${items[0]?.parsed?.x?.toFixed(3)}`,
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
			Data Source: {data.metadata.dataSource} | Points: {data.metadata.pointCount} | q Range: [{data.metadata.qRange.min.toFixed(
				1
			)}, {data.metadata.qRange.max.toFixed(1)}]
		</p>
		{#if data.metadata.uniqueIPCount}
			<p class="text-xs font-medium text-green-600">
				✓ Real NetFlow Data Analysis - {data.metadata.uniqueIPCount.toLocaleString()} unique IP addresses
				analyzed
			</p>
		{:else}
			<p class="text-xs text-amber-600">⚠ Using test data from MAAD sample set</p>
		{/if}
	</div>
	<div class="relative h-96 w-full">
		<canvas bind:this={chartCanvas}></canvas>
	</div>
</div>
