<script lang="ts">
	import { onMount } from 'svelte';
	import Chart from 'chart.js/auto';
	import annotationPlugin from 'chartjs-plugin-annotation';

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

		// Generate error bar annotations for each data point
		const errorBarAnnotations: any = {};

		points.forEach((point, index) => {
			const capWidth = 0.02; // Width of error bar caps

			// Vertical line (main error bar)
			errorBarAnnotations[`errorBar_${index}`] = {
				type: 'line',
				xMin: point.q,
				xMax: point.q,
				yMin: point.tauTilde - point.sd,
				yMax: point.tauTilde + point.sd,
				borderColor: 'rgba(128, 128, 128, 0.7)',
				borderWidth: 1.5
			};

			// Top cap (horizontal line at top of error bar)
			errorBarAnnotations[`errorBarTop_${index}`] = {
				type: 'line',
				xMin: point.q - capWidth,
				xMax: point.q + capWidth,
				yMin: point.tauTilde + point.sd,
				yMax: point.tauTilde + point.sd,
				borderColor: 'rgba(128, 128, 128, 0.7)',
				borderWidth: 1.5
			};

			// Bottom cap (horizontal line at bottom of error bar)
			errorBarAnnotations[`errorBarBottom_${index}`] = {
				type: 'line',
				xMin: point.q - capWidth,
				xMax: point.q + capWidth,
				yMin: point.tauTilde - point.sd,
				yMax: point.tauTilde - point.sd,
				borderColor: 'rgba(128, 128, 128, 0.7)',
				borderWidth: 1.5
			};
		});

		const chartData = {
			datasets: [
				{
					label: 'τ̃(q) - Structure Function',
					data: points.map((p) => ({ x: p.q, y: p.tauTilde })),
					borderColor: 'rgb(59, 130, 246)',
					backgroundColor: 'rgba(59, 130, 246, 0.1)',
					borderWidth: 2,
					pointRadius: 0,
					pointHoverRadius: 0,
					pointBackgroundColor: 'rgb(59, 130, 246)',
					pointBorderColor: 'white',
					pointBorderWidth: 1,
					fill: false,
					tension: 0.1
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
						min: -2.1,
						max: 4.1,
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
								const pointIndex = item.dataIndex;
								const point = points[pointIndex];
								return [
									`${item.dataset.label}: ${value}`,
									`Standard Deviation: ±${point.sd.toFixed(6)}`
								];
							}
						}
					},
					annotation: {
						annotations: errorBarAnnotations
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
			Data Source: {data.metadata.dataSource} | q Range: [{data.metadata.qRange.min.toFixed(1)}, {data.metadata.qRange.max.toFixed(
				1
			)}]
		</p>
		{#if data.metadata.uniqueIPCount && data.metadata.uniqueIPCount > 0}
			<p class="text-xs font-medium text-green-600">
				✓ Real NetFlow Data Analysis - {data.metadata.uniqueIPCount.toLocaleString()} unique IP addresses
				analyzed
			</p>
			<!-- {:else if data.metadata.uniqueIPCount === -1}
			<p class="text-xs font-medium text-green-600">
				✓ Real NetFlow Data Analysis - IPv4 source addresses processed directly
			</p> -->
		{:else if data.metadata.uniqueIPCount !== -1}
			<p class="text-xs text-amber-600">⚠ Using test data from MAAD sample set</p>
		{/if}
	</div>
	<div class="relative h-96 w-full">
		<canvas bind:this={chartCanvas}></canvas>
	</div>
</div>
