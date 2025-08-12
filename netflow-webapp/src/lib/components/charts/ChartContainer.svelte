<script lang="ts">
	import { onMount } from 'svelte';
	import { Chart } from 'chart.js/auto';
	import { getRelativePosition } from 'chart.js/helpers';
	import { goto } from '$app/navigation';
	import {
		formatLabels,
		getXAxisTitle,
		parseClickedLabel,
		generateSlugFromLabel
	} from './chart-utils';
	import type {
		NetflowDataPoint,
		GroupByOption,
		ChartTypeOption,
		DataOption,
		ClickedElement,
		ChartConfig
	} from '$lib/components/netflow/types.ts';

	interface Props {
		results: NetflowDataPoint[];
		groupBy: GroupByOption;
		chartType: ChartTypeOption;
		dataOptions: DataOption[];
		onDrillDown?: (newGroupBy: GroupByOption, newStartDate: string, newEndDate: string) => void;
		onNavigateToFile?: (slug: string) => void;
	}

	let { results, groupBy, chartType, dataOptions, onDrillDown, onNavigateToFile }: Props = $props();

	let chartCanvas: HTMLCanvasElement;
	let chart: Chart;

	function getClickedElement(activeElements: any[]): ClickedElement | null {
		if (activeElements.length > 0) {
			const element = activeElements[0];
			const datasetIndex = element.datasetIndex;
			const index = element.index;
			const dataset = chart.data.datasets[datasetIndex];
			const label = chart.data.labels?.[index] as string;
			const value = dataset.data[index];
			return { dataset, label, value, datasetIndex, index };
		}
		return null;
	}

	function handleChartClick(e: any, activeElements: any[]) {
		const clickedElement = getClickedElement(activeElements);
		if (!clickedElement) {
			const canvasPosition = getRelativePosition(e, chart);
			const dataX = chart.scales.x.getValueForPixel(canvasPosition.x);
			const dataY = chart.scales.y.getValueForPixel(canvasPosition.y);
			console.log('Clicked coordinates:', { x: dataX, y: dataY });
			return;
		}

		console.log('Clicked point:', {
			dataset: clickedElement.dataset.label,
			label: clickedElement.label,
			value: clickedElement.value,
			datasetIndex: clickedElement.datasetIndex,
			index: clickedElement.index
		});

		// Handle drill-down logic
		if (groupBy === 'date') {
			const date = parseClickedLabel(clickedElement.label, groupBy);
			const startOfMonth = new Date(date.getTime() - 15 * 24 * 60 * 60 * 1000);
			const endOfMonth = new Date(date.getTime() + 16 * 24 * 60 * 60 * 1000);
			onDrillDown?.(
				'hour',
				startOfMonth.toISOString().slice(0, 10),
				endOfMonth.toISOString().slice(0, 10)
			);
		} else if (groupBy === 'hour') {
			const date = parseClickedLabel(clickedElement.label, groupBy);
			const startOfWeek = new Date(date.getTime() - 3 * 24 * 60 * 60 * 1000);
			const endOfWeek = new Date(date.getTime() + 4 * 24 * 60 * 60 * 1000);
			onDrillDown?.(
				'30min',
				startOfWeek.toISOString().slice(0, 10),
				endOfWeek.toISOString().slice(0, 10)
			);
		} else if (groupBy === '30min') {
			const date = parseClickedLabel(clickedElement.label, groupBy);
			const endDate = new Date(date.getTime() + 24 * 60 * 60 * 1000);
			onDrillDown?.('5min', date.toISOString().slice(0, 10), endDate.toISOString().slice(0, 10));
		} else if (groupBy === '5min') {
			const slug = generateSlugFromLabel(clickedElement.label, groupBy);
			if (onNavigateToFile) {
				onNavigateToFile(slug);
			} else {
				goto(`/nfcapd/${slug}`);
			}
		}
	}

	function createChartConfig(): ChartConfig {
		const labels = formatLabels(results, groupBy);
		const xAxisTitle = getXAxisTitle(groupBy);

		// Use manual chart type selection - matches original logic
		const isStackedChart = chartType === 'stacked';

		// Original predefined colors
		const predefinedColors = [
			'rgb(75, 192, 192)',
			'rgb(255, 99, 132)',
			'rgb(54, 162, 235)',
			'rgb(255, 206, 86)',
			'rgb(153, 102, 255)',
			'rgb(255, 159, 64)',
			'rgb(255, 99, 71)',
			'rgb(0, 206, 209)',
			'rgb(60, 179, 113)',
			'rgb(218, 112, 214)',
			'rgb(255, 215, 0)',
			'rgb(128, 0, 128)',
			'rgb(0, 128, 128)',
			'rgb(255, 0, 255)',
			'rgb(0, 255, 0)',
			'rgb(128, 128, 0)',
			'rgb(0, 0, 128)',
			'rgb(255, 140, 0)',
			'rgb(34, 139, 34)',
			'rgb(139, 0, 0)'
		];

		// Parse data from results - matches original parsing logic
		const datasets: any[] = [];
		let colorIndex = 0;

		for (const option of dataOptions) {
			if (option.checked) {
				// Original parsing: split by newline, get line at option.index + 1, split by space, get element [1]
				const data = results.map((item) =>
					parseInt(item.data.split('\n')[option.index + 1].split(' ')[1])
				);
				const color = predefinedColors[colorIndex % predefinedColors.length];
				colorIndex++;

				datasets.push({
					label: option.label,
					data: data,
					borderColor: color,
					backgroundColor: isStackedChart
						? color.replace('rgb', 'rgba').replace(')', ', 0.6)')
						: color,
					fill: isStackedChart ? 'origin' : false,
					tension: 0.1,
					radius: 0,
					hitRadius: 2,
					hoverRadius: 5
				});
			}
		}

		// Check if all selected metrics are bytes
		const selectedOptions = dataOptions.filter(o => o.checked);
		const allAreBytesMetrics = selectedOptions.every(o => o.label.includes('Bytes'));

		// Original scales configuration
		const scales: any = {
			x: {
				title: {
					display: true,
					text: xAxisTitle
				}
			},
			y: isStackedChart
				? {
						display: true,
						type: 'linear',
						stacked: true,
						beginAtZero: true,
						title: {
							display: true,
							text: 'Value'
						},
						ticks: {
							callback: function (value: any) {
								const num = Number(value);
								
								// Use binary units for bytes, decimal for others
								if (allAreBytesMetrics) {
									if (num >= Math.pow(1024, 5)) return (num / Math.pow(1024, 5)).toFixed(1) + 'PB';
									if (num >= Math.pow(1024, 4)) return (num / Math.pow(1024, 4)).toFixed(1) + 'TB';
									if (num >= Math.pow(1024, 3)) return (num / Math.pow(1024, 3)).toFixed(1) + 'GB';
									if (num >= Math.pow(1024, 2)) return (num / Math.pow(1024, 2)).toFixed(1) + 'MB';
									if (num >= 1024) return (num / 1024).toFixed(1) + 'KB';
									return num.toString() + ' bytes';
								} else {
									if (num >= 1e15) return (num / 1e15).toFixed(1) + 'Q';
									if (num >= 1e12) return (num / 1e12).toFixed(1) + 'T';
									if (num >= 1e9) return (num / 1e9).toFixed(1) + 'B';
									if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
									if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
									return num.toString();
								}
							}
						}
					}
				: {
						display: true,
						type: 'logarithmic',
						beginAtZero: true,
						min: 1,
						title: {
							display: true,
							text: 'Value (Log Scale)'
						},
						ticks: {
							callback: function (value: any) {
								const num = Number(value);
								if (num >= 1e15) return (num / 1e15).toFixed(1) + 'Q';
								if (num >= 1e12) return (num / 1e12).toFixed(1) + 'T';
								if (num >= 1e9) return (num / 1e9).toFixed(1) + 'B';
								if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
								if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
								return num.toString();
							}
						}
					}
		};

		return {
			type: 'line',
			data: {
				labels,
				datasets
			},
			options: {
				onClick: handleChartClick,
				responsive: true,
				scales: scales,
				plugins: {
					legend: {
						display: true,
						position: 'top' as const
					}
				}
			}
		};
	}

	onMount(() => {
		// Initialize empty chart (matches original)
		chart = new Chart(chartCanvas, {
			type: 'line',
			data: {
				labels: [],
				datasets: [] // Start with no datasets
			},
			options: {
				onClick: handleChartClick,
				responsive: true,
				scales: {
					x: {
						title: {
							display: true,
							text: 'Date'
						}
					}
				}
			}
		});

		return () => {
			if (chart) {
				chart.destroy();
			}
		};
	});

	// Update chart when props change (matches original loadData behavior)
	$effect(() => {
		if (chart && results.length > 0) {
			const config = createChartConfig();
			chart.data = config.data;
			chart.options = config.options;
			chart.update();
		}
	});
</script>

<div class="chart-container">
	<canvas bind:this={chartCanvas} class="h-[600px] w-full"></canvas>
</div>

<style>
	.chart-container {
		position: relative;
		height: 600px;
		width: 100%;
	}
</style>
