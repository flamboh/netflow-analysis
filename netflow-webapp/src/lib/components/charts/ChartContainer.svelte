<script lang="ts">
	import { onMount } from 'svelte';
	import { Chart } from 'chart.js/auto';
	import { getRelativePosition } from 'chart.js/helpers';
	import { goto } from '$app/navigation';
	import { formatLabels, getXAxisTitle, formatNumber, generateColors, parseClickedLabel, generateSlugFromLabel } from './chart-utils.ts';
	import type { NetflowDataPoint, GroupByOption, ChartTypeOption, ClickedElement, ChartConfig } from '$lib/components/netflow/types.ts';

	interface Props {
		results: NetflowDataPoint[];
		groupBy: GroupByOption;
		chartType: ChartTypeOption;
		onDrillDown?: (newGroupBy: GroupByOption, newStartDate: string, newEndDate: string) => void;
		onNavigateToFile?: (slug: string) => void;
	}

	let { results, groupBy, chartType, onDrillDown, onNavigateToFile }: Props = $props();

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
			onDrillDown?.('hour', startOfMonth.toISOString().slice(0, 10), endOfMonth.toISOString().slice(0, 10));
		} else if (groupBy === 'hour') {
			const date = parseClickedLabel(clickedElement.label, groupBy);
			const startOfWeek = new Date(date.getTime() - 3 * 24 * 60 * 60 * 1000);
			const endOfWeek = new Date(date.getTime() + 4 * 24 * 60 * 60 * 1000);
			onDrillDown?.('30min', startOfWeek.toISOString().slice(0, 10), endOfWeek.toISOString().slice(0, 10));
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

		// Parse data from results
		const datasets: any[] = [];
		if (results.length > 0) {
			// Decode binary data to extract enabled metrics
			const sampleData = results[0].data;
			const binaryLength = sampleData.length;
			const colors = generateColors(binaryLength);

			const metricLabels = [
				'Flows', 'Flows TCP', 'Flows UDP', 'Flows ICMP', 'Flows Other',
				'Packets', 'Packets TCP', 'Packets UDP', 'Packets ICMP', 'Packets Other',
				'Bytes', 'Bytes TCP', 'Bytes UDP', 'Bytes ICMP', 'Bytes Other'
			];

			for (let i = 0; i < binaryLength; i++) {
				const data = results.map(item => {
					const byte = item.data.charCodeAt(i);
					// Convert from 8-bit unsigned to proper value
					return byte === 255 ? 0 : byte;
				});

				datasets.push({
					label: metricLabels[i] || `Metric ${i}`,
					data: data,
					borderColor: colors[i % colors.length],
					backgroundColor: chartType === 'stacked' ? colors[i % colors.length] + '80' : 'transparent',
					fill: chartType === 'stacked',
					tension: 0.1
				});
			}
		}

		return {
			type: chartType === 'stacked' ? 'line' : 'line',
			data: {
				labels,
				datasets
			},
			options: {
				onClick: handleChartClick,
				responsive: true,
				scales: {
					x: {
						title: {
							display: true,
							text: xAxisTitle
						}
					},
					y: {
						title: {
							display: true,
							text: 'Count'
						},
						ticks: {
							callback: function(value: any) {
								return formatNumber(value);
							}
						}
					}
				},
				plugins: {
					legend: {
						display: true,
						position: 'top' as const
					}
				},
				...(chartType === 'stacked' && {
					elements: {
						area: {
							fill: true
						}
					}
				})
			}
		};
	}

	onMount(() => {
		const config = createChartConfig();
		chart = new Chart(chartCanvas, config);

		return () => {
			if (chart) {
				chart.destroy();
			}
		};
	});

	// Update chart when props change
	$effect(() => {
		if (chart) {
			const config = createChartConfig();
			chart.data = config.data;
			chart.options = config.options;
			chart.update();
		}
	});
</script>

<div class="chart-container">
	<canvas bind:this={chartCanvas} class="w-full h-96"></canvas>
</div>

<style>
	.chart-container {
		position: relative;
		height: 400px;
		width: 100%;
	}
</style>