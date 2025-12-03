<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import { Chart } from 'chart.js/auto';
	import { getRelativePosition } from 'chart.js/helpers';
	import { goto } from '$app/navigation';
	import { verticalCrosshairPlugin } from './crosshair-plugin';
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
		ChartConfig,
		ChartDataset
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
	let chart: Chart | null = null;
	let resizeObserver: ResizeObserver | null = null;

	function formatTickLabel(date: Date | null, currentGroupBy: GroupByOption, index: number): string {
		if (!date || Number.isNaN(date.getTime())) {
			return '';
		}
		const weekday = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][date.getDay()];
		const month = date.getMonth() + 1;
		const day = date.getDate();
		const hours = date.getHours();
		const minutes = date.getMinutes();

		if (currentGroupBy === 'date') {
			return date.getDay() === 1 ? `${weekday} ${month}/${day}` : '';
		}

		if (currentGroupBy === 'hour') {
			return hours === 0 ? `${weekday} ${month}/${day}` : '';
		}

		if (currentGroupBy === '30min') {
			if (minutes === 0 && (hours === 0 || hours === 12)) {
				return `${weekday} ${month}/${day} ${hours.toString().padStart(2, '0')}:00`;
			}
			return '';
		}

		if (currentGroupBy === '5min') {
			if (minutes === 0) {
				return `${weekday} ${month}/${day} ${hours.toString().padStart(2, '0')}:00`;
			}
			return '';
		}

		return index === 0 ? `${weekday} ${month}/${day}` : '';
	}

	function shouldHighlightTick(date: Date | null, currentGroupBy: GroupByOption, index: number): boolean {
		if (!date || Number.isNaN(date.getTime())) {
			return index === 0;
		}
		const hours = date.getHours();
		const minutes = date.getMinutes();

		if (currentGroupBy === 'date') {
			return date.getDay() === 1;
		}
		if (currentGroupBy === 'hour') {
			return hours === 0;
		}
		if (currentGroupBy === '30min') {
			return minutes === 0 && (hours === 0 || hours === 12);
		}
		if (currentGroupBy === '5min') {
			return minutes === 0;
		}
		return index === 0;
	}

	function getClickedElement(
		activeElements: { datasetIndex: number; index: number }[]
	): ClickedElement | null {
		if (!chart) {
			return null;
		}
		if (activeElements.length > 0) {
			const element = activeElements[0];
			const datasetIndex = element.datasetIndex;
			const index = element.index;
			const dataset = chart.data.datasets[datasetIndex] as ChartDataset;
			const label = chart.data.labels?.[index] as string;
			const value = dataset.data[index] as number;
			return {
				dataset: {
					label: dataset.label,
					data: dataset.data,
					backgroundColor: dataset.backgroundColor as string,
					borderColor: dataset.borderColor as string
				},
				label,
				value,
				datasetIndex,
				index
			};
		}
		return null;
	}

	function handleChartClick(
		e: MouseEvent,
		activeElements: { datasetIndex: number; index: number }[]
	) {
		if (!chart) {
			return;
		}
		// Always get the canvas position and convert to data values
		const canvasPosition = getRelativePosition(e, chart);
		const dataX = chart.scales.x.getValueForPixel(canvasPosition.x);
		const dataY = chart.scales.y.getValueForPixel(canvasPosition.y);

		console.log('=== CHART CLICK DEBUG ===');
		console.log('Canvas position:', canvasPosition);
		console.log('Data coordinates:', { x: dataX, y: dataY });
		console.log('Current groupBy:', groupBy);
		console.log('Chart labels:', chart.data.labels);

		// Convert the x-axis data value to the appropriate date based on current groupBy
		let clickedDate: Date;

		if (typeof dataX === 'number') {
			// dataX is the index in the labels array
			const labelIndex = Math.round(dataX);
			const labels = chart.data.labels;

			console.log('Calculated label index:', labelIndex);
			console.log('Total labels:', labels?.length);

			if (labels && labelIndex >= 0 && labelIndex < labels.length) {
				const clickedLabel = labels[labelIndex] as string;
				console.log('Clicked label:', clickedLabel);
				clickedDate = parseClickedLabel(clickedLabel, groupBy);
			} else {
				// Handle clicks outside the data range
				console.log('Click outside data range, using boundary logic');
				if (labels && labels.length > 0) {
					let targetLabel: string;
					if (labelIndex < 0) {
						targetLabel = labels[0] as string;
					} else {
						targetLabel = labels[labels.length - 1] as string;
					}
					clickedDate = parseClickedLabel(targetLabel, groupBy);
				} else {
					console.log('No labels available, cannot determine date');
					return;
				}
			}
		} else {
			console.log('Unexpected dataX type:', typeof dataX, dataX);
			return;
		}

		console.log('Parsed clicked date:', clickedDate);

		const clickedElement = getClickedElement(activeElements);
		if (clickedElement) {
			console.log('Clicked on data point:', {
				dataset: clickedElement.dataset.label,
				label: clickedElement.label,
				value: clickedElement.value,
				datasetIndex: clickedElement.datasetIndex,
				index: clickedElement.index
			});
		} else {
			console.log('Clicked on empty space - will drill down based on calculated date');
		}

		// Handle drill-down logic using the calculated clickedDate
		// This works regardless of whether there was a data point at the exact click location
		console.log('=== DRILL DOWN LOGIC ===');
		console.log('Current groupBy:', groupBy);
		console.log('Using date for drill-down:', clickedDate);

		if (groupBy === 'date') {
			const startOfMonth = new Date(clickedDate.getTime() - 15 * 24 * 60 * 60 * 1000);
			const endOfMonth = new Date(clickedDate.getTime() + 16 * 24 * 60 * 60 * 1000);
			console.log('Drilling down to hour view with date range:', {
				start: startOfMonth.toISOString().slice(0, 10),
				end: endOfMonth.toISOString().slice(0, 10)
			});
			onDrillDown?.(
				'hour',
				startOfMonth.toISOString().slice(0, 10),
				endOfMonth.toISOString().slice(0, 10)
			);
		} else if (groupBy === 'hour') {
			const startOfWeek = new Date(clickedDate.getTime() - 3 * 24 * 60 * 60 * 1000);
			const endOfWeek = new Date(clickedDate.getTime() + 4 * 24 * 60 * 60 * 1000);
			console.log('Drilling down to 30min view with date range:', {
				start: startOfWeek.toISOString().slice(0, 10),
				end: endOfWeek.toISOString().slice(0, 10)
			});
			onDrillDown?.(
				'30min',
				startOfWeek.toISOString().slice(0, 10),
				endOfWeek.toISOString().slice(0, 10)
			);
		} else if (groupBy === '30min') {
			const endDate = new Date(clickedDate.getTime() + 24 * 60 * 60 * 1000);
			console.log('Drilling down to 5min view with date range:', {
				start: clickedDate.toISOString().slice(0, 10),
				end: endDate.toISOString().slice(0, 10)
			});
			onDrillDown?.(
				'5min',
				clickedDate.toISOString().slice(0, 10),
				endDate.toISOString().slice(0, 10)
			);
		} else if (groupBy === '5min') {
			// For 5min level, we need to create a slug from the clicked date
			// Convert the date back to a label format that generateSlugFromLabel expects
			let labelForSlug: string;
			if (clickedElement) {
				labelForSlug = clickedElement.label;
			} else {
				// Format the date to match the label format for 5min grouping
				const year = clickedDate.getFullYear();
				const month = String(clickedDate.getMonth() + 1).padStart(2, '0');
				const day = String(clickedDate.getDate()).padStart(2, '0');
				const hour = String(clickedDate.getHours()).padStart(2, '0');
				const minute = String(clickedDate.getMinutes()).padStart(2, '0');
				labelForSlug = `${year}-${month}-${day} ${hour}:${minute}`;
			}

			const slug = generateSlugFromLabel(labelForSlug, groupBy);
			console.log('Navigating to file with slug:', slug);
			if (onNavigateToFile) {
				onNavigateToFile(slug);
			} else {
				goto(`/api/netflow/files/${slug}`);
			}
		}

		console.log('=== END CHART CLICK DEBUG ===');
	}

	function parseLabelToDate(label: string | undefined): Date | null {
		if (!label) return null;
		const [datePart, timePart] = label.split(' ');
		const [yearStr, monthStr, dayStr] = datePart.split('-');
		const year = Number(yearStr);
		const month = Number(monthStr);
		const day = Number(dayStr);
		if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
		const [hourStr, minuteStr] = timePart?.split(':') ?? [];
		const hour = Number(hourStr);
		const minute = Number(minuteStr);
		const date = new Date(
			year,
			month - 1,
			day,
			Number.isFinite(hour) ? hour : 0,
			Number.isFinite(minute) ? minute : 0
		);
		return Number.isNaN(date.getTime()) ? null : date;
	}

	function getLabelDateFromLabels(labels: (string | number | null | undefined)[], idx: number): Date | null {
		const label = labels[idx];
		return typeof label === 'string' ? parseLabelToDate(label) : null;
	}

	function createChartConfig(): ChartConfig {
		const labels = formatLabels(results, groupBy);
		const getLabelDate = (idx: number): Date | null => getLabelDateFromLabels(labels, idx);
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
		const datasets: ChartDataset[] = [];
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
		const selectedOptions = dataOptions.filter((o) => o.checked);
		const allAreBytesMetrics = selectedOptions.every((o) => o.label.includes('Bytes'));

		// Original scales configuration
		const scales: Record<string, object> = {
			x: {
				title: {
					display: true,
					text: xAxisTitle
				},
				ticks: {
					autoSkip: false,
					maxRotation: 45,
					minRotation: 45,
					callback: (_val: string | number, idx: number) =>
						formatTickLabel(getLabelDate(Number(idx)), groupBy, Number(idx))
				},
				grid: {
					color: (ctx: { index?: number; tick?: { index?: number } }) => {
						const tickIndex = ctx.index ?? ctx.tick?.index ?? 0;
						const safeIndex = Number.isFinite(Number(tickIndex)) ? Number(tickIndex) : 0;
						return shouldHighlightTick(getLabelDate(safeIndex), groupBy, safeIndex)
							? 'rgba(0,0,0,0.08)'
							: 'rgba(0,0,0,0.02)';
					}
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
							callback: function (value: string | number) {
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
							callback: function (value: string | number) {
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
				maintainAspectRatio: false,
				scales: scales,
				plugins: {
					legend: {
						display: true,
						position: 'top' as const
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
						}
					}
				} as Record<string, object>
			}
		};
	}

	onMount(() => {
		// Register the crosshair plugin
		Chart.register(verticalCrosshairPlugin);

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
				maintainAspectRatio: false,
				scales: {
					x: {
						title: {
							display: true,
							text: 'Date'
						},
						ticks: {
							autoSkip: false,
							maxRotation: 45,
							minRotation: 45,
							callback: (_val: string | number, idx: number) =>
								formatTickLabel(
									getLabelDateFromLabels((chart?.data.labels as (string | number | null | undefined)[] | undefined) ?? [],
									Number(idx)),
									groupBy,
									Number(idx)
								)
						},
						grid: {
							color: (ctx: { index?: number; tick?: { index?: number } }) => {
								const tickIndex = ctx.index ?? ctx.tick?.index ?? 0;
								const safeIndex = Number.isFinite(Number(tickIndex)) ? Number(tickIndex) : 0;
								return shouldHighlightTick(
									getLabelDateFromLabels(
										(chart?.data.labels as (string | number | null | undefined)[] | undefined) ?? [],
										safeIndex
									),
									groupBy,
									safeIndex
								)
									? 'rgba(0,0,0,0.08)'
									: 'rgba(0,0,0,0.02)';
							}
						}
					}
				},
				plugins: {
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
						}
					}
				} as Record<string, object>
			}
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
		} as any);

		const container = chartCanvas.parentElement;
		if (container) {
			resizeObserver = new ResizeObserver(() => {
				chart?.resize();
			});
			resizeObserver.observe(container);
		}

		return () => {
			resizeObserver?.disconnect();
			resizeObserver = null;
			chart?.destroy();
			chart = null;
		};
	});

	onDestroy(() => {
		chart?.destroy();
		chart = null;
		resizeObserver?.disconnect();
		resizeObserver = null;
	});

	// Update chart when props change (matches original loadData behavior)
	$effect(() => {
		if (chart && results.length > 0) {
			const config = createChartConfig();
			chart.data = config.data;
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			chart.options = config.options as any;
			chart.update();
		}
	});
</script>

<div class="chart-container">
	<canvas bind:this={chartCanvas} class="h-full w-full"></canvas>
</div>

<style>
	.chart-container {
		position: relative;
		height: 100%;
		width: 100%;
	}
</style>
