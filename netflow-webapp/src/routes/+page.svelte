<script lang="ts">
	import { onMount } from 'svelte';
	import { Chart } from 'chart.js/auto';
	import { getRelativePosition } from 'chart.js/helpers';

	let today = new Date().toJSON().slice(0, 10);

	let startDate = $state('2024-03-01');
	let endDate = $state(today);
	let routers = $state({
		'cc-ir1-gw': true,
		'oh-ir1-gw': true
	});
	let groupBy = $state('date');
	let chartType = $state('stacked');

	let results = $state<{ time: string; data: string }[]>([]);
	let chartCanvas: HTMLCanvasElement;
	let chart: Chart;

	let dataOptions: { label: string; index: number; checked: boolean }[] = $state([
		{ label: 'Flows', index: 0, checked: true },
		{ label: 'Flows TCP', index: 1, checked: false },
		{ label: 'Flows UDP', index: 2, checked: false },
		{ label: 'Flows ICMP', index: 3, checked: false },
		{ label: 'Flows Other', index: 4, checked: false },
		{ label: 'Packets', index: 5, checked: true },
		{ label: 'Packets TCP', index: 6, checked: false },
		{ label: 'Packets UDP', index: 7, checked: false },
		{ label: 'Packets ICMP', index: 8, checked: false },
		{ label: 'Packets Other', index: 9, checked: false },
		{ label: 'Bytes', index: 10, checked: true },
		{ label: 'Bytes TCP', index: 11, checked: false },
		{ label: 'Bytes UDP', index: 12, checked: false },
		{ label: 'Bytes ICMP', index: 13, checked: false },
		{ label: 'Bytes Other', index: 14, checked: false }
	]);

	function getClickedElement(activeElements: any) {
		if (activeElements.length > 0) {
			const element = activeElements[0];
			const datasetIndex = element.datasetIndex;
			const index = element.index;
			const dataset = chart.data.datasets[datasetIndex];
			const label = chart.data.labels?.[index];
			const value = dataset.data[index];
			return { dataset, label, value, datasetIndex, index };
		}
	}

	onMount(() => {
		// Initialize empty chart
		chart = new Chart(chartCanvas, {
			type: 'line',
			data: {
				labels: [],
				datasets: [] // Start with no datasets
			},
			options: {
				onClick: (e, activeElements) => {
					const clickedElement: any = getClickedElement(activeElements);
					if (clickedElement) {
						console.log('Clicked point:', {
							dataset: clickedElement.dataset.label,
							label: clickedElement.label,
							value: clickedElement.value,
							datasetIndex: clickedElement.datasetIndex,
							index: clickedElement.index
						});
						// if (groupBy === 'month') {
						// 	groupBy = 'date';
						// 	startDate = clickedElement.label + '-01';
						// 	endDate = clickedElement.label + '-31';
						if (groupBy === 'date') {
							groupBy = 'hour';
							const date = new Date(clickedElement.label);
							// Set 1 month span around clicked date
							const startOfMonth = new Date(date.getFullYear(), date.getMonth(), 1);
							const endOfMonth = new Date(date.getFullYear(), date.getMonth() + 1, 0);
							startDate = startOfMonth.toISOString().slice(0, 10);
							endDate = endOfMonth.toISOString().slice(0, 10);
						} else if (groupBy === 'hour') {
							groupBy = '30min';
							const hourLabel = clickedElement.label; // Format: "YYYY-MM-DD HH:00"
							const [datePart, timePart] = hourLabel.split(' ');
							const date = new Date(datePart);
							// Set 1 week span around clicked date
							const startOfWeek = new Date(date.getTime() - 3 * 24 * 60 * 60 * 1000);
							const endOfWeek = new Date(date.getTime() + 4 * 24 * 60 * 60 * 1000);
							startDate = startOfWeek.toISOString().slice(0, 10);
							endDate = endOfWeek.toISOString().slice(0, 10);
						} else if (groupBy === '30min') {
							groupBy = '5min';
							const minuteLabel = clickedElement.label; // Format: "YYYY-MM-DD HH:MM"
							const [datePart, timePart] = minuteLabel.split(' ');
							const date = new Date(datePart);
							// Set 1 day span
							startDate = date.toISOString().slice(0, 10);
							endDate = new Date(date.getTime() + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
						} else {
							groupBy = 'date';
							startDate = '2024-03-01';
							endDate = today;
						}
						loadData();
					} else {
						const canvasPosition = getRelativePosition(e, chart);
						const dataX = chart.scales.x.getValueForPixel(canvasPosition.x);
						const dataY = chart.scales.y.getValueForPixel(canvasPosition.y);
						console.log('Clicked coordinates:', { x: dataX, y: dataY });
					}
				},
				responsive: true,
				scales: {
					x: {
						// Keep x-axis config
						title: {
							display: true,
							text: 'Date'
						}
					}
					// Y-axes will be added dynamically in loadData
				}
			}
		});
	});

	function dataOptionsToBinary() {
		const result = dataOptions.reduce(
			(acc, curr) => acc + (curr.checked ? 1 : 0) * Math.pow(2, curr.index),
			0
		);
		return result;
	}

	async function loadData() {
		const response = await fetch(
			'/data?startDate=' +
				Math.floor(new Date(startDate).getTime() / 1000) +
				'&endDate=' +
				Math.floor(new Date(endDate).getTime() / 1000) +
				'&routers=' +
				(routers['cc-ir1-gw'] ? 'cc-ir1-gw' : '') +
				',' +
				(routers['oh-ir1-gw'] ? 'oh-ir1-gw' : '') +
				'&dataOptions=' +
				dataOptionsToBinary() +
				'&groupBy=' +
				groupBy,
			{
				method: 'GET',
				headers: {
					'Content-Type': 'application/json'
				}
			}
		);

		if (response.ok) {
			const res = await response.json();
			results = res.result;
			// Update chart with new data
			if (chart) {
				// Format labels and title based on groupBy selection
				let labels: string[];
				let xAxisTitle: string;

				if (groupBy === 'month') {
					labels = results.map((item) => {
						const year = item.time.slice(0, 4);
						const month = item.time.slice(4, 6);
						return `${year}-${month}`;
					});
					xAxisTitle = 'Month';
				} else if (groupBy === 'date') {
					labels = results.map((item) => {
						const year = item.time.slice(0, 4);
						const month = item.time.slice(4, 6);
						const day = item.time.slice(6, 8);
						return `${year}-${month}-${day}`;
					});
					xAxisTitle = 'Date';
				} else if (groupBy === 'hour') {
					labels = results.map((item) => {
						const year = item.time.slice(0, 4);
						const month = item.time.slice(4, 6);
						const day = item.time.slice(6, 8);
						const hour = item.time.slice(9, 11);
						return `${year}-${month}-${day} ${hour}:00`;
					});
					xAxisTitle = 'Hour';
				} else if (groupBy === '30min') {
					labels = results.map((item) => {
						const year = item.time.slice(0, 4);
						const month = item.time.slice(4, 6);
						const day = item.time.slice(6, 8);
						const hour = item.time.slice(9, 11);
						const minute = item.time.slice(12, 14);
						return `${year}-${month}-${day} ${hour}:${minute}`;
					});
					xAxisTitle = '30 Minutes';
				} else {
					// 5min
					labels = results.map((item) => {
						const year = item.time.slice(0, 4);
						const month = item.time.slice(4, 6);
						const day = item.time.slice(6, 8);
						const hour = item.time.slice(9, 11);
						const minute = item.time.slice(12, 14);
						return `${year}-${month}-${day} ${hour}:${minute}`;
					});
					xAxisTitle = '5 Minutes';
				}

				chart.data.labels = labels;
				chart.data.datasets = []; // Clear existing datasets

				// Use manual chart type selection
				const isStackedChart = chartType === 'stacked';

				const newScales: any = {
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
										if (num >= 1e15) return (num / 1e15).toFixed(0) + ' quadrillion';
										if (num >= 1e12) return (num / 1e12).toFixed(0) + ' trillion';
										if (num >= 1e9) return (num / 1e9).toFixed(0) + ' billion';
										if (num >= 1e6) return (num / 1e6).toFixed(0) + ' million';
										if (num >= 1e3) return (num / 1e3).toFixed(0) + ' thousand';
										return num.toString();
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
										if (num >= 1e15) return (num / 1e15).toFixed(0) + ' quadrillion';
										if (num >= 1e12) return (num / 1e12).toFixed(0) + ' trillion';
										if (num >= 1e9) return (num / 1e9).toFixed(0) + ' billion';
										if (num >= 1e6) return (num / 1e6).toFixed(0) + ' million';
										if (num >= 1e3) return (num / 1e3).toFixed(0) + ' thousand';
										return num.toString();
									}
								}
							}
				};

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
				let colorIndex = 0;

				for (const option of dataOptions) {
					if (option.checked) {
						const data = results.map((item) =>
							parseInt(item.data.split('\n')[option.index + 1].split(' ')[1])
						);
						const color = predefinedColors[colorIndex % predefinedColors.length];
						colorIndex++;

						chart.data.datasets.push({
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

				chart.options.scales = newScales;
				chart.update();
			}
		} else {
			console.error('Failed to load data');
		}
	}

	function handleSubmit(e: Event) {
		e.preventDefault();
		loadData();
	}

	function selectAllFlows() {
		dataOptions.forEach((option) => {
			option.checked = option.label.includes('Flows');
		});
	}

	function selectAllPackets() {
		dataOptions.forEach((option) => {
			option.checked = option.label.includes('Packets');
		});
	}

	function selectAllBytes() {
		dataOptions.forEach((option) => {
			option.checked = option.label.includes('Bytes');
		});
	}

	onMount(() => {
		loadData();
	});
</script>

<div class="flex min-h-screen w-screen flex-col justify-center bg-slate-600">
	<p class="m-4 text-center text-4xl text-white">NetFlow Analysis and Visualization</p>
	<form onsubmit={handleSubmit} class="flex flex-col items-center justify-center">
		<div class="flex flex-row items-center">
			<span class="mx-2 text-white">from</span>
			<label>
				<span class="hidden">Choose date:</span>
				<input
					class="m-1 w-48 bg-slate-300 hover:cursor-pointer"
					type="date"
					bind:value={startDate}
				/>
			</label>
			<span class="mx-2 text-white">to</span>
			<label>
				<span class="hidden">Choose date:</span>
				<input
					class="m-1 w-48 bg-slate-300 hover:cursor-pointer"
					type="date"
					bind:value={endDate}
				/>
			</label>
		</div>
		<label class="mb-2 flex flex-row items-center">
			<span class="mx-2 text-white">On routers:</span>
			<input type="checkbox" bind:checked={routers['cc-ir1-gw']} onchange={loadData} />
			<span class="mx-2 text-white">cc-ir1-gw</span>
			<input type="checkbox" bind:checked={routers['oh-ir1-gw']} onchange={loadData} />
			<span class="mx-2 text-white">oh-ir1-gw</span>
		</label>
		<div class="flex flex-row items-center">
			<div class="flex flex-col items-center justify-center">
				<div class="grid grid-cols-3 gap-4">
					<div class="flex flex-col gap-2">
						<button
							class="mb-2 rounded bg-blue-500 px-2 py-1 text-sm text-white hover:bg-blue-600"
							onclick={selectAllFlows}
						>
							All Flows
						</button>
						{#each dataOptions.slice(0, 5) as option}
							<label class="flex items-center gap-2">
								<input type="checkbox" bind:checked={option.checked} onchange={loadData} />
								<span class="text-white">{option.label}</span>
							</label>
						{/each}
					</div>
					<div class="flex flex-col gap-2">
						<button
							class="mb-2 rounded bg-green-500 px-2 py-1 text-sm text-white hover:bg-green-600"
							onclick={selectAllPackets}
						>
							All Packets
						</button>
						{#each dataOptions.slice(5, 10) as option}
							<label class="flex items-center gap-2">
								<input type="checkbox" bind:checked={option.checked} onchange={loadData} />
								<span class="text-white">{option.label}</span>
							</label>
						{/each}
					</div>
					<div class="flex flex-col gap-2">
						<button
							class="mb-2 rounded bg-red-500 px-2 py-1 text-sm text-white hover:bg-red-600"
							onclick={selectAllBytes}
						>
							All Bytes
						</button>
						{#each dataOptions.slice(10, 15) as option}
							<label class="flex items-center gap-2">
								<input type="checkbox" bind:checked={option.checked} onchange={loadData} />
								<span class="text-white">{option.label}</span>
							</label>
						{/each}
					</div>
				</div>
			</div>
		</div>
		<div class="my-2 flex flex-row items-center justify-center">
			<div class="flex flex-row items-center justify-center">
				<span class="mx-2 text-white">group by</span>
				<input
					type="radio"
					bind:group={groupBy}
					name="groupBy"
					value="month"
					checked={groupBy === 'month'}
					onchange={loadData}
				/>
				<span class="mx-2 text-white">month</span>
				<input
					type="radio"
					bind:group={groupBy}
					name="groupBy"
					value="date"
					checked={groupBy === 'date'}
					onchange={loadData}
				/>
				<span class="mx-2 text-white">date</span>
				<input
					type="radio"
					bind:group={groupBy}
					name="groupBy"
					value="hour"
					checked={groupBy === 'hour'}
					onchange={loadData}
				/>
				<span class="mx-2 text-white">hour</span>
				<input
					type="radio"
					bind:group={groupBy}
					name="groupBy"
					value="30min"
					checked={groupBy === '30min'}
					onchange={loadData}
				/>
				<span class="mx-2 text-white">30min</span>
				<input
					type="radio"
					bind:group={groupBy}
					name="groupBy"
					value="5min"
					checked={groupBy === '5min'}
					onchange={loadData}
				/>
				<span class="mx-2 text-white">5min</span>
			</div>
		</div>
		<div class="my-2 flex flex-row items-center justify-center">
			<div class="flex flex-row items-center justify-center">
				<span class="mx-2 text-white">chart type</span>
				<input
					type="radio"
					bind:group={chartType}
					name="chartType"
					value="stacked"
					checked={chartType === 'stacked'}
					onchange={loadData}
				/>
				<span class="mx-2 text-white">stacked</span>
				<input
					type="radio"
					bind:group={chartType}
					name="chartType"
					value="line"
					checked={chartType === 'line'}
					onchange={loadData}
				/>
				<span class="mx-2 text-white">line</span>
			</div>
		</div>
	</form>
	<div class="flex flex-col items-center justify-center">
		<button
			class="mb-12 mt-4 rounded-lg bg-slate-300 p-2 text-center text-4xl text-black"
			onclick={loadData}
		>
			Load
		</button>
	</div>
	<div class="mx-auto w-full max-w-7xl flex-col rounded-lg bg-white p-4">
		<div class="text-2xl text-black">
			NetFlow data from {startDate} to {endDate} on {routers['cc-ir1-gw'] ? 'cc-ir1-gw' : ''}
			{routers['oh-ir1-gw'] ? ' and oh-ir1-gw' : ''} grouped by {groupBy}
		</div>
		<canvas bind:this={chartCanvas}></canvas>
	</div>
</div>
