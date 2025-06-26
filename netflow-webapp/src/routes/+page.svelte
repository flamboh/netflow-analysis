<script lang="ts">
	import { onMount } from 'svelte';
	import { Chart } from 'chart.js/auto';

	let today = new Date().toJSON().slice(0, 10);
	let startDate = $state('2025-05-01');
	let endDate = $state(today);
	let fullDay = $state(true);
	let time = $state('12:00');
	let endTime = $state('01:00');
	let routers = $state({
		'cc-ir1-gw': true,
		'oh-ir1-gw': true
	});
	let groupBy = $state('date');

	let results = $state<{ time: string; data: string }[]>([]);
	let chartCanvas: HTMLCanvasElement;
	let chart: Chart;

	let dataOptions: { label: string; index: number; checked: boolean }[] = $state([
		{ label: 'Flows', index: 0, checked: false },
		{ label: 'Flows TCP', index: 1, checked: false },
		{ label: 'Flows UDP', index: 2, checked: false },
		{ label: 'Flows ICMP', index: 3, checked: false },
		{ label: 'Flows Other', index: 4, checked: false },
		{ label: 'Packets', index: 5, checked: false },
		{ label: 'Packets TCP', index: 6, checked: false },
		{ label: 'Packets UDP', index: 7, checked: false },
		{ label: 'Packets ICMP', index: 8, checked: false },
		{ label: 'Packets Other', index: 9, checked: false },
		{ label: 'Bytes', index: 10, checked: false },
		{ label: 'Bytes TCP', index: 11, checked: false },
		{ label: 'Bytes UDP', index: 12, checked: false },
		{ label: 'Bytes ICMP', index: 13, checked: false },
		{ label: 'Bytes Other', index: 14, checked: false }
		// { label: 'Sequence Failures', index: 15, checked: false }
	]);

	onMount(() => {
		// Initialize empty chart
		chart = new Chart(chartCanvas, {
			type: 'line',
			data: {
				labels: [],
				datasets: [] // Start with no datasets
			},
			options: {
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
				'&fullDay=' +
				fullDay +
				'&time=' +
				time.replace(':', '') +
				'&endTime=' +
				endTime.replace(':', '') +
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
			console.log('hello');
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
						const year = item.time.slice(2, 4);
						const month = item.time.slice(4, 6);
						const day = item.time.slice(6, 8);
						return `${month}/${day}/${year}`;
					});
					xAxisTitle = 'Date';
				} else {
					// hour
					labels = results.map((item) => {
						const hour = item.time.slice(9, 11);
						return `${hour}`;
					});
					xAxisTitle = 'Hour';
				}

				chart.data.labels = labels;
				chart.data.datasets = []; // Clear existing datasets

				const newScales: any = {
					x: {
						title: {
							display: true,
							text: xAxisTitle
						}
					},
					y: {
						display: true,
						type: 'logarithmic',
						beginAtZero: true,
						min: 1, // Avoid log(0) issues
						title: {
							display: true,
							text: 'Value (Log Scale)'
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
							tension: 0.1
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
</script>

<div class="flex min-h-screen w-screen flex-col justify-center bg-slate-600">
	<p class="m-4 text-center text-4xl text-white">nfdump data line graph</p>
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
		<div class="flex flex-row items-center">
			<span class="mx-2 text-white">at</span>
			<label>
				<span class="hidden">Choose start time:</span>
				<input
					class="m-1 w-48 bg-slate-300 hover:cursor-pointer disabled:cursor-not-allowed disabled:brightness-50"
					type="time"
					bind:value={time}
					step="300"
					disabled={fullDay}
				/>
				<span class="mx-2 text-white">or</span>
				<input type="checkbox" bind:checked={fullDay} />
				<span class="mx-2 text-white">full day</span>
			</label>
		</div>
		<label class="mb-2 flex flex-row items-center">
			<span class="mx-2 text-white">On routers:</span>
			<input type="checkbox" bind:checked={routers['cc-ir1-gw']} />
			<span class="mx-2 text-white">cc-ir1-gw</span>
			<input type="checkbox" bind:checked={routers['oh-ir1-gw']} />
			<span class="mx-2 text-white">oh-ir1-gw</span>
		</label>
		<div class="flex flex-row items-center">
			<div class="flex flex-col items-center justify-center">
				<div class="grid grid-cols-3 gap-4">
					{#each dataOptions.reduce((acc, curr, i) => {
							const colIndex = Math.floor(i / 5);
							if (!acc[colIndex]) acc[colIndex] = [];
							acc[colIndex].push(curr);
							return acc;
						}, [] as (typeof dataOptions)[]) as column}
						<div class="flex flex-col gap-2">
							{#each column as option}
								<label class="flex items-center gap-2">
									<input type="checkbox" bind:checked={option.checked} />
									<span class="text-white">{option.label}</span>
								</label>
							{/each}
						</div>
					{/each}
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
				/>
				<span class="mx-2 text-white">month</span>
				<input
					type="radio"
					bind:group={groupBy}
					name="groupBy"
					value="date"
					checked={groupBy === 'date'}
				/>
				<span class="mx-2 text-white">date</span>
				<input
					type="radio"
					bind:group={groupBy}
					name="groupBy"
					value="hour"
					checked={groupBy === 'hour'}
				/>
				<span class="mx-2 text-white">hour</span>
			</div>
		</div>
	</form>
	<div class="flex flex-col items-center justify-center">
		<button
			class="m-4 rounded-lg bg-slate-300 p-2 text-center text-4xl text-black"
			onclick={loadData}
		>
			Load
		</button>
		<div class="text-white">NFDUMP data at {time} from {startDate} onward</div>
	</div>

	<div class="mx-auto w-full max-w-4xl rounded-lg bg-white p-4 py-8">
		<canvas bind:this={chartCanvas}></canvas>
	</div>
</div>
