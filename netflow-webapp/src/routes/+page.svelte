<script lang="ts">
	import { onMount } from 'svelte';
	import { Chart } from 'chart.js/auto';

	let today = new Date().toJSON().slice(0, 10);
	let startDate = $state('2025-05-01');
	let endDate = $state(today);
	let fullDay = $state(false);
	let time = $state('12:00');
	let endTime = $state('01:00');
	let routers = $state({
		'cc-ir1-gw': false,
		'oh-ir1-gw': false
	});

	let results = $state<{ time: string; data: string }[]>([]);
	let chartCanvas: HTMLCanvasElement;
	let chart: Chart;

	let dataOptions: { label: string; index: number; checked: boolean }[] = [
		{ label: 'Flows', index: 1, checked: false },
		{ label: 'Flows TCP', index: 2, checked: false },
		{ label: 'Flows UDP', index: 3, checked: false },
		{ label: 'Flows ICMP', index: 4, checked: false },
		{ label: 'Flows Other', index: 5, checked: false },
		{ label: 'Packets', index: 6, checked: false },
		{ label: 'Packets TCP', index: 7, checked: false },
		{ label: 'Packets UDP', index: 8, checked: false },
		{ label: 'Packets ICMP', index: 9, checked: false },
		{ label: 'Packets Other', index: 10, checked: false },
		{ label: 'Bytes', index: 11, checked: false },
		{ label: 'Bytes TCP', index: 12, checked: false },
		{ label: 'Bytes UDP', index: 13, checked: false },
		{ label: 'Bytes ICMP', index: 14, checked: false },
		{ label: 'Bytes Other', index: 15, checked: false },
		{ label: 'First', index: 16, checked: false },
		{ label: 'Last', index: 17, checked: false },
		{ label: 'MSEC First', index: 18, checked: false },
		{ label: 'MSEC Last', index: 19, checked: false },
		{ label: 'Sequence Failures', index: 20, checked: false }
	];

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

	async function loadData() {
		const response = await fetch(
			'/data?startDate=' +
				startDate.replaceAll('-', '') +
				'&endDate=' +
				endDate.replaceAll('-', '') +
				'&fullDay=' +
				fullDay +
				'&time=' +
				time.replace(':', '') +
				'&endTime=' +
				endTime.replace(':', '') +
				'&routers=' +
				(routers['cc-ir1-gw'] ? 'cc-ir1-gw' : '') +
				',' +
				(routers['oh-ir1-gw'] ? 'oh-ir1-gw' : ''),
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
			console.log(results); 	
			// Update chart with new data
			if (chart) {
				chart.data.labels = results.map(
					(item) =>
						item.time.slice(4, 6) + '/' + item.time.slice(6, 8) + '/' + item.time.slice(2, 4)
				);
				chart.data.datasets = []; // Clear existing datasets

				const newScales: any = {
					x: {
						title: {
							display: true,
							text: 'Date'
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
							parseInt(item.data.split('\n')[option.index].split(' ')[1])
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

<div class="flex h-screen w-screen flex-col justify-center bg-slate-600">
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
				<div class="grid grid-cols-4 gap-2">
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

	<div class="mx-auto w-full max-w-4xl rounded-lg bg-white p-4">
		<canvas bind:this={chartCanvas}></canvas>
	</div>
</div>
