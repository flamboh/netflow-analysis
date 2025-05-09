<script lang="ts">
	let today = new Date().toJSON().slice(0, 10);
	let date = $state(today);
	let time = $state('00:00');
	let data = $state([]);

	async function loadData() {
		const response = await fetch(
			'/data?date=' + date.replaceAll('-', '') + '&time=' + time.replace(':', ''),
			{
				method: 'GET',
				headers: {
					'Content-Type': 'application/json'
				}
			}
		);

		if (response.ok) {
			const res = await response.json();
			data = res.result; // Update the data with the result from the server
			console.log('Loaded data:', data); // Log the result to the browser console
		} else {
			console.error('Failed to load data');
		}
	}
	function handleSubmit(e: Event) {
		e.preventDefault(); // Prevent the default form submission
		loadData(); // Call the loaddata function when the form is submitted
	}
</script>

<div class="flex h-screen w-screen flex-col justify-center bg-slate-600">
	<form onsubmit={handleSubmit} class="flex flex-col items-center justify-center">
		<label>
			<span class="hidden">Choose date:</span>
			<input class="m-1 w-48 bg-slate-300 hover:cursor-pointer" type="date" bind:value={date} />
		</label>
		<label>
			<span class="hidden">Choose time:</span>
			<input
				class="m-1 w-48 bg-slate-300 hover:cursor-pointer"
				type="time"
				bind:value={time}
				step="300"
			/>
		</label>
	</form>
	<button class="text-4xl text-white" onclick={loadData}>Load Data from {date} at {time}</button>
	{#each data as item}
		<p class="text-center text-2xl font-bold text-white">{item}</p>
	{/each}
	<!-- <p class="text-center text-2xl font-bold text-white">{data}</p> -->
</div>
