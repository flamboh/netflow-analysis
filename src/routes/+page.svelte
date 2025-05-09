<script lang="ts">
	let today = new Date().toJSON().slice(0, 10);
	let startDate = $state(today);
	let text = $state([]);

	async function loadText() {
		const response = await fetch('/text');
		if (response.ok) {
			const data = await response.json();
			text = data.result; // Update the text with the result from the server
			console.log('Loaded text:', text); // Log the result to the browser console
		} else {
			console.error('Failed to load text');
		}
	}
	function handleSubmit(e: Event) {
		e.preventDefault(); // Prevent the default form submission
		loadText(); // Call the loadText function when the form is submitted
	}
</script>

<div class="flex h-screen w-screen flex-col justify-center bg-slate-600">
	<form onsubmit={handleSubmit}>
		<label>
			<span class="hidden">Choose date:</span>
			<input
				class="m-1 w-48 bg-slate-300 hover:cursor-pointer"
				type="date"
				min={today}
				bind:value={startDate}
			/>
		</label>
		<label>
			<span class="hidden">Choose time:</span>
			<input class="m-1 w-48 bg-slate-300 hover:cursor-pointer" type="time" />
		</label>
	</form>
	<button class="text-4xl text-white" onclick={loadText}>Load Data</button>
	<p class="text-center text-2xl font-bold text-white">{text}</p>
</div>
