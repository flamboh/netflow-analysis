<script>
	let text = '';

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
</script>

<button on:click={loadText}>Load Text</button>
<p class="text-2xl">{text}</p>
