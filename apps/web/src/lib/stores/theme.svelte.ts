class Theme {
	dark = $state(false);

	syncFromDom() {
		if (typeof document === 'undefined') {
			return;
		}

		this.dark = document.documentElement.classList.contains('dark');
	}

	setDark(next: boolean) {
		this.dark = next;

		if (typeof document === 'undefined') {
			return;
		}

		document.documentElement.classList.toggle('dark', next);
		document.documentElement.style.colorScheme = next ? 'dark' : 'light';
		localStorage.setItem('dark-mode', String(next));
	}

	toggle() {
		this.setDark(!this.dark);
	}
}

export const theme = new Theme();
