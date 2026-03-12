import { browser } from '$app/environment';

const STORAGE_KEY = 'netflow-theme';

type Theme = 'light' | 'dark';

class ThemeStore {
	current = $state<Theme>('light');

	constructor() {
		if (browser) {
			const stored = localStorage.getItem(STORAGE_KEY) as Theme | null;
			if (stored === 'light' || stored === 'dark') {
				this.current = stored;
			} else if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
				this.current = 'dark';
			}
			this.applyToDocument();
		}
	}

	toggle() {
		this.current = this.current === 'dark' ? 'light' : 'dark';
		this.applyToDocument();
		if (browser) {
			localStorage.setItem(STORAGE_KEY, this.current);
		}
	}

	get isDark() {
		return this.current === 'dark';
	}

	private applyToDocument() {
		if (!browser) return;
		const root = document.documentElement;
		if (this.current === 'dark') {
			root.classList.add('dark');
		} else {
			root.classList.remove('dark');
		}
	}
}

export const theme = new ThemeStore();
