import { fileURLToPath } from 'node:url';
import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';

const repoRoot = fileURLToPath(new URL('../..', import.meta.url));

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	server: {
		fs: {
			allow: [repoRoot]
		}
	},
	test: {
		environment: 'node',
		include: ['tests/**/*.test.ts'],
		exclude: ['tests/e2e/**']
	}
});
