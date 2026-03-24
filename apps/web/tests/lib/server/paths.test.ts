import { afterEach, describe, expect, it, vi } from 'vitest';

async function loadPathsModule() {
	vi.resetModules();
	return import('../../../src/lib/server/paths');
}

describe('server paths', () => {
	afterEach(() => {
		vi.unstubAllEnvs();
	});

	it('resolves configured absolute and relative paths from repo root', async () => {
		vi.stubEnv('DATASETS_CONFIG_PATH', 'config/datasets.json');
		vi.stubEnv('NETFLOW_DB_DIR', '/tmp/netflow-db');
		vi.stubEnv('MAAD_DIR', 'vendor/custom-maad');
		vi.stubEnv('LOG_PATH', 'logs/custom');

		const paths = await loadPathsModule();

		expect(paths.getDatasetsConfigPath()).toMatch(/config\/datasets\.json$/);
		expect(paths.getNetflowDbDir()).toBe('/tmp/netflow-db');
		expect(paths.getMaadDir()).toMatch(/vendor\/custom-maad$/);
		expect(paths.getLogPath()).toMatch(/logs\/custom$/);
	});
});
