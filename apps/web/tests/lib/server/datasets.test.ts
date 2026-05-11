import os from 'os';
import path from 'path';
import { spawnSync } from 'child_process';
import { afterEach, describe, expect, it, vi } from 'vitest';

async function loadDatasetsModule() {
	vi.resetModules();
	return import('../../../src/lib/server/datasets');
}

function createSqliteFixture(): string {
	const tempDir = os.tmpdir();
	const dbPath = path.join(tempDir, `datasets-test-${crypto.randomUUID()}.sqlite`);
	const seedResult = spawnSync(
		'sqlite3',
		[
			dbPath,
			`
				CREATE TABLE datasets (
					id TEXT PRIMARY KEY NOT NULL,
					label TEXT NOT NULL,
					default_start_date TEXT NOT NULL,
					source_mode TEXT DEFAULT 'static' NOT NULL,
					discovery_mode TEXT DEFAULT 'static' NOT NULL,
					sort_order INTEGER DEFAULT 0 NOT NULL
				);
				CREATE TABLE netflow_stats_v2 (
					source_id TEXT NOT NULL,
					bucket_start INTEGER NOT NULL,
					ip_version INTEGER NOT NULL
				);
				INSERT INTO datasets (
					id,
					label,
					default_start_date,
					source_mode,
					discovery_mode,
					sort_order
				) VALUES ('alpha', 'Alpha Label', '2025-03-01', 'static', 'static', 0);
				INSERT INTO netflow_stats_v2 (source_id, bucket_start, ip_version)
				VALUES ('router-b', 1740823200, 4), ('router-a', 1740823200, 4);
			`
		],
		{ encoding: 'utf-8' }
	);
	expect(seedResult.status, seedResult.stderr).toBe(0);
	return dbPath;
}

describe('dataset server helpers', () => {
	afterEach(() => {
		vi.unstubAllEnvs();
	});

	it('lists dataset summaries from local sqlite metadata', async () => {
		vi.stubEnv('LOCAL_SQLITE_PATH', createSqliteFixture());
		vi.stubEnv('DEFAULT_DATASET', 'alpha');

		const datasets = await loadDatasetsModule();

		await expect(datasets.listDatasetSummaries()).resolves.toEqual([
			{
				datasetId: 'alpha',
				label: 'Alpha Label',
				defaultStartDate: '2025-03-01',
				discoveryMode: 'static',
				sourceCount: 2,
				isDefault: true
			}
		]);
		await expect(datasets.listDatasetSources('alpha')).resolves.toEqual(['router-a', 'router-b']);
		await expect(
			datasets.getRequestedDataset(new URL('http://localhost/api?dataset=alpha'))
		).resolves.toBe('alpha');
	});

	it('rejects unknown datasets', async () => {
		vi.stubEnv('LOCAL_SQLITE_PATH', createSqliteFixture());

		const datasets = await loadDatasetsModule();

		await expect(datasets.getDatasetConfig('missing')).rejects.toThrow(/Unknown dataset 'missing'/);
	});
});
