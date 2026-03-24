import fs from 'fs';
import os from 'os';
import path from 'path';
import { spawnSync } from 'child_process';
import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('better-sqlite3', () => ({
	default: class MockDatabase {
		constructor(private readonly dbPath: string) {}

		prepare(query: string) {
			return {
				get: () => {
					const result = spawnSync('sqlite3', [this.dbPath, query], { encoding: 'utf-8' });
					if (result.status !== 0) {
						throw new Error(result.stderr || 'sqlite3 query failed');
					}

					const minTimestamp = Number(result.stdout.trim());
					return { minTimestamp: Number.isFinite(minTimestamp) ? minTimestamp : null };
				}
			};
		}

		close() {}
	}
}));

async function loadDatasetsModule() {
	vi.resetModules();
	return import('../../../src/lib/server/datasets');
}

describe('dataset server helpers', () => {
	afterEach(() => {
		vi.unstubAllEnvs();
	});

	it('lists dataset summaries from registry + sqlite min timestamp', async () => {
		const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'datasets-test-'));
		const dbPath = path.join(tempDir, 'netflow.sqlite');
		const registryPath = path.join(tempDir, 'datasets.json');

		const seedResult = spawnSync(
			'sqlite3',
			[
				dbPath,
				'CREATE TABLE netflow_stats (timestamp INTEGER NOT NULL); INSERT INTO netflow_stats (timestamp) VALUES (1740823200);'
			],
			{ encoding: 'utf-8' }
		);
		expect(seedResult.status).toBe(0);

		fs.mkdirSync(path.join(tempDir, 'captures', 'router-b'), { recursive: true });
		fs.mkdirSync(path.join(tempDir, 'captures', 'router-a'), { recursive: true });
		fs.writeFileSync(
			registryPath,
			JSON.stringify({
				datasets: [
					{
						dataset_id: 'alpha',
						label: 'Alpha Label',
						root_path: path.join(tempDir, 'captures'),
						db_path: dbPath,
						discovery_mode: 'live'
					}
				]
			})
		);

		vi.stubEnv('DATASETS_CONFIG_PATH', registryPath);
		vi.stubEnv('DEFAULT_DATASET', 'alpha');

		const datasets = await loadDatasetsModule();

		expect(datasets.listDatasetSummaries()).toEqual([
			{
				datasetId: 'alpha',
				label: 'Alpha Label',
				defaultStartDate: '2025-03-01',
				discoveryMode: 'live',
				sourceCount: 2,
				isDefault: true
			}
		]);
		expect(datasets.listDatasetSources('alpha')).toEqual(['router-a', 'router-b']);
		expect(datasets.getRequestedDataset(new URL('http://localhost/api?dataset=alpha'))).toBe(
			'alpha'
		);
	});

	it('falls back when sqlite db is missing and rejects unknown datasets', async () => {
		const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'datasets-test-'));
		const registryPath = path.join(tempDir, 'datasets.json');

		fs.writeFileSync(
			registryPath,
			JSON.stringify([
				{
					dataset_id: 'alpha',
					label: 'Alpha Label',
					root_path: path.join(tempDir, 'captures'),
					db_path: path.join(tempDir, 'missing.sqlite'),
					source_ids: ['r2', 'r1']
				}
			])
		);

		vi.stubEnv('DATASETS_CONFIG_PATH', registryPath);

		const datasets = await loadDatasetsModule();
		const startDate = datasets.getDatasetDefaultStartDate('alpha');

		expect(startDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
		expect(datasets.listDatasetSources('alpha')).toEqual(['r1', 'r2']);
		expect(() => datasets.getDatasetConfig('missing')).toThrow(/Unknown dataset 'missing'/);
	});
});
