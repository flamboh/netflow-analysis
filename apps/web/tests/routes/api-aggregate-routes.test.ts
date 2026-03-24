import { describe, expect, it, vi } from 'vitest';
import { GET as getRouters } from '../../src/routes/api/routers/+server';
import { GET as getIpStats } from '../../src/routes/api/ip/stats/+server';
import { GET as getProtocolStats } from '../../src/routes/api/protocol/stats/+server';
import { GET as getSpectrumStats } from '../../src/routes/api/netflow/spectrum-stats/+server';
import { GET as getStructureStats } from '../../src/routes/api/netflow/structure-stats/+server';
import { getDatasetDb, getRequestedDataset, listDatasetSources } from '$lib/server/datasets';

vi.mock('$lib/server/datasets', () => ({
	getDatasetDb: vi.fn(),
	getRequestedDataset: vi.fn(),
	listDatasetSources: vi.fn()
}));

describe('aggregate API routes', () => {
	it('lists routers for a dataset and returns 404 when none exist', async () => {
		vi.mocked(getRequestedDataset).mockReturnValue('alpha');
		vi.mocked(listDatasetSources).mockReturnValueOnce(['r1', 'r2']).mockReturnValueOnce([]);

		const okResponse = await getRouters({
			url: new URL('http://localhost/api/routers?dataset=alpha')
		} as never);
		const emptyResponse = await getRouters({
			url: new URL('http://localhost/api/routers?dataset=alpha')
		} as never);

		await expect(okResponse.json()).resolves.toEqual(['r1', 'r2']);
		expect(emptyResponse.status).toBe(404);
		await expect(emptyResponse.json()).resolves.toEqual({
			error: "No routers available for dataset 'alpha'"
		});
	});

	it('validates ip stats requests and returns grouped data', async () => {
		const all = vi.fn().mockReturnValue([
			{
				router: 'r1',
				bucketStart: 100,
				bucketEnd: 200,
				saIpv4Count: 3,
				daIpv4Count: 4,
				saIpv6Count: 5,
				daIpv6Count: 6,
				processedAt: 'now'
			}
		]);
		vi.mocked(getRequestedDataset).mockReturnValue('alpha');
		vi.mocked(getDatasetDb).mockReturnValue({
			prepare: vi.fn().mockReturnValue({ all })
		} as never);

		const badResponse = await getIpStats({
			url: new URL('http://localhost/api/ip/stats?routers=&startDate=1&endDate=2')
		} as never);
		const okResponse = await getIpStats({
			url: new URL(
				'http://localhost/api/ip/stats?routers=r1&granularity=5m&startDate=100&endDate=200'
			)
		} as never);

		expect(badResponse.status).toBe(400);
		await expect(okResponse.json()).resolves.toEqual({
			buckets: [
				{
					router: 'r1',
					bucketStart: 100,
					bucketEnd: 200,
					saIpv4Count: 3,
					daIpv4Count: 4,
					saIpv6Count: 5,
					daIpv6Count: 6,
					processedAt: 'now',
					granularity: '5m'
				}
			],
			availableGranularities: ['5m', '30m', '1h', '1d'],
			requestedRouters: ['r1']
		});
	});

	it('maps protocol unknown-dataset errors to 400', async () => {
		vi.mocked(getRequestedDataset).mockImplementation(() => {
			throw new Error("Unknown dataset 'bad'");
		});

		const response = await getProtocolStats({
			url: new URL(
				'http://localhost/api/protocol/stats?routers=r1&startDate=100&endDate=200'
			)
		} as never);

		expect(response.status).toBe(400);
		await expect(response.json()).resolves.toEqual({ error: "Unknown dataset 'bad'" });
	});

	it('parses spectrum and structure json payloads, tolerating bad json', async () => {
		const all = vi
			.fn()
			.mockReturnValueOnce([
				{
					router: 'r1',
					bucketStart: 100,
					spectrumJsonSa: '[{"alpha":1,"f":2}]',
					spectrumJsonDa: 'not-json'
				}
			])
			.mockReturnValueOnce([
				{
					router: 'r1',
					bucketStart: 100,
					structureJsonSa: '[{"q":1,"s":2}]',
					structureJsonDa: 'not-json'
				}
			]);
		vi.mocked(getRequestedDataset).mockReturnValue('alpha');
		vi.mocked(getDatasetDb).mockReturnValue({
			prepare: vi.fn().mockReturnValue({ all })
		} as never);

		const spectrumResponse = await getSpectrumStats({
			url: new URL(
				'http://localhost/api/netflow/spectrum-stats?routers=r1&startDate=100&endDate=200'
			)
		} as never);
		const structureResponse = await getStructureStats({
			url: new URL(
				'http://localhost/api/netflow/structure-stats?routers=r1&startDate=100&endDate=200'
			)
		} as never);

		await expect(spectrumResponse.json()).resolves.toEqual({
			buckets: [{ bucketStart: 100, router: 'r1', spectrumSa: [{ alpha: 1, f: 2 }], spectrumDa: [] }],
			requestedRouters: ['r1']
		});
		await expect(structureResponse.json()).resolves.toEqual({
			buckets: [
				{ bucketStart: 100, router: 'r1', structureSa: [{ q: 1, s: 2 }], structureDa: [] }
			],
			requestedRouters: ['r1']
		});
	});
});
