import { describe, expect, it, vi } from 'vitest';
import { load } from '../../src/routes/datasets/[dataset]/+page';

describe('/datasets/[dataset] load', () => {
	it('returns the dataset page props from dataset metadata', async () => {
		const fetch = vi.fn().mockResolvedValue({
			ok: true,
			status: 200,
			json: async () => ({
				data: [
					{
						datasetId: 'uoregon',
						label: 'UONet-in',
						defaultStartDate: '2025-02-11',
						discoveryMode: 'live',
						sourceCount: 2,
						isDefault: true
					}
				],
				error: null
			})
		});

		const result = await load({
			params: { dataset: 'uoregon' },
			fetch
		} as never);

		expect(fetch).toHaveBeenCalledWith('/api/datasets');
		expect(result).toEqual({
			datasetId: 'uoregon',
			title: 'UONet-in',
			defaultStartDate: '2025-02-11'
		});
	});
});
