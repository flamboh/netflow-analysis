import { describe, expect, it, vi } from 'vitest';

vi.mock('$app/paths', () => ({
	resolve: (_routeId: string, params: Record<string, string>) => `/netflow/files/${params.slug}`
}));

describe('buildNetflowFileHref', () => {
	it('builds only the dataset search when provided', async () => {
		const { buildNetflowFileSearch } = await import('$lib/utils/netflow-file-navigation');

		expect(buildNetflowFileSearch('uoregon')).toBe('?dataset=uoregon');
	});

	it('includes dataset query when provided', async () => {
		const { buildNetflowFileHref } = await import('$lib/utils/netflow-file-navigation');

		expect(buildNetflowFileHref('202506192010', 'uoregon')).toBe(
			'/netflow/files/202506192010?dataset=uoregon'
		);
	});

	it('omits query when dataset is empty', async () => {
		const { buildNetflowFileHref, buildNetflowFileSearch } =
			await import('$lib/utils/netflow-file-navigation');

		expect(buildNetflowFileHref('202506192010', '')).toBe('/netflow/files/202506192010');
		expect(buildNetflowFileSearch('')).toBe('');
	});

	it('delegates navigation to the built href', async () => {
		const { navigateToNetflowFile } = await import('$lib/utils/netflow-file-navigation');
		const navigate = vi.fn().mockResolvedValue(undefined);

		await navigateToNetflowFile(navigate, '202506192010', 'uoregon');

		expect(navigate).toHaveBeenCalledWith('/netflow/files/202506192010?dataset=uoregon');
	});
});
