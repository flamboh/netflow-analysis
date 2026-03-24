import fs from 'fs';
import { describe, expect, it, vi } from 'vitest';
import {
	getDatasetFromRequest,
	getNetflowFilePath,
	slugToBucketStart
} from '../../src/routes/api/netflow/files/[slug]/utils';
import { GET as getIpCounts } from '../../src/routes/api/netflow/files/[slug]/ip-counts/+server';
import { GET as getSpectrum } from '../../src/routes/api/netflow/files/[slug]/spectrum/+server';
import { GET as getStructure } from '../../src/routes/api/netflow/files/[slug]/structure/+server';
import { GET as getDetails } from '../../src/routes/api/netflow/files/[slug]/details/+server';
import { GET as getSingularities } from '../../src/routes/api/netflow/files/[slug]/singularities/+server';
import { getRequestedDataset, getDatasetDb } from '$lib/server/datasets';
import { getMaadDir } from '$lib/server/paths';

vi.mock('$lib/server/datasets', () => ({
	getRequestedDataset: vi.fn(),
	getDatasetDb: vi.fn()
}));

vi.mock('$lib/server/paths', () => ({
	getMaadDir: vi.fn()
}));

vi.mock('fs', async () => {
	const actual = await vi.importActual<typeof import('fs')>('fs');
	return {
		...actual,
		default: {
			...actual,
			existsSync: vi.fn()
		},
		existsSync: vi.fn()
	};
});

vi.mock('child_process', () => ({
	exec: vi.fn(),
	promisify: undefined
}));

describe('netflow file helpers and routes', () => {
	it('parses slugs and resolves dataset from requests', async () => {
		vi.mocked(getRequestedDataset).mockReturnValue('alpha');

		expect(slugToBucketStart('202503010005')).toBe(1740816300);
		expect(slugToBucketStart('bad')).toBeNull();
		expect(getDatasetFromRequest(new URL('http://localhost/api?dataset=alpha'))).toBe('alpha');
	});

	it('looks up netflow file paths by slug + router', async () => {
		vi.mocked(getDatasetDb).mockReturnValue({
			prepare: vi.fn().mockReturnValue({
				get: vi.fn().mockReturnValue({ file_path: '/captures/r1/nfcapd.202503010005' })
			})
		} as never);

		await expect(getNetflowFilePath('alpha', '202503010005', 'r1')).resolves.toBe(
			'/captures/r1/nfcapd.202503010005'
		);
	});

	it('returns ip/spectrum/structure payloads and validation errors', async () => {
		const get = vi
			.fn()
			.mockReturnValueOnce({
				saIpv4Count: 1,
				daIpv4Count: 2,
				saIpv6Count: 3,
				daIpv6Count: 4
			})
			.mockReturnValueOnce({
				spectrumJsonSa: '[{"alpha":1,"f":2}]',
				spectrumJsonDa: '[{"alpha":3,"f":4}]'
			})
			.mockReturnValueOnce({
				structureJsonSa: '[{"q":1,"s":2}]',
				structureJsonDa: '[{"q":3,"s":4}]'
			});
		vi.mocked(getRequestedDataset).mockReturnValue('alpha');
		vi.mocked(getDatasetDb).mockReturnValue({
			prepare: vi.fn().mockReturnValue({ get })
		} as never);

		const badIpResponse = await getIpCounts({
			params: { slug: 'bad' },
			url: new URL('http://localhost/api/netflow/files/bad/ip-counts')
		} as never);
		const ipResponse = await getIpCounts({
			params: { slug: '202503010005' },
			url: new URL('http://localhost/api/netflow/files/x/ip-counts?router=r1&source=true')
		} as never);
		const spectrumResponse = await getSpectrum({
			params: { slug: '202503010005' },
			url: new URL('http://localhost/api/netflow/files/x/spectrum?router=r1&source=false')
		} as never);
		const structureResponse = await getStructure({
			params: { slug: '202503010005' },
			url: new URL('http://localhost/api/netflow/files/x/structure?router=r1&source=true')
		} as never);

		expect(badIpResponse.status).toBe(400);
		await expect(ipResponse.json()).resolves.toEqual({ ipv4Count: 1, ipv6Count: 3 });
		await expect(spectrumResponse.json()).resolves.toEqual({
			slug: '202503010005',
			router: 'r1',
			filename: 'nfcapd.202503010005',
			spectrum: [{ alpha: 3, f: 4 }],
			metadata: {
				dataSource: 'Database: spectrum_stats 5m bucket (Destination Addresses)',
				uniqueIPCount: -1,
				pointCount: 1,
				addressType: 'Destination',
				alphaRange: { min: 3, max: 3 }
			}
		});
		await expect(structureResponse.json()).resolves.toEqual({
			slug: '202503010005',
			router: 'r1',
			filename: 'nfcapd.202503010005',
			structureFunction: [{ q: 1, s: 2 }],
			metadata: {
				dataSource: 'Database: structure_stats 5m bucket (Source Addresses)',
				uniqueIPCount: -1,
				pointCount: 1,
				addressType: 'Source',
				qRange: { min: 1, max: 1 }
			}
		});
	});

	it('builds file details response with attached derived datasets', async () => {
		vi.mocked(getRequestedDataset).mockReturnValue('alpha');
		vi.mocked(getDatasetDb).mockReturnValue({
			prepare: vi.fn().mockReturnValue({
				all: vi.fn().mockReturnValue([
					{
						router: 'r1',
						file_path: '/captures/r1/nfcapd.202503010005',
						flows: 1,
						flows_tcp: 2,
						flows_udp: 3,
						flows_icmp: 4,
						flows_other: 5,
						packets: 6,
						packets_tcp: 7,
						packets_udp: 8,
						packets_icmp: 9,
						packets_other: 10,
						bytes: 11,
						bytes_tcp: 12,
						bytes_udp: 13,
						bytes_icmp: 14,
						bytes_other: 15,
						first_timestamp: 16,
						last_timestamp: 17,
						msec_first: 18,
						msec_last: 19,
						sequence_failures: 20,
						processed_at: 'now',
						saIpv4Count: 2,
						daIpv4Count: 3,
						saIpv6Count: 4,
						daIpv6Count: 5,
						structureJsonSa: '[{"q":2,"s":8}]',
						structureJsonDa: null,
						spectrumJsonSa: '[{"alpha":1.5,"f":2}]',
						spectrumJsonDa: null
					}
				])
			})
		} as never);
		vi.mocked(fs.existsSync).mockReturnValue(true);

		const response = await getDetails({
			params: { slug: '202503010005' },
			url: new URL('http://localhost/api/netflow/files/x/details?dataset=alpha')
		} as never);

		expect(response.status).toBe(200);
		await expect(response.json()).resolves.toEqual({
			routers: [
				{
					summary: {
						router: 'r1',
						file_path: '/captures/r1/nfcapd.202503010005',
						file_exists_on_disk: true,
						flows: 1,
						flows_tcp: 2,
						flows_udp: 3,
						flows_icmp: 4,
						flows_other: 5,
						packets: 6,
						packets_tcp: 7,
						packets_udp: 8,
						packets_icmp: 9,
						packets_other: 10,
						bytes: 11,
						bytes_tcp: 12,
						bytes_udp: 13,
						bytes_icmp: 14,
						bytes_other: 15,
						first_timestamp: 16,
						last_timestamp: 17,
						msec_first: 18,
						msec_last: 19,
						sequence_failures: 20,
						processed_at: 'now'
					},
					ipCountsSource: { ipv4Count: 2, ipv6Count: 4 },
					ipCountsDestination: { ipv4Count: 3, ipv6Count: 5 },
					structureSource: {
						slug: '202503010005',
						router: 'r1',
						filename: 'nfcapd.202503010005',
						structureFunction: [{ q: 2, s: 8 }],
						metadata: {
							dataSource: 'Database: structure_stats 5m bucket (Source Addresses)',
							uniqueIPCount: -1,
							pointCount: 1,
							addressType: 'Source',
							qRange: { min: 2, max: 2 }
						}
					},
					structureDestination: null,
					spectrumSource: {
						slug: '202503010005',
						router: 'r1',
						filename: 'nfcapd.202503010005',
						spectrum: [{ alpha: 1.5, f: 2 }],
						metadata: {
							dataSource: 'Database: spectrum_stats 5m bucket (Source Addresses)',
							uniqueIPCount: -1,
							pointCount: 1,
							addressType: 'Source',
							alphaRange: { min: 1.5, max: 1.5 }
						}
					},
					spectrumDestination: null
				}
			]
		});
	});

	it('handles singularities validation and missing-file failures', async () => {
		vi.mocked(getRequestedDataset).mockReturnValue('alpha');
		vi.mocked(getDatasetDb).mockReturnValue({
			prepare: vi.fn().mockReturnValue({
				get: vi.fn().mockReturnValue({ file_path: '/captures/r1/nfcapd.202503010005' })
			})
		} as never);
		vi.mocked(getMaadDir).mockReturnValue('/tmp/maad');
		vi.mocked(fs.existsSync).mockReturnValue(false);

		const badResponse = await getSingularities({
			params: { slug: '202503010005' },
			url: new URL('http://localhost/api/netflow/files/x/singularities?router=r1&source=maybe')
		} as never);
		const missingFileResponse = await getSingularities({
			params: { slug: '202503010005' },
			url: new URL('http://localhost/api/netflow/files/x/singularities?router=r1&source=true')
		} as never);

		expect(badResponse.status).toBe(400);
		expect(missingFileResponse.status).toBe(404);
	});
});
