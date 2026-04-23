import { describe, expect, it } from 'vitest';
import {
	assertNetflowV2Database,
	getNetflowSchemaVersion
} from '../../../src/lib/server/netflow-v2';
import type { ReadonlyDatasetDb } from '../../../src/lib/server/datasets';

const validSchema: Record<string, string[]> = {
	processed_inputs_v2: [
		'input_kind',
		'input_locator',
		'source_id',
		'bucket_start',
		'bucket_end',
		'status',
		'error_message',
		'discovered_at',
		'processed_at'
	],
	netflow_stats_v2: [
		'source_id',
		'bucket_start',
		'bucket_end',
		'ip_version',
		'flows',
		'flows_tcp',
		'flows_udp',
		'flows_icmp',
		'flows_other',
		'packets',
		'packets_tcp',
		'packets_udp',
		'packets_icmp',
		'packets_other',
		'bytes',
		'bytes_tcp',
		'bytes_udp',
		'bytes_icmp',
		'bytes_other',
		'processed_at'
	],
	ip_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'sa_ipv4_count',
		'da_ipv4_count',
		'sa_ipv6_count',
		'da_ipv6_count',
		'processed_at'
	],
	protocol_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'unique_protocols_count_ipv4',
		'unique_protocols_count_ipv6',
		'protocols_list_ipv4',
		'protocols_list_ipv6',
		'processed_at'
	],
	structure_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'ip_version',
		'structure_json_sa',
		'structure_json_da',
		'metadata_json_sa',
		'metadata_json_da',
		'processed_at'
	],
	spectrum_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'ip_version',
		'spectrum_json_sa',
		'spectrum_json_da',
		'metadata_json_sa',
		'metadata_json_da',
		'processed_at'
	],
	dimension_stats_v2: [
		'source_id',
		'granularity',
		'bucket_start',
		'bucket_end',
		'ip_version',
		'dimensions_json_sa',
		'dimensions_json_da',
		'metadata_json_sa',
		'metadata_json_da',
		'processed_at'
	]
};

function createSchemaDb(schema: Record<string, string[]>): ReadonlyDatasetDb {
	return {
		prepare(sql: string) {
			return {
				get: () => undefined,
				all: () => {
					if (sql.includes('sqlite_master')) {
						return Object.keys(schema).map((name) => ({ name }));
					}

					const match = /PRAGMA table_info\(([^)]+)\)/.exec(sql);
					if (match) {
						return (schema[match[1]] ?? []).map((name) => ({ name }));
					}

					return [];
				}
			};
		},
		close() {}
	};
}

describe('netflow v2 schema contract', () => {
	it('accepts the current pipeline v2 schema', () => {
		expect(() => assertNetflowV2Database(createSchemaDb(validSchema))).not.toThrow();
		expect(getNetflowSchemaVersion(createSchemaDb(validSchema))).toBe('v2');
	});

	it('detects legacy v1 databases while v1 is still viewable', () => {
		expect(getNetflowSchemaVersion(createSchemaDb({ netflow_stats: ['router'] }))).toBe('v1');
	});

	it('fails loudly when required tables or columns are missing', () => {
		const missingTable = { ...validSchema };
		delete missingTable.dimension_stats_v2;

		expect(() => assertNetflowV2Database(createSchemaDb(missingTable))).toThrow(
			/Missing tables: dimension_stats_v2/
		);

		expect(() =>
			assertNetflowV2Database(
				createSchemaDb({
					...validSchema,
					processed_inputs_v2: validSchema.processed_inputs_v2.filter(
						(column) => column !== 'status'
					)
				})
			)
		).toThrow(/processed_inputs_v2 is missing v2 columns: status/);
	});
});
