import { createDateFromPSTComponents } from '$lib/utils/timezone';
import { getDatasetDb, getRequestedDataset } from '$lib/server/datasets';
import { getNetflowSchemaVersion } from '$lib/server/netflow-v2';

export interface NetflowRecord {
	router: string;
	input_locator: string;
}

export function getDb(dataset: string) {
	return getDatasetDb(dataset);
}

export function slugToBucketStart(slug: string): number | null {
	if (slug.length !== 12) {
		return null;
	}

	const year = Number(slug.slice(0, 4));
	const month = Number(slug.slice(4, 6));
	const day = Number(slug.slice(6, 8));
	const hour = Number(slug.slice(8, 10));
	const minute = Number(slug.slice(10, 12));

	if (
		Number.isNaN(year) ||
		Number.isNaN(month) ||
		Number.isNaN(day) ||
		Number.isNaN(hour) ||
		Number.isNaN(minute)
	) {
		return null;
	}

	const date = createDateFromPSTComponents(year, month, day, hour, minute);
	return Math.floor(date.getTime() / 1000);
}

export async function getNetflowFilePath(
	dataset: string,
	slug: string,
	router: string
): Promise<string | null> {
	const bucketStart = slugToBucketStart(slug);
	if (bucketStart === null) {
		return null;
	}

	const query = `
		SELECT input_locator FROM processed_inputs_v2
		WHERE source_id = ? AND bucket_start = ? AND input_kind = 'nfcapd'
		LIMIT 1
	`;

	const database = getDb(dataset);
	const schema = getNetflowSchemaVersion(database);
	if (schema === 'v2') {
		const result = database.prepare(query).get(router, bucketStart) as NetflowRecord | undefined;
		return result?.input_locator || null;
	}

	const filePattern = `nfcapd.${slug}`;
	const result = database
		.prepare(
			`
			SELECT file_path AS input_locator FROM netflow_stats
			WHERE file_path LIKE '%' || ? AND router = ?
			LIMIT 1
		`
		)
		.get(filePattern, router) as NetflowRecord | undefined;

	return result?.input_locator || null;
}

export function getDatasetFromRequest(url: URL): string {
	return getRequestedDataset(url);
}
