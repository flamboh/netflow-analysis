import { createDateFromPSTComponents } from '$lib/utils/timezone';
import { getDatasetDb, getRequestedDataset } from '$lib/server/datasets';

export interface NetflowRecord {
	router: string;
	file_path: string;
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
	const filePattern = `nfcapd.${slug}`;
	const query = `
		SELECT file_path FROM netflow_stats
		WHERE file_path LIKE '%' || ? AND router = ?
		LIMIT 1
	`;

	const database = getDb(dataset);
	const result = database.prepare(query).get(filePattern, router) as NetflowRecord | undefined;

	return result?.file_path || null;
}

export function getDatasetFromRequest(url: URL): string {
	return getRequestedDataset(url);
}
