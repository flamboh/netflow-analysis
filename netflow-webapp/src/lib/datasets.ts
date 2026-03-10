export type DatasetSummary = {
	datasetId: string;
	label: string;
	defaultStartDate: string;
	discoveryMode: string;
	sourceCount: number;
};

let cachedDatasetSummaries: DatasetSummary[] | null = null;
let pendingDatasetSummariesRequest: Promise<DatasetSummary[]> | null = null;

export function getCachedDatasetSummaries(): DatasetSummary[] | null {
	return cachedDatasetSummaries;
}

export async function loadDatasetSummaries(): Promise<DatasetSummary[]> {
	if (cachedDatasetSummaries) {
		return cachedDatasetSummaries;
	}

	if (pendingDatasetSummariesRequest) {
		return pendingDatasetSummariesRequest;
	}

	pendingDatasetSummariesRequest = fetch('/api/datasets')
		.then(async (response) => {
			if (!response.ok) {
				throw new Error(`Failed to load datasets: ${response.statusText}`);
			}
			return (await response.json()) as DatasetSummary[];
		})
		.then((datasets) => {
			cachedDatasetSummaries = datasets;
			return datasets;
		})
		.finally(() => {
			pendingDatasetSummariesRequest = null;
		});

	return pendingDatasetSummariesRequest;
}
