export interface TimeRange {
	start: number;
	end: number;
}

interface CacheEntry<T> {
	records: T[];
	segments: TimeRange[];
	inflight: Map<string, Promise<T[]>>;
}

const cache = new Map<string, CacheEntry<unknown>>();

function getEntry<T>(key: string): CacheEntry<T> {
	const existing = cache.get(key) as CacheEntry<T> | undefined;
	if (existing) {
		return existing;
	}

	const created: CacheEntry<T> = {
		records: [],
		segments: [],
		inflight: new Map()
	};
	cache.set(key, created as CacheEntry<unknown>);
	return created;
}

function normalizeRange(range: TimeRange): TimeRange {
	return {
		start: Math.min(range.start, range.end),
		end: Math.max(range.start, range.end)
	};
}

function mergeSegments(segments: TimeRange[]): TimeRange[] {
	if (segments.length === 0) {
		return [];
	}

	const sorted = segments
		.map(normalizeRange)
		.filter((segment) => segment.start < segment.end)
		.sort((a, b) => a.start - b.start);

	if (sorted.length === 0) {
		return [];
	}

	const merged: TimeRange[] = [{ ...sorted[0] }];
	for (let index = 1; index < sorted.length; index += 1) {
		const segment = sorted[index];
		const last = merged[merged.length - 1];
		if (segment.start <= last.end) {
			last.end = Math.max(last.end, segment.end);
			continue;
		}
		merged.push({ ...segment });
	}

	return merged;
}

function rangeKey(range: TimeRange): string {
	return `${range.start}:${range.end}`;
}

export function getMissingWindowRanges(key: string, requestedRange: TimeRange): TimeRange[] {
	const entry = getEntry(key);
	const request = normalizeRange(requestedRange);
	if (request.start === request.end) {
		return [];
	}

	const covered = mergeSegments(entry.segments);
	if (covered.length === 0) {
		return [request];
	}

	const missing: TimeRange[] = [];
	let cursor = request.start;

	for (const segment of covered) {
		if (segment.end <= cursor) {
			continue;
		}
		if (segment.start >= request.end) {
			break;
		}
		if (segment.start > cursor) {
			missing.push({ start: cursor, end: Math.min(segment.start, request.end) });
		}
		cursor = Math.max(cursor, segment.end);
		if (cursor >= request.end) {
			break;
		}
	}

	if (cursor < request.end) {
		missing.push({ start: cursor, end: request.end });
	}

	return missing.filter((segment) => segment.start < segment.end);
}

export function readCachedWindow<T>(
	key: string,
	requestedRange: TimeRange,
	matchesRange: (record: T, requestedRange: TimeRange) => boolean
): T[] {
	const entry = getEntry<T>(key);
	return entry.records.filter((record) => matchesRange(record, requestedRange));
}

export async function ensureCachedWindow<T>(options: {
	key: string;
	requestedRange: TimeRange;
	fetchRange: (range: TimeRange) => Promise<T[]>;
	getRecordKey: (record: T) => string;
	compareRecords: (left: T, right: T) => number;
}): Promise<void> {
	const entry = getEntry<T>(options.key);
	const missingRanges = getMissingWindowRanges(options.key, options.requestedRange);

	const fetchedRanges = await Promise.all(
		missingRanges.map(async (range) => {
			const key = rangeKey(range);
			const inflight = entry.inflight.get(key);
			if (inflight) {
				return await inflight;
			}

			const request = options.fetchRange(range);

			entry.inflight.set(key, request);
			try {
				return await request;
			} finally {
				entry.inflight.delete(key);
			}
		})
	);

	if (fetchedRanges.length === 0) {
		return;
	}

	const merged = new Map(entry.records.map((record) => [options.getRecordKey(record), record]));
	fetchedRanges.flat().forEach((record) => {
		merged.set(options.getRecordKey(record), record);
	});

	entry.records = [...merged.values()].sort(options.compareRecords);
	entry.segments = mergeSegments([...entry.segments, ...missingRanges]);
}
