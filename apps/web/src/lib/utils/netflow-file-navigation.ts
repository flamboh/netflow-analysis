import type { goto } from '$app/navigation';
import { resolve } from '$app/paths';

export function buildNetflowFileSearch(dataset?: string): string {
	const normalizedDataset = dataset?.trim();

	if (!normalizedDataset) {
		return '';
	}

	return `?dataset=${encodeURIComponent(normalizedDataset)}`;
}

export function buildNetflowFileHref(slug: string, dataset?: string): string {
	const pathname = resolve('/netflow/files/[slug]', { slug });
	return `${pathname}${buildNetflowFileSearch(dataset)}`;
}

export function navigateToNetflowFile(
	navigate: typeof goto,
	slug: string,
	dataset?: string
): Promise<void> {
	return navigate(buildNetflowFileHref(slug, dataset));
}
