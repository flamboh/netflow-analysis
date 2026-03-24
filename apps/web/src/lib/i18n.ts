const messages = {
	'error.404.return_to_prefix': 'Return to the ',
	'error.404.dataset_index': 'dataset index',
	'error.404.or_use_prefix': ' or use the ',
	'error.404.file_lookup': 'file lookup page',
	'error.404.timestamp_hint': ' if you know the exact timestamp slug.'
} as const;

export type MessageKey = keyof typeof messages;

export function t(key: MessageKey): string {
	return messages[key];
}
