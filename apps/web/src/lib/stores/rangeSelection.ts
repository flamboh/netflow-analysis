import { writable } from 'svelte/store';

export interface RangeSelectionState {
	sourceChartId: string;
	startLabel: string;
	endLabel: string;
}

export const rangeSelectionStore = writable<RangeSelectionState | null>(null);
