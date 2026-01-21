import type { Chart } from 'chart.js';

/**
 * Centralized crosshair store that manages synchronized crosshairs across multiple charts.
 * When one chart is hovered, this store triggers redraws on all other registered charts
 * so they can display crosshairs at the same x-axis position.
 */
class CrosshairStore {
	private charts = new Map<string, Chart>();
	private _hoveredLabel: string | null = null;
	private _sourceChartId: string | null = null;

	/**
	 * Register a chart instance with the store.
	 * Call this when the chart is created/mounted.
	 */
	register(id: string, chart: Chart): void {
		this.charts.set(id, chart);
	}

	/**
	 * Unregister a chart instance from the store.
	 * Call this when the chart is destroyed/unmounted.
	 */
	unregister(id: string): void {
		this.charts.delete(id);
	}

	/**
	 * Update the hover state and trigger redraws on all other charts.
	 * Called by the crosshair plugin when mouse moves over a chart.
	 */
	setHover(label: string | null, sourceId: string): void {
		this._hoveredLabel = label;
		this._sourceChartId = sourceId;

		// Directly redraw all OTHER charts - no reactive overhead
		this.charts.forEach((chart, id) => {
			if (id !== sourceId) {
				chart.draw();
			}
		});
	}

	/**
	 * Clear the hover state and redraw all charts.
	 * Called when mouse leaves a chart entirely.
	 */
	clearHover(): void {
		this._hoveredLabel = null;
		this._sourceChartId = null;

		// Redraw all charts to clear crosshairs
		this.charts.forEach((chart) => {
			chart.draw();
		});
	}

	/**
	 * Get the external hover label for a specific chart.
	 * Returns the hovered label if this chart is NOT the source of the hover,
	 * otherwise returns null (so the chart uses its own local crosshair).
	 */
	getExternalLabel(chartId: string): string | null {
		if (this._sourceChartId !== chartId) {
			return this._hoveredLabel;
		}
		return null;
	}

	/**
	 * Get the current hovered label (for debugging/inspection).
	 */
	get hoveredLabel(): string | null {
		return this._hoveredLabel;
	}

	/**
	 * Get the current source chart ID (for debugging/inspection).
	 */
	get sourceChartId(): string | null {
		return this._sourceChartId;
	}
}

// Export singleton instance
export const crosshairStore = new CrosshairStore();
