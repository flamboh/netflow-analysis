import type {
	ChartState,
	NetflowDataPoint,
	GroupByOption,
	ChartTypeOption,
	DataOption,
	RouterConfig
} from '$lib/components/netflow/types.ts';

class NetflowStore {
	private _state = $state<ChartState>({
		startDate: '2024-03-01',
		endDate: new Date().toJSON().slice(0, 10),
		routers: {
			'cc-ir1-gw': true,
			'oh-ir1-gw': true
		},
		groupBy: 'date',
		chartType: 'stacked',
		dataOptions: [
			{ label: 'Flows', index: 0, checked: true },
			{ label: 'Flows TCP', index: 1, checked: true },
			{ label: 'Flows UDP', index: 2, checked: true },
			{ label: 'Flows ICMP', index: 3, checked: true },
			{ label: 'Flows Other', index: 4, checked: true },
			{ label: 'Packets', index: 5, checked: false },
			{ label: 'Packets TCP', index: 6, checked: false },
			{ label: 'Packets UDP', index: 7, checked: false },
			{ label: 'Packets ICMP', index: 8, checked: false },
			{ label: 'Packets Other', index: 9, checked: false },
			{ label: 'Bytes', index: 10, checked: false },
			{ label: 'Bytes TCP', index: 11, checked: false },
			{ label: 'Bytes UDP', index: 12, checked: false },
			{ label: 'Bytes ICMP', index: 13, checked: false },
			{ label: 'Bytes Other', index: 14, checked: false }
		]
	});

	private _results = $state<NetflowDataPoint[]>([]);
	private _loading = $state(false);
	private _error = $state<string | null>(null);

	// Getters
	get state() {
		return this._state;
	}

	get results() {
		return this._results;
	}

	get loading() {
		return this._loading;
	}

	get error() {
		return this._error;
	}

	// Individual state getters
	get startDate() {
		return this._state.startDate;
	}

	get endDate() {
		return this._state.endDate;
	}

	get routers() {
		return this._state.routers;
	}

	get groupBy() {
		return this._state.groupBy;
	}

	get chartType() {
		return this._state.chartType;
	}

	get dataOptions() {
		return this._state.dataOptions;
	}

	// Computed values
	get dataOptionsBinary() {
		return this._state.dataOptions.reduce(
			(acc, curr) => acc + (curr.checked ? 1 : 0) * Math.pow(2, curr.index),
			0
		);
	}

	get activeRouters() {
		return Object.entries(this._state.routers)
			.filter(([, active]) => active)
			.map(([router]) => router)
			.join(',');
	}

	get hasData() {
		return this._results.length > 0;
	}

	// State mutations
	setStartDate(date: string) {
		this._state.startDate = date;
	}

	setEndDate(date: string) {
		this._state.endDate = date;
	}

	setDateRange(startDate: string, endDate: string) {
		this._state.startDate = startDate;
		this._state.endDate = endDate;
	}

	setRouters(routers: RouterConfig) {
		this._state.routers = routers;
	}

	toggleRouter(routerName: keyof RouterConfig) {
		this._state.routers[routerName] = !this._state.routers[routerName];
	}

	setGroupBy(groupBy: GroupByOption) {
		this._state.groupBy = groupBy;
	}

	setChartType(chartType: ChartTypeOption) {
		this._state.chartType = chartType;
	}

	setDataOptions(dataOptions: DataOption[]) {
		this._state.dataOptions = dataOptions;
	}

	updateDataOption(index: number, checked: boolean) {
		const option = this._state.dataOptions[index];
		if (option) {
			option.checked = checked;
		}
	}

	selectMetricType(type: 'flows' | 'packets' | 'bytes') {
		this._state.dataOptions.forEach((option) => {
			const isSelected =
				type === 'flows'
					? option.label.toLowerCase().includes('flow')
					: type === 'packets'
						? option.label.toLowerCase().includes('packet')
						: option.label.toLowerCase().includes('byte');
			option.checked = isSelected;
		});
	}

	selectAllMetrics() {
		this._state.dataOptions.forEach((option) => {
			option.checked = true;
		});
	}

	selectNoMetrics() {
		this._state.dataOptions.forEach((option) => {
			option.checked = false;
		});
	}

	// Data management
	setResults(results: NetflowDataPoint[]) {
		this._results = results;
	}

	setLoading(loading: boolean) {
		this._loading = loading;
	}

	setError(error: string | null) {
		this._error = error;
	}

	// Complex actions
	drillDown(newGroupBy: GroupByOption, newStartDate: string, newEndDate: string) {
		this._state.groupBy = newGroupBy;
		this._state.startDate = newStartDate;
		this._state.endDate = newEndDate;
	}

	reset() {
		this._state.groupBy = 'date';
		this._state.startDate = '2024-03-01';
		this._state.endDate = new Date().toJSON().slice(0, 10);
	}

	// API integration
	async loadData() {
		this._loading = true;
		this._error = null;

		try {
			const response = await fetch(
				'/api/netflow/stats?startDate=' +
					Math.floor(new Date(this._state.startDate).getTime() / 1000) +
					'&endDate=' +
					Math.floor(new Date(this._state.endDate).getTime() / 1000) +
					'&routers=' +
					this.activeRouters +
					'&dataOptions=' +
					this.dataOptionsBinary +
					'&groupBy=' +
					this._state.groupBy,
				{
					method: 'GET',
					headers: {
						'Content-Type': 'application/json'
					}
				}
			);

			if (response.ok) {
				const res = await response.json();
				this._results = res.result;
			} else {
				this._error = `Failed to load data: ${response.status} ${response.statusText}`;
			}
		} catch (err) {
			this._error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`;
		} finally {
			this._loading = false;
		}
	}
}

// Create and export the store instance
export const netflowStore = new NetflowStore();
