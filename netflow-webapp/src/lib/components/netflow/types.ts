export interface NetflowDataPoint {
	time: string;
	data: string;
}

export interface DataOption {
	label: string;
	index: number;
	checked: boolean;
}

export interface RouterConfig {
	[key: string]: boolean;
}

export type GroupByOption = 'month' | 'date' | 'hour' | '30min' | '5min';

export type ChartTypeOption = 'stacked' | 'line';

export interface ChartState {
	startDate: string;
	endDate: string;
	routers: RouterConfig;
	groupBy: GroupByOption;
	chartType: ChartTypeOption;
	dataOptions: DataOption[];
}

export interface ClickedElement {
	dataset: any;
	label: string;
	value: any;
	datasetIndex: number;
	index: number;
}

export interface ChartConfig {
	type: string;
	data: {
		labels: string[];
		datasets: any[];
	};
	options: any;
}
