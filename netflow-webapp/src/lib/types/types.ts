export interface NetflowStatsRow {
	date: string;
	flows?: number;
	flows_tcp?: number;
	flows_udp?: number;
	flows_icmp?: number;
	flows_other?: number;
	packets?: number;
	packets_tcp?: number;
	packets_udp?: number;
	packets_icmp?: number;
	packets_other?: number;
	bytes?: number;
	bytes_tcp?: number;
	bytes_udp?: number;
	bytes_icmp?: number;
	bytes_other?: number;
}

export interface NetflowStatsResult {
	time: string;
	data: string;
}

export const IP_GRANULARITIES = ['5m', '30m', '1h', '1d'] as const;

export type IpGranularity = (typeof IP_GRANULARITIES)[number];

export type IpMetricKey = 'saIpv4Count' | 'daIpv4Count' | 'saIpv6Count' | 'daIpv6Count';

export interface IpStatsCounts {
	saIpv4Count: number;
	daIpv4Count: number;
	saIpv6Count: number;
	daIpv6Count: number;
}

export interface IpStatsBucket extends IpStatsCounts {
	granularity: IpGranularity;
	bucketStart: number;
	bucketEnd: number;
	processedAt?: string;
}

export interface IpStatsResponse {
	buckets: IpStatsBucket[];
	availableGranularities: IpGranularity[];
	requestedRouters: string[];
}

export interface IpChartState {
	startDate: string;
	endDate: string;
	granularity: IpGranularity;
	selectedRouters: string[];
	activeMetrics: IpMetricKey[];
}

export interface SpectrumPoint {
	alpha: number;
	f: number;
}

export interface SpectrumData {
	slug: string;
	router: string;
	filename: string;
	spectrum: SpectrumPoint[];
	metadata: {
		dataSource: string;
		uniqueIPCount?: number;
		pointCount: number;
		addressType: string;
		alphaRange: { min: number; max: number };
	};
}

export interface Singularity {
	rank: string;
	address: string;
	alpha: number;
	intercept: number;
	r2: number;
	nPls: number;
}

export interface SingularitiesData {
	slug: string;
	router: string;
	filename: string;
	singularities: Singularity[];
	metadata: {
		dataSource: string;
		uniqueIPCount?: number;
		pointCount: number;
		addressType: string;
	};
}
