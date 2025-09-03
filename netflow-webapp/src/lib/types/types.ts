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
