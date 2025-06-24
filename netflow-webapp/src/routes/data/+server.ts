import { json } from '@sveltejs/kit';
import moment from 'moment';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';

const execPromise = promisify(exec);

async function loadDataFullDay(
	startDate: string,
	endDate: string,
	path: string
): Promise<Array<{ time: string; data: string }>> {
	const momentDate = moment(startDate, 'YYYYMMDD');
	const end = moment(endDate, 'YYYYMMDD').set({ hour: 23, minute: 59 });

	const allResults: Array<{ time: string; data: string }> = [];

	for (let currentTime = momentDate; currentTime <= end; currentTime.add(1, 'days')) {
		const timeStr = currentTime.format('YYYYMMDDHHmm');

		const fileToRead =
			path + timeStr.slice(0, 4) + '/' + timeStr.slice(4, 6) + '/' + timeStr.slice(6, 8);

		console.log('reading file ' + fileToRead);

		try {
			const { stdout } = await execPromise('nfdump -I -o json -R ' + fileToRead);
			const result = stdout;
			allResults.push({ time: timeStr, data: result });
		} catch (error) {
			console.error(`Error reading file ${fileToRead}: ${error}`);
		}
	}
	return allResults;
}

async function loadDataAtTime(
	startDate: string,
	endDate: string,
	time: string,
	path: string
): Promise<{ time: string; data: string }[]> {
	const momentDate = moment(startDate, 'YYYYMMDD');
	const momentTime = moment(time, 'HHmm');

	const start = momentDate.clone().set({ hour: momentTime.hour(), minute: momentTime.minute() });
	const end = moment(endDate, 'YYYYMMDD').set({ hour: 23, minute: 59 });

	const allResults: Array<{ time: string; data: string }> = [];

	for (let currentTime = start; currentTime <= end; currentTime.add(1, 'days')) {
		// console.log(currentTime.format('YYYYMMDDHHmm'));

		const timeStr = currentTime.format('YYYYMMDDHHmm');

		const fileToRead =
			path +
			timeStr.slice(0, 4) +
			'/' +
			timeStr.slice(4, 6) +
			'/' +
			timeStr.slice(6, 8) +
			'/nfcapd.' +
			timeStr;

		// console.log('reading file ' + fileToRead);

		try {
			const { stdout } = await execPromise('nfdump -I -o json -r ' + fileToRead);
			const result = stdout;
			allResults.push({ time: timeStr, data: result });
		} catch (error) {
			console.error(`Error reading file ${fileToRead}: ${error}`);
		}
	}
	return allResults;
}

export async function GET({ url }) {
	try {
		// Extract the 'date' parameter from the URL
		const startDate = url.searchParams.get('startDate');
		if (!startDate) {
			return json({ error: 'Start date parameter is required' }, { status: 400 });
		}
		const endDate = url.searchParams.get('endDate');
		if (!endDate) {
			return json({ error: 'End date parameter is required' }, { status: 400 });
		}
		const fullDay = url.searchParams.get('fullDay');
		if (!fullDay) {
			return json({ error: 'Full day parameter is required' }, { status: 400 });
		}
		// Extract the time parameters from the URL
		const time = url.searchParams.get('time');
		if (!time) {
			return json({ error: 'Time parameter is required' }, { status: 400 });
		}
		const routers = url.searchParams.get('routers');
		if (!routers) {
			return json({ error: 'Routers parameter is required' }, { status: 400 });
		}
		console.log(routers);

		let allResultsOH: Array<{ time: string; data: string }> = [];
		let allResultsCC: Array<{ time: string; data: string }> = [];

		if (fullDay === 'true') {
			if (routers.includes('cc-ir1-gw')) {
				allResultsCC = await loadDataFullDay(
					startDate,
					endDate,
					'/research/tango_cis/uonet-in/cc-ir1-gw/'
				);
			}
			if (routers.includes('oh-ir1-gw')) {
				allResultsOH = await loadDataFullDay(
					startDate,
					endDate,
					'/research/tango_cis/uonet-in/oh-ir1-gw/'
				);
			}
		} else {
			if (routers.includes('cc-ir1-gw')) {
				allResultsCC = await loadDataAtTime(
					startDate,
					endDate,
					time,
					'/research/tango_cis/uonet-in/cc-ir1-gw/'
				);
			}
			if (routers.includes('oh-ir1-gw')) {
				allResultsOH = await loadDataAtTime(
					startDate,
					endDate,
					time,
					'/research/tango_cis/uonet-in/oh-ir1-gw/'
				);
			}
		}
		console.log('allResultsCC');
		console.log(allResultsCC);
		console.log('allResultsOH');
		console.log(allResultsOH);
		// Combine and aggregate results from both routers
		const allResults = allResultsCC.map((ccResult, index) => {
			const ohResult = allResultsOH[index];
			return {
				time: ccResult.time,
				data: ohResult ? String(Number(ccResult.data) + Number(ohResult.data)) : ccResult.data
			};
		});

		if (allResultsOH.length > allResultsCC.length) {
			// Add any remaining OH results
			allResults.push(...allResultsOH.slice(allResultsCC.length));
		}
		console.log(allResults);
		return json({ result: allResults });
	} catch (error) {
		console.error(`exec error: ${error}`);
		return json({ error: 'Internal server error' }, { status: 500 });
	}
}
