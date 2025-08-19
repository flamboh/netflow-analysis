import { json } from '@sveltejs/kit';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import type { RequestHandler } from './$types';

export const POST: RequestHandler = async () => {
	try {
		// Path to the Python script relative to the project root
		const scriptPath = path.join(process.cwd(), '..', 'netflow-db', 'flow_db.py');
		const logDir = path.join(process.cwd(), '..', 'logs', 'flowStats');

		// Create PST timestamp for log filename
		const pstDate = new Date()
			.toLocaleString('sv-SE', {
				timeZone: 'America/Los_Angeles'
			})
			.replace(/:/g, '-')
			.replace(' ', '-');

		const logFile = path.join(logDir, `database-update-${pstDate}-PST.log`);

		// Ensure logs directory exists
		if (!fs.existsSync(logDir)) {
			fs.mkdirSync(logDir, { recursive: true });
		}

		// Create log file stream
		const logStream = fs.createWriteStream(logFile, { flags: 'w' });
		const pstTimestamp =
			new Date().toLocaleString('sv-SE', {
				timeZone: 'America/Los_Angeles'
			}) + ' PST';

		logStream.write(`Database update started at ${pstTimestamp}\n`);
		logStream.write(`Command: python3 ${scriptPath}\n`);
		logStream.write('='.repeat(50) + '\n');

		return new Promise((resolve) => {
			// Spawn the Python process
			const pythonProcess = spawn('python3', [scriptPath], {
				cwd: path.join(process.cwd(), '..', 'netflow-db'),
				stdio: ['pipe', 'pipe', 'pipe']
			});

			let stdout = '';
			let stderr = '';

			// Collect stdout and write to log
			pythonProcess.stdout.on('data', (data) => {
				const text = data.toString();
				stdout += text;
				logStream.write(text);
			});

			// Collect stderr and write to log
			pythonProcess.stderr.on('data', (data) => {
				const text = data.toString();
				stderr += text;
				logStream.write(text);
			});

			// Handle process completion
			pythonProcess.on('close', (code) => {
				const endPstTimestamp =
					new Date().toLocaleString('sv-SE', {
						timeZone: 'America/Los_Angeles'
					}) + ' PST';

				logStream.write(`\n${'='.repeat(50)}\n`);
				logStream.write(`Process completed at ${endPstTimestamp}\n`);
				logStream.write(`Exit code: ${code}\n`);
				logStream.end();

				if (code === 0) {
					resolve(
						json({
							success: true,
							message: 'Database update completed successfully',
							output: stdout,
							logFile: logFile
						})
					);
				} else {
					resolve(
						json(
							{
								success: false,
								message: 'Database update failed',
								error: stderr || 'Unknown error occurred',
								output: stdout,
								exitCode: code,
								logFile: logFile
							},
							{ status: 500 }
						)
					);
				}
			});

			// Handle process errors
			pythonProcess.on('error', (error) => {
				const errorPstTimestamp =
					new Date().toLocaleString('sv-SE', {
						timeZone: 'America/Los_Angeles'
					}) + ' PST';

				logStream.write(`\n${'='.repeat(50)}\n`);
				logStream.write(`Process error at ${errorPstTimestamp}\n`);
				logStream.write(`Error: ${error.message}\n`);
				logStream.end();

				resolve(
					json(
						{
							success: false,
							message: 'Failed to start database update process',
							error: error.message,
							logFile: logFile
						},
						{ status: 500 }
					)
				);
			});

			// Set a timeout to prevent hanging (30 minutes max)
			setTimeout(
				() => {
					const timeoutPstTimestamp =
						new Date().toLocaleString('sv-SE', {
							timeZone: 'America/Los_Angeles'
						}) + ' PST';

					logStream.write(`\n${'='.repeat(50)}\n`);
					logStream.write(`Process timeout at ${timeoutPstTimestamp}\n`);
					logStream.write(`Killing process after 30 minutes\n`);
					logStream.end();

					pythonProcess.kill('SIGTERM');
					resolve(
						json(
							{
								success: false,
								message: 'Database update timed out after 30 minutes',
								error: 'Process timeout',
								logFile: logFile
							},
							{ status: 408 }
						)
					);
				},
				30 * 60 * 1000
			);
		});
	} catch (error) {
		return json(
			{
				success: false,
				message: 'Server error during database update',
				error: error instanceof Error ? error.message : 'Unknown error'
			},
			{ status: 500 }
		);
	}
};
