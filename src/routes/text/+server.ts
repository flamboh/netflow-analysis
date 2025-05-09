import { json } from '@sveltejs/kit';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';

const execPromise = promisify(exec);

export async function GET() {
  try {
    const { stdout } = await execPromise('cat ~/oliver/data'); // Execute the command and wait for the result
    const result = `${stdout}`; // Trim the output to remove extra newlines
    return json({ result }); // Return the result as JSON
  } catch (error) {
    console.error(`exec error: ${error}`);
    return json({ error: 'Failed to retrieve working directory' }, { status: 500 });
  }
}