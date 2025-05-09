import { json } from '@sveltejs/kit';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';

const execPromise = promisify(exec);

export async function GET() {
  try {

    const { stdout } = await execPromise(
      'nfdump -I -r ~/../../research/tango_cis/uonet-in/oh-ir1-gw/2025/05/08/nfcapd.202505081240'
    );

    // Process and return the result
    const result = stdout.split(/[:\n]+/).filter((element, index) => index % 2 === 1);
    return json({ result }); // Return the result as JSON
  } catch (error) {
    console.error(`exec error: ${error}`);
    
  }
}