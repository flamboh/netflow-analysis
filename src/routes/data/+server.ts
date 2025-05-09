import { json } from '@sveltejs/kit';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';

const execPromise = promisify(exec);

export async function GET({ url }) {
  try {
    // Extract the 'date' parameter from the URL
    const date = url.searchParams.get('date');
    if (!date) {
      return json({ error: 'Date parameter is required' }, { status: 400 });
    }
    // Extract the 'time' parameter from the URL
    const time = url.searchParams.get('time');
    if (!time) {
      return json({ error: 'Time parameter is required' }, { status: 400 });
    }


    // const { stdout } = await execPromise(
    //   'nfdump -I -r ~/../../research/tango_cis/uonet-in/oh-ir1-gw/2025/05/08/nfcapd.202505081240'
    // );
    // get nfdump output for given date and time
    const fileToRead = '~/../../research/tango_cis/uonet-in/oh-ir1-gw/' + date.slice(0,4) + '/' + date.slice(4,6) + '/' + date.slice(6,8) + '/nfcapd.' + date + time;
    console.log("reading file " + fileToRead)
    // console.log(date, time);
    const { stdout } = await execPromise(
      'nfdump -I -r ' + fileToRead
    );


    // Process and return the result
    const result = stdout.split("\n").filter((element, index) => index % 1 === 0);
    return json({ result }); // Return the result as JSON
  } catch (error) {
    console.error(`exec error: ${error}`);
    
  }
}