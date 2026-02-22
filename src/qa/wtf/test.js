
import wtf             from 'wtfnode'

import Test           from './cli/test.js'
import YadamuLibrary   from '../../node/lib/yadamuLibrary.js'

class WTF extends Test {}

async function main() {
 
  try {
    const testHarness = new Test();
    try {
	  await testHarness.doTests();
    } catch (e) {
	  testHarness.reportError(e)
    }
    await testHarness.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main().then(() => { console.log('Finished')}).catch((e) => {console.log(e) })

