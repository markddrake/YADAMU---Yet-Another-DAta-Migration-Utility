
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Compare extends YadamuCLI {}

async function main() {
  
  try {
	const yadamuCompare = new Compare();
    try {
      await yadamuCompare.doCompare();
    } catch (e) {
	  yadamuCompare.reportError(e)
    }
    await yadamuCompare.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

export { Compare as default}