
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Compare extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()
  
  try {
	const yadamuCompare = new Compare();
    try {
      await yadamuCompare.doCompare();
    } catch (e) {
	  yadamuCompare.reportError(e)
    }
    await yadamuCompare.close();
    YadamuCLI.setExitCode(yadamuCompare.EXIT_CODE)
  } catch (e) {
    YadamuLibrary.reportError(e)
    YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

export { Compare as default}