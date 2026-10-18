
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Export extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()

  try {
    const yadamuExport = new Export();
    try {
      await yadamuExport.doExport();
    } catch (e) {
	  yadamuExport.reportError(e)
	}
    await yadamuExport.close();
    YadamuCLI.setExitCode(yadamuExport.EXIT_CODE)
  } catch (e) {
    YadamuLibrary.reportError(e)
    YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

export { Export as default}