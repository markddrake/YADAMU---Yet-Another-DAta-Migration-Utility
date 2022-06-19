
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Export extends YadamuCLI {}

async function main() {

  try {
    const yadamuExport = new Export();
    try {
      await yadamuExport.doExport();
    } catch (e) {
	  yadamuExport.reportError(e)
	}
    await yadamuExport.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

export { Export as default}