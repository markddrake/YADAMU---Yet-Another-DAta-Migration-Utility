"use strict"

import YadamuCLI from './yadamuCLI.js'
import YadamuLibrary from './yadamuLibrary.js'

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